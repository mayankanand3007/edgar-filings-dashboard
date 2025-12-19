"""Raw-layer loading utilities.

This module parses pandas/Dask partitions and upserts them into the `raw_filings`
collection in MongoDB in batches for reliability.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Iterable, List, Any

import pandas as pd
import dask.dataframe as dd
from pymongo import MongoClient, UpdateOne
from pymongo.errors import PyMongoError

from edgar_pipeline.config import get_settings

log = logging.getLogger(__name__)

BATCH_SIZE = 1000  # critical for stability

def _chunks(iterable: List[dict[str, Any]], size: int) -> Iterable[List[dict[str, Any]]]:
    """Chunk an iterable of dicts into batches of `size`.

    Args:
        iterable: List of dict records.
        size: Batch size.

    Yields:
        Slices of the iterable as lists.
    """
    for i in range(0, len(iterable), size):
        yield iterable[i : i + size]


def _load_partition(pdf: pd.DataFrame) -> int:
    """Upsert a pandas partition into the `raw_filings` MongoDB collection.

    Notes:
        This function is executed inside Dask worker processes and therefore
        creates and closes its own MongoDB connection for isolation.

    Args:
        pdf: Pandas DataFrame partition to load. Expected to include at least
            `accession_number` and other Raw fields.

    Returns:
        The number of documents upserted from this partition.
    """
    settings = get_settings()

    client = MongoClient(
        settings.mongo_uri,
        retryWrites=False,
        serverSelectionTimeoutMS=5000,
    )
    collection = client[settings.mongo_db]["raw_filings"]

    if "ingest_ts" not in pdf.columns:
        pdf["ingest_ts"] = datetime.now(timezone.utc).isoformat()

    records = pdf.to_dict(orient="records")
    upserted = 0

    for batch in _chunks(records, BATCH_SIZE):
        ops = [
            UpdateOne(
                {"accession_number": doc["accession_number"]},
                {"$set": doc},
                upsert=True,
            )
            for doc in batch
        ]

        if not ops:
            continue

        try:
            result = collection.bulk_write(ops, ordered=False)
            upserted += result.upserted_count + result.modified_count
        except PyMongoError as e:
            log.warning("Bulk write failed for one batch: %s", e)
            continue

    client.close()
    return upserted


def load_raw_to_mongo(ddf: Any) -> int:
    """Load a Dask DataFrame into the `raw_filings` collection using partitioned upserts.

    This function materializes counts for logging, maps partitions to `_load_partition`,
    and aggregates the total number of upserted documents.

    Args:
        ddf: Dask DataFrame with Raw-schema columns.

    Returns:
        Total number of documents upserted into `raw_filings`.
    """
    row_count = ddf.shape[0].compute()
    log.info("Loading %d rows into raw_filings collection...", row_count)

    counts = ddf.map_partitions(
        _load_partition,
        meta=("upserted", "int"),
    ).compute()

    total = int(counts.sum())
    log.info("Loaded %d documents into raw_filings.", total)
    return total
