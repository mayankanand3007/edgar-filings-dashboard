"""Load cleaned filings into MongoDB with partitioned upserts.

Module notes:
- Each Dask partition is processed independently and upserted in batches.
- Datetimes are normalized for BSON compatibility.
"""
from __future__ import annotations

import logging
from datetime import datetime, date, timezone
from typing import Iterable, List, Tuple, Any
from typing import cast, Any as TypingAny

import pandas as pd
import dask.dataframe as dd
from dask import delayed, compute  # type: ignore[attr-defined]
from pymongo import MongoClient, UpdateOne

from edgar_pipeline.config import get_settings

log = logging.getLogger(__name__)

BATCH_SIZE = 1000

def _chunks(data: List[dict[str, Any]], size: int) -> Iterable[List[dict[str, Any]]]:
    """Yield lists of documents in batches.

    Args:
        data: List of dict documents.
        size: Batch size.
    """
    for i in range(0, len(data), size):
        yield data[i : i + size]


def _normalize_doc(doc: dict[str, Any]) -> dict[str, Any]:
    """Convert non-BSON-safe types to Mongo-safe types."""
    for k, v in list(doc.items()):
        if isinstance(v, date) and not isinstance(v, datetime):
            doc[k] = datetime.combine(v, datetime.min.time())
        # datetime is BSON-safe; keep as-is
    return doc


def _process_partition(pdf: pd.DataFrame) -> Tuple[int, int]:
    """Runs inside a worker (delayed task).

    Upserts docs into `clean_filings` using `accession_number` and
    returns a tuple `(good_rows, bad_rows)`.
    """
    if pdf is None or len(pdf) == 0:
        return 0, 0

    settings = get_settings()
    client = MongoClient(
        settings.mongo_uri,
        retryWrites=False,
        serverSelectionTimeoutMS=5000,
    )
    collection = client[settings.mongo_db]["clean_filings"]

    good = 0
    bad = 0

    docs: List[dict[str, Any]] = []

    for _, row in pdf.iterrows():
        try:
            doc = row.to_dict()
            doc["clean_ts"] = datetime.now(timezone.utc)
            doc = _normalize_doc(doc)

            # must have accession_number to upsert
            if "accession_number" not in doc or doc["accession_number"] in (None, ""):
                bad += 1
                continue

            docs.append(doc)
            good += 1
        except Exception:
            bad += 1

    # bulk upsert in batches
    for batch in _chunks(docs, BATCH_SIZE):
        ops = [
            UpdateOne(
                {"accession_number": d["accession_number"]},
                {"$set": d},
                upsert=True,
            )
            for d in batch
        ]
        if ops:
            collection.bulk_write(ops, ordered=False)

    client.close()
    return good, bad


def load_clean_to_mongo(ddf: Any) -> tuple[int, int]:
    """Driver function.

    Uses `to_delayed()` to avoid Dask metadata mismatch issues when
    executing partitioned upserts.
    """
    log.info("Loading clean filings into MongoDB...")

    # Make sure we operate on pandas partitions
    delayed_parts = ddf.to_delayed()
    tasks = [delayed(_process_partition)(part) for part in delayed_parts]

    # `compute` is untyped in our environment; cast to Any before calling
    results = cast(TypingAny, compute)(*tasks)  # tuple of (good,bad) for each partition

    good_total = sum(g for g, _ in results)
    bad_total = sum(b for _, b in results)

    log.info("Clean load complete: good=%d bad=%d", good_total, bad_total)
    return int(good_total), int(bad_total)
