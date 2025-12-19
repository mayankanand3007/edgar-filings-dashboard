"""Utilities for loading Gold DataFrames into MongoDB.

Gold datasets are typically small (aggregated) and are materialized to pandas
before being upserted into dedicated collections. This module centralizes the
upsert strategy and logging behavior.
"""

from __future__ import annotations

from typing import Any
import logging
import dask.dataframe as dd
from pymongo import UpdateOne

from edgar_pipeline.config import get_settings
from edgar_pipeline.db import get_client, get_db

log = logging.getLogger(__name__)


def load_gold(
    ddf: Any,
    collection_name: str,
    key_fields: list[str],
) -> None:
    """Safely load a GOLD aggregation into MongoDB.

    Strategy:
    - Compute Dask DF → Pandas (gold is small)
    - Upsert row-by-row (deterministic, stable)

    Args:
        ddf: Dask DataFrame representing the gold dataset.
        collection_name: Target MongoDB collection name.
        key_fields: List of fields used as the upsert key.

    Returns:
        None. Side effect: upserts rows into the specified collection.
    """
    # -------------------------
    # Mongo setup
    # -------------------------
    s = get_settings()
    client = get_client(s.mongo_uri)
    db = get_db(client, s.mongo_db)
    collection = db[collection_name]

    log.info("Generating gold collection: %s", collection_name)

    # -------------------------
    # Materialize (SAFE)
    # -------------------------
    pdf = ddf.compute()

    if pdf.empty:
        log.warning("No rows to load for %s", collection_name)
        return

    # -------------------------
    # Build bulk ops
    # -------------------------
    ops = []
    for row in pdf.to_dict("records"):
        query = {k: row[k] for k in key_fields}
        ops.append(
            UpdateOne(
                query,
                {"$set": row},
                upsert=True,
            )
        )

    # -------------------------
    # Write to Mongo
    # -------------------------
    if ops:
        collection.bulk_write(ops, ordered=False)

    log.info(
        "Gold load complete for %s: %d rows",
        collection_name,
        len(ops),
    )
