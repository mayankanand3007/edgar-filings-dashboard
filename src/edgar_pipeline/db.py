"""MongoDB helpers and bulk upsert utility.

Centralizes creation of Mongo clients and a stable bulk_upsert implementation
used throughout the pipeline.
"""

from __future__ import annotations

from typing import Any, Iterable
from pymongo import MongoClient, UpdateOne
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import PyMongoError

import os
import certifi


def get_client(uri: str) -> MongoClient:
    """Return a configured PyMongo MongoClient for the provided URI.

    Args:
        uri: MongoDB connection URI.

    Returns:
        Configured MongoClient instance.
    """
    return MongoClient(
        uri,
        tls=True,
        tlsCAFile=certifi.where(),
        serverSelectionTimeoutMS=30000,
        socketTimeoutMS=30000,
        connectTimeoutMS=30000,
    )


def get_db(
    client: MongoClient[dict[str, Any]],
    db_name: str,
) -> Database[dict[str, Any]]:
    """Return the named Database instance from a MongoClient.

    Args:
        client: PyMongo MongoClient.
        db_name: Database name.

    Returns:
        A Database object.
    """
    return client[db_name]


def bulk_upsert(
    collection: Collection[dict[str, Any]],
    docs: Iterable[dict[str, Any]],
    key_field: str,
    batch_size: int = 1000,
) -> int:
    """Bulk upsert documents using `key_field` as the selector.

    This helper writes in batches and swallows transient errors; it returns
    the number of documents that were attempted (a conservative metric).

    Args:
        collection: Target PyMongo collection.
        docs: Iterable of document dictionaries to upsert.
        key_field: Document key to use for upsert selector.
        batch_size: Number of ops per bulk_write call.

    Returns:
        Integer number of documents attempted.
    """
    ops: list[UpdateOne] = []
    attempted = 0

    for d in docs:
        if key_field not in d:
            continue

        ops.append(
            UpdateOne(
                {key_field: d[key_field]},
                {"$set": d},
                upsert=True,
            )
        )
        attempted += 1

        if len(ops) >= batch_size:
            try:
                collection.bulk_write(ops, ordered=False)
            except PyMongoError as e:
                # Log upstream — do not crash pipeline
                print(f"[WARN] bulk_upsert batch failed: {e}")
            ops.clear()

    if ops:
        try:
            collection.bulk_write(ops, ordered=False)
        except PyMongoError as e:
            print(f"[WARN] bulk_upsert final batch failed: {e}")

    return attempted
