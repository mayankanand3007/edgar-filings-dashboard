"""Fetch and persist company profile data (e.g., SIC) from SEC submissions.

This utility iterates CIKs present in a raw collection, fetches SEC profiles,
and upserts them into a profiles collection for downstream enrichment.
"""

from __future__ import annotations

import logging
from pathlib import Path

from pymongo import MongoClient, UpdateOne

from edgar_pipeline.enrich.sec_profile import fetch_company_profile

log = logging.getLogger(__name__)

def build_cik_profiles(
    mongo_uri: str,
    mongo_db: str,
    raw_collection: str,
    profile_collection: str,
    user_agent: str,
    cache_dir: Path,
    limit: int = 1500,
) -> int:
    """Fetch and upsert SEC company profiles (SIC) into a profiles collection.

    Args:
        mongo_uri: MongoDB connection string.
        mongo_db: Database name.
        raw_collection: Name of the source collection to scan for CIKs.
        profile_collection: Name of the target profiles collection.
        user_agent: SEC User-Agent for requests.
        cache_dir: Local directory for request caching.
        limit: Optional limit to number of CIKs to fetch.

    Returns:
        Number of CIKs fetched (int).
    """
    client = MongoClient(mongo_uri)
    db = client[mongo_db]
    raw = db[raw_collection]
    prof = db[profile_collection]

    ciks = raw.distinct("cik")
    ciks = [int(c) for c in ciks if c is not None]
    if limit and len(ciks) > limit:
        ciks = ciks[:limit]

    existing = set(prof.distinct("cik"))
    to_fetch = [c for c in ciks if c not in existing]

    if not to_fetch:
        log.info("CIK profiles already present (%d).", len(existing))
        client.close()
        return 0

    log.info("Fetching %d CIK profiles (SIC) from SEC submissions...", len(to_fetch))

    ops: list[UpdateOne] = []
    for cik in to_fetch:
        try:
            p = fetch_company_profile(cik=cik, user_agent=user_agent, cache_dir=cache_dir)
            doc = {
                "cik": p.cik,
                "sic": p.sic,
                "sic_description": p.sic_description,
                "company_name_sec": p.name,
            }
            ops.append(UpdateOne({"cik": p.cik}, {"$set": doc}, upsert=True))
        except Exception as e:
            log.warning("Failed to fetch CIK=%s profile: %s", cik, e)

        if len(ops) >= 500:
            prof.bulk_write(ops, ordered=False)
            ops.clear()

    if ops:
        prof.bulk_write(ops, ordered=False)

    client.close()
    return len(to_fetch)
