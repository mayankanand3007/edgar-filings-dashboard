"""Command-line interface for orchestrating the pipeline.

Provides subcommands: `ingest`, `clean`, `gold`, and `all`. Each command
is implemented as a `cmd_*` function that accepts an argparse namespace.
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Any, List
from typing import cast, Any as TypingAny

from dotenv import load_dotenv
import pandas as pd
import dask.dataframe as dd

from edgar_pipeline.config import get_settings
from edgar_pipeline.logging_config import configure_logging
from edgar_pipeline.db import get_client, get_db

# INGEST
from edgar_pipeline.ingest.fetch_index import IndexTarget, download_master_idx
from edgar_pipeline.ingest.parse_index import parse_many_master_idx
from edgar_pipeline.ingest.load_raw import load_raw_to_mongo

# CLEAN
from edgar_pipeline.clean.transform import clean_raw_ddf
from edgar_pipeline.clean.load_clean import load_clean_to_mongo

# GOLD
from edgar_pipeline.aggregate.build_gold import (
    gold_filings_by_form_month,
    gold_forms_by_year,
    gold_top_ciks_by_volume,
    gold_10k_by_year,
    gold_10k_top_ciks_by_volume,
    gold_10k_recent,
    gold_10k_by_sic,
    gold_10k_by_sic_year,
    gold_10k_company_vs_sic_share,
    gold_10k_anomalies,
)
"""Command-line interface for orchestrating the pipeline.

Provides subcommands: `ingest`, `clean`, `gold`, and `all`. Each command
is implemented as a `cmd_*` function that accepts an argparse namespace.
"""

from edgar_pipeline.aggregate.load_gold import load_gold

log = logging.getLogger(__name__)


# --------------------------------------------------
# Helpers
# --------------------------------------------------
def _targets_for_year(year: int, quarter: int | None = None) -> list[IndexTarget]:
    """Return a list of IndexTarget objects for a given year and optional quarter.

    Args:
        year: Integer year (e.g. 2024).
        quarter: Optional quarter (1-4). If omitted, returns all four quarters.
    """
    if quarter is not None:
        return [IndexTarget(year=year, quarter=quarter)]
    return [IndexTarget(year=year, quarter=q) for q in (1, 2, 3, 4)]


def _load_collection_to_ddf(
    collection: Any,
    projection: dict[str, Any],
    batch_size: int = 50_000,
) -> Any:
    """Safely load a MongoDB collection into a Dask DataFrame using batched reads."""
    cursor = collection.find({}, projection).batch_size(batch_size)

    pdf_batches: List[pd.DataFrame] = []
    buffer: List[dict[str, Any]] = []

    for doc in cursor:
        buffer.append(doc)
        if len(buffer) >= batch_size:
            pdf_batches.append(pd.DataFrame(buffer))
            buffer.clear()

    if buffer:
        pdf_batches.append(pd.DataFrame(buffer))

    if not pdf_batches:
        dd_mod = cast(TypingAny, dd)
        return dd_mod.from_pandas(pd.DataFrame(), npartitions=1)

    pdf = pd.concat(pdf_batches, ignore_index=True)
    nparts = max(1, len(pdf) // 200_000)

    log.info("Loaded %d documents into %d Dask partitions", len(pdf), nparts)

    dd_mod = cast(TypingAny, dd)
    return dd_mod.from_pandas(pdf, npartitions=nparts)


# --------------------------------------------------
# INGEST
# --------------------------------------------------
def cmd_ingest(args: argparse.Namespace) -> None:
    """Ingest master index files for the requested year range into `raw_filings`.

    Args:
        args: argparse namespace with `from_year`, `to_year`, `quarter`, `force`.
    """
    s = get_settings()
    client = get_client(s.mongo_uri)
    db = get_db(client, s.mongo_db)

    for year in range(args.from_year, args.to_year + 1):
        already = db["raw_filings"].count_documents({"year": year})
        if already > 0 and not args.force:
            log.info("Year %d already ingested (%d docs). Skipping.", year, already)
            continue

        targets = _targets_for_year(year, args.quarter)
        paths, yq = [], []

        for t in targets:
            p = download_master_idx(t, s.edgar_data_dir, s.sec_user_agent)
            paths.append(p)
            yq.append((t.year, t.quarter))

        ddf = parse_many_master_idx(paths, yq)
        ddf = ddf.repartition(npartitions=min(4, ddf.npartitions))

        log.info("Parsed %d filings for year=%d", int(ddf.shape[0].compute()), year)
        load_raw_to_mongo(ddf)

    log.info("Ingest completed.")


# --------------------------------------------------
# CLEAN
# --------------------------------------------------
def cmd_clean(_: argparse.Namespace) -> None:
    """Run cleaning and validation and write results to `clean_filings`.

    This command reads from `raw_filings`, applies `clean_raw_ddf`, and
    persists validated records to MongoDB.
    """
    s = get_settings()
    client = get_client(s.mongo_uri)
    db = get_db(client, s.mongo_db)

    raw = db["raw_filings"]
    ddf = _load_collection_to_ddf(raw, {"_id": False})

    if ddf.shape[0].compute() == 0:
        raise RuntimeError("raw_filings is empty. Run ingest first.")

    ddf_clean = clean_raw_ddf(ddf)
    good, bad = load_clean_to_mongo(ddf_clean)

    log.info(
        "clean_filings count=%d (good=%d bad=%d)",
        db["clean_filings"].count_documents({}),
        good,
        bad,
    )


# --------------------------------------------------
# GOLD
# --------------------------------------------------
def cmd_gold(args: argparse.Namespace) -> None:
    """Compute Gold aggregations from the Clean layer and upsert into Mongo.

    Args:
        args: argparse namespace with `top_n` and other options.
    """
    s = get_settings()
    client = get_client(s.mongo_uri)
    db = get_db(client, s.mongo_db)

    clean = db["clean_filings"]
    ddf = _load_collection_to_ddf(clean, {"_id": False})

    if ddf.shape[0].compute() == 0:
        raise RuntimeError("clean_filings is empty. Run clean first.")

    # ---- GENERAL GOLD ----
    load_gold(gold_filings_by_form_month(ddf), "gold_filings_by_form_month", ["month", "form_type"])
    load_gold(gold_forms_by_year(ddf), "gold_forms_by_year", ["year", "form_type"])
    load_gold(gold_top_ciks_by_volume(ddf, args.top_n), "gold_top_ciks", ["cik"])

    # ---- 10-K GOLD ----
    load_gold(gold_10k_by_year(ddf), "gold_10k_by_year", ["year"])
    load_gold(gold_10k_top_ciks_by_volume(ddf, args.top_n), "gold_10k_top_ciks", ["cik"])
    load_gold(gold_10k_recent(ddf), "gold_10k_recent", ["accession_number"])

    # ---- SIC (OPTIONAL) ----
    if "sic" in ddf.columns:
        load_gold(gold_10k_by_sic(ddf), "gold_10k_by_sic", ["sic", "company_name"])
        load_gold(gold_10k_by_sic_year(ddf), "gold_10k_by_sic_year", ["year", "sic"])
        load_gold(
            gold_10k_company_vs_sic_share(ddf),
            "gold_10k_company_vs_sic_share",
            ["year", "sic", "company_name"],
        )
        load_gold(gold_10k_anomalies(ddf), "gold_10k_anomalies", ["year"])
    else:
        log.warning("Column 'sic' not found. Skipping SIC-based gold tables.")

    log.info("Gold layer successfully generated.")


# --------------------------------------------------
# ALL
# --------------------------------------------------
def cmd_all(args: argparse.Namespace) -> None:
    """Convenience: run ingest → clean → gold with the provided args."""
    cmd_ingest(args)
    cmd_clean(args)
    cmd_gold(args)




# --------------------------------------------------
# CLI
# --------------------------------------------------
def build_parser() -> argparse.ArgumentParser:
    """Build and return the top-level argument parser for the CLI.

    The returned parser has subcommands `ingest`, `clean`, `gold`, and `all`
    with commonly used options configured.

    Returns:
        Configured argparse.ArgumentParser instance.
    """
    p = argparse.ArgumentParser(prog="edgar_pipeline")
    sub = p.add_subparsers(dest="cmd", required=True)

    p_ingest = sub.add_parser("ingest")
    p_ingest.add_argument("--from-year", type=int, default=2020)
    p_ingest.add_argument("--to-year", type=int, default=2025)
    p_ingest.add_argument("--quarter", type=int, choices=[1, 2, 3, 4], default=None)
    p_ingest.add_argument("--force", action="store_true")

    sub.add_parser("clean")

    p_gold = sub.add_parser("gold")
    p_gold.add_argument("--top-n", type=int, default=25)

    p_all = sub.add_parser("all")
    p_all.add_argument("--from-year", type=int, default=2020)
    p_all.add_argument("--to-year", type=int, default=2025)
    p_all.add_argument("--quarter", type=int, choices=[1, 2, 3, 4], default=None)
    p_all.add_argument("--force", action="store_true")
    p_all.add_argument("--top-n", type=int, default=25)

    return p


def main() -> None:
    """CLI entry point: parse args, configure logging and dispatch commands."""
    load_dotenv()
    configure_logging(Path("logs/pipeline.log"))

    args = build_parser().parse_args()

    if args.cmd == "ingest":
        cmd_ingest(args)
    elif args.cmd == "clean":
        cmd_clean(args)
    elif args.cmd == "gold":
        cmd_gold(args)
    elif args.cmd == "all":
        cmd_all(args)
    else:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
