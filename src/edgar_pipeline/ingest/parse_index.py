"""Parsing helpers for SEC master idx files.

The `parse_master_idx_to_pandas` function converts the text index into a
pandas DataFrame while `parse_many_master_idx` converts a list into a Dask
DataFrame with a stable partitioning strategy.
"""

from __future__ import annotations

from typing import Any, cast
import logging
from datetime import datetime, timezone
from pathlib import Path
import re
import pandas as pd
import dask.dataframe as dd

log = logging.getLogger(__name__)

LINE_RE = re.compile(r"^(\d+)\|(.+?)\|(.+?)\|(\d{4}-\d{2}-\d{2})\|(.+)$")

def parse_master_idx_to_pandas(path: Path, year: int, quarter: int) -> pd.DataFrame:
    """Parse a single SEC `master.idx` file into a pandas DataFrame.

    This reads the legacy `master.idx` text format (pipe-delimited) and
    normalizes columns to match the Raw schema used in ingestion.

    Args:
        path: Path to the `master.idx` file (text, latin-1 encoding).
        year: Year associated with the file.
        quarter: Quarter associated with the file.

    Returns:
        pandas.DataFrame with columns: `cik`, `company_name`, `form_type`,
        `date_filed`, `accession_number`, `file_name`, `year`, `quarter`,
        `edgar_url`, `ingest_ts`.
    """
    lines = path.read_text(encoding="latin-1").splitlines()
    rows: list[dict[str, object]] = []

    for ln in lines:
        m = LINE_RE.match(ln)
        if not m:
            continue
        cik = int(m.group(1))
        company = m.group(2)
        form = m.group(3)
        date_filed = m.group(4)
        filename = m.group(5)
        accession = filename.split("/")[-1].replace(".txt", "")
        edgar_url = f"https://www.sec.gov/Archives/{filename}"

        rows.append(
            {
                "cik": cik,
                "company_name": company,
                "form_type": form,
                "date_filed": date_filed,
                "accession_number": accession,
                "file_name": filename,
                "year": year,
                "quarter": f"{year}Q{quarter}",
                "edgar_url": edgar_url,
                "ingest_ts": datetime.now(timezone.utc).isoformat(),
            }
        )

    return pd.DataFrame(rows)

def parse_many_master_idx(paths: list[Path], years_quarters: list[tuple[int, int]]) -> Any:
    """Parse many master.idx files into a concatenated Dask DataFrame.

    Args:
        paths: List of Paths to `master.idx` files.
        years_quarters: Corresponding list of `(year, quarter)` tuples.

    Returns:
        Dask DataFrame concatenating all parsed master idx files with a
        partitioning strategy suitable for downstream ingestion.
    """
    if len(paths) != len(years_quarters):
        raise ValueError("paths and years_quarters must have same length")

    parts: list[Any] = []
    dd_mod = cast(Any, dd)
    for p, (y, q) in zip(paths, years_quarters):
        pdf = parse_master_idx_to_pandas(p, y, q)
        parts.append(dd_mod.from_pandas(pdf, npartitions=max(1, len(pdf) // 200_000)))

    if not parts:
        return dd_mod.from_pandas(pd.DataFrame(), npartitions=1)

    return dd_mod.concat(parts, interleave_partitions=True)
