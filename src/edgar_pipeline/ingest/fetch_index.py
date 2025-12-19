"""Utilities to identify and download SEC master index files.

`IndexTarget` represents a specific (year, quarter) index to download.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

log = logging.getLogger(__name__)

BASE = "https://www.sec.gov/Archives"

@dataclass(frozen=True)
class IndexTarget:
    """Target (year, quarter) for a master index file.

    Attributes:
        year: Four-digit year (e.g. 2024).
        quarter: Quarter number (1-4).
    """
    year: int
    quarter: int  # 1-4


def master_idx_url(year: int, quarter: int) -> str:
    """Return the SEC master.idx URL for a given year and quarter.

    Args:
        year: Four-digit year (e.g., 2024).
        quarter: Quarter number (1-4).

    Returns:
        Fully-qualified URL string to the master.idx file.
    """
    return f"{BASE}/edgar/full-index/{year}/QTR{quarter}/master.idx"

def download_master_idx(target: IndexTarget, out_dir: Path, user_agent: str) -> Path:
    """Download or return cached SEC master index file for a target.

    Args:
        target: `IndexTarget` specifying year and quarter to download.
        out_dir: Local directory to cache downloaded index files.
        user_agent: SEC User-Agent header value to send with the request.

    Returns:
        Path to the downloaded (or cached) `master.idx` file.

    Raises:
        requests.HTTPError if the remote request fails (non-2xx status).
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    url = master_idx_url(target.year, target.quarter)
    out_path = out_dir / f"master_{target.year}_QTR{target.quarter}.idx"

    if out_path.exists() and out_path.stat().st_size > 0:
        log.info("Cache hit: %s", out_path)
        return out_path

    log.info("Downloading %s", url)
    import requests  # type: ignore[import-untyped]  # local import to avoid requiring type stubs at module import
    r = requests.get(url, headers={"User-Agent": user_agent}, timeout=60)
    r.raise_for_status()
    out_path.write_bytes(r.content)
    log.info("Saved: %s (%d bytes)", out_path, out_path.stat().st_size)
    return out_path
