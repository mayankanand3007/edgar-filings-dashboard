"""Helpers to fetch and parse SEC company submission profiles.

The main exported item is `fetch_company_profile` which returns a
`CompanyProfile` dataclass containing SIC and name information.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from pathlib import Path

log = logging.getLogger(__name__)

SEC_SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"

@dataclass(frozen=True)
class CompanyProfile:
    """Data container for SEC company profile information.

    Attributes:
        cik: CIK as integer.
        sic: SIC code as string or None.
        sic_description: Human-readable SIC description or None.
        name: Company name as reported by SEC or None.
    """
    cik: int
    sic: str | None
    sic_description: str | None
    name: str | None


def _pad_cik(cik: int) -> str:
    """Return zero-padded 10-digit CIK string for SEC endpoints.

    Args:
        cik: Numeric CIK.

    Returns:
        Zero-padded 10-digit string.
    """
    return str(int(cik)).zfill(10)


def fetch_company_profile(
    cik: int,
    user_agent: str,
    cache_dir: Path,
    sleep_seconds: float = 0.12,
    timeout: float = 30.0,
) -> CompanyProfile:
    """Fetch and parse the SEC submissions JSON for a CIK, with local caching.

    Args:
        cik: Company CIK as integer.
        user_agent: SEC User-Agent header value.
        cache_dir: Local directory for caching responses.
        sleep_seconds: Optional throttle between requests.
        timeout: Request timeout in seconds.

    Returns:
        CompanyProfile instance with SIC and name fields.
    """
    cache_dir.mkdir(parents=True, exist_ok=True)
    cik10 = _pad_cik(cik)
    cache_path = cache_dir / f"CIK{cik10}.json"

    if cache_path.exists():
        data = json.loads(cache_path.read_text(encoding="utf-8"))
    else:
        url = SEC_SUBMISSIONS_URL.format(cik=cik10)
        headers = {"User-Agent": user_agent, "Accept-Encoding": "gzip, deflate"}
        import requests  # type: ignore[import-untyped]  # local import to avoid requiring type stubs at module import
        resp = requests.get(url, headers=headers, timeout=timeout)
        if resp.status_code != 200:
            raise RuntimeError(f"SEC submissions fetch failed for CIK={cik10}: {resp.status_code}")
        data = resp.json()
        cache_path.write_text(json.dumps(data), encoding="utf-8")
        time.sleep(sleep_seconds)

    sic = str(data.get("sic")) if data.get("sic") is not None else None
    sic_desc = data.get("sicDescription")
    name = data.get("name")

    return CompanyProfile(cik=int(cik), sic=sic, sic_description=sic_desc, name=name)
