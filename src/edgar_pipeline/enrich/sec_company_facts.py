"""SEC Company Facts fetcher and helper functions.

Provides cached access to the SEC company facts JSON and helpers to extract
industry identifiers.
"""

from __future__ import annotations

from typing import Any, Optional
import json
from pathlib import Path

SEC_HEADERS = {
    "User-Agent": "Vignesh Paramasivam parama34@rowan.edu",
    "Accept-Encoding": "gzip, deflate",
}

CACHE_DIR = Path("data/sec_company_facts")
CACHE_DIR.mkdir(parents=True, exist_ok=True)


def fetch_company_facts(cik: int) -> Any:
    """Fetch & cache companyfacts JSON for the given CIK.

    Args:
        cik: Company CIK as int.

    Returns:
        Parsed JSON dictionary for the company's facts (raw JSON structure).
    """
    cik_str = f"{cik:010d}"
    cache_file = CACHE_DIR / f"{cik_str}.json"

    if cache_file.exists():
        return json.loads(cache_file.read_text())

    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik_str}.json"
    import requests  # type: ignore[import-untyped]  # local import
    r = requests.get(url, headers=SEC_HEADERS, timeout=30)
    r.raise_for_status()

    cache_file.write_text(r.text)
    return r.json()


def extract_sic_naics(facts: dict[str, Any]) -> tuple[Optional[str], Optional[str]]:
    """Extract SIC and NAICS codes from the company facts payload.

    Args:
        facts: Parsed companyfacts JSON.

    Returns:
        Tuple (sic, naics) or (None, None) when unavailable.
    """
    entity = facts.get("entityName")

    sic = facts.get("sic")
    naics = facts.get("naics")

    return sic, naics
