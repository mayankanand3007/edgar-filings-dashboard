from __future__ import annotations

from datetime import date, datetime, timezone
import pytest
from pydantic import ValidationError
from edgar_pipeline.models import RawFiling


def test_raw_filing_validates() -> None:
    rec = {
        "cik": 1000045,
        "company_name": "ACME CORP",
        "form_type": "10-K",
        "date_filed": date(2024, 1, 2),
        "accession_number": "0001000045-24-000001",
        "file_name": "edgar/data/1000045/0001000045-24-000001.txt",
        "year": 2024,
        "quarter": "2024Q1",
        "edgar_url": "https://www.sec.gov/Archives/edgar/data/1000045/0001000045-24-000001.txt",
        "ingest_ts": datetime.now(timezone.utc),
    }
    RawFiling.model_validate(rec)


def test_raw_filing_rejects_negative_cik() -> None:
    rec = {
        "cik": -1,
        "company_name": "X",
        "form_type": "10-K",
        "date_filed": date(2024, 1, 2),
        "accession_number": "a",
        "file_name": "x",
        "year": 2024,
        "quarter": "2024Q1",
        "edgar_url": "https://www.sec.gov/Archives/x",
        "ingest_ts": datetime.now(timezone.utc),
    }
    with pytest.raises(ValidationError):
        RawFiling.model_validate(rec)
