"""Pydantic models used for Clean and Gold validation.

These models define the expected schema for Clean records and several Gold
outputs used by the dashboard and tests.
"""

from __future__ import annotations

from datetime import date, datetime
from pydantic import BaseModel, Field, HttpUrl, ConfigDict

class RawFiling(BaseModel):
    """Schema for a raw parsed filing (before cleaning/validation)."""
    model_config = ConfigDict(extra="forbid")
    cik: int = Field(..., ge=0)
    company_name: str
    form_type: str
    date_filed: date
    accession_number: str
    file_name: str
    year: int = Field(..., ge=1993, le=2100)
    quarter: str
    edgar_url: HttpUrl
    ingest_ts: datetime

class CleanFiling(BaseModel):
    """Schema for a cleaned and validated filing record.

    Attributes:
        cik: Central Index Key (numeric CIK).
        company_name: Optional company name.
        form_type: Filing form type (e.g., '10-K').
        date_filed: Filing date as `date`.
        accession_number: Unique accession string.
        file_name: Path/filename in EDGAR archives.
        year: Filing year (validated range).
        quarter: Filing quarter tag.
        edgar_url: Full EDGAR URL for the filing document.
        ingest_ts: Timestamp when row was ingested.
        cleaned_ts: Timestamp when record was cleaned/validated.
    """
    model_config = ConfigDict(extra="forbid")
    cik: int = Field(..., ge=0)
    company_name: str | None
    form_type: str
    date_filed: date
    accession_number: str
    file_name: str
    year: int = Field(..., ge=1993, le=2100)
    quarter: str
    edgar_url: HttpUrl
    ingest_ts: datetime
    cleaned_ts: datetime

class GoldFilingsByFormMonth(BaseModel):
    """Gold model representing monthly filing counts per form type."""
    model_config = ConfigDict(extra="forbid")
    month: str
    form_type: str
    filings_count: int = Field(..., ge=0)

class GoldFormsByYear(BaseModel):
    """Gold model representing yearly filing counts per form type."""
    model_config = ConfigDict(extra="forbid")
    year: int
    form_type: str
    filings_count: int = Field(..., ge=0)

class GoldTopCiksByVolume(BaseModel):
    """Gold model for top CIKs ranked by filing volume."""
    model_config = ConfigDict(extra="forbid")
    cik: int
    company_name: str | None
    filings_count: int = Field(..., ge=0)
