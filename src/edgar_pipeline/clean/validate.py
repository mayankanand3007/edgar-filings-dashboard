"""Validation utilities for the Clean layer.

This module validates partition data against the Pydantic `CleanFiling` model
and converts string timestamps into native Python types prior to validation.
"""
from __future__ import annotations

from datetime import datetime, date
from typing import Any
import pandas as pd

from edgar_pipeline.models import CleanFiling


def validate_partition(pdf: pd.DataFrame) -> tuple[list[dict[str, Any]], int]:
    """Validate a pandas partition of records using Pydantic.

    Converts string-encoded timestamps to native types and uses
    `CleanFiling.model_validate` to validate each record.

    Args:
        pdf: Pandas DataFrame for the partition.

    Returns:
        A tuple of (list_of_validated_records, bad_count).
    """
    good: list[dict[str, Any]] = []
    bad = 0

    for rec in pdf.to_dict(orient="records"):
        if isinstance(rec.get("ingest_ts"), str):
            rec["ingest_ts"] = datetime.fromisoformat(rec["ingest_ts"].replace("Z", ""))
        if isinstance(rec.get("cleaned_ts"), str):
            rec["cleaned_ts"] = datetime.fromisoformat(rec["cleaned_ts"].replace("Z", ""))
        if isinstance(rec.get("date_filed"), str):
            rec["date_filed"] = date.fromisoformat(rec["date_filed"])

        try:
            m = CleanFiling.model_validate(rec)
            good.append(m.model_dump(mode="python"))
        except Exception:
            bad += 1

    return good, bad
