"""Cleaning and normalization utilities.

This module contains transformations that are applied partition-wise using
Dask. The output is a Dask DataFrame whose schema is stable and suitable for
Pydantic validation in the Clean layer.
"""
from __future__ import annotations

import pandas as pd
import dask.dataframe as dd
import logging
from typing import Any

log = logging.getLogger(__name__)

def clean_raw_ddf(ddf: Any) -> Any:
    """Clean raw EDGAR filings data.

    Performs text normalization, date parsing, year derivation, and ensures a
    safe `sic` column exists (defaults to "UNKNOWN").

    Returns:
        Transformed Dask DataFrame with a stable schema for downstream steps.
    """
    log.info("Starting clean_raw_ddf transformation")

    def _clean_partition(pdf: pd.DataFrame) -> pd.DataFrame:
        """Partition-level cleaning function applied via map_partitions.

        Args:
            pdf: Pandas DataFrame for the partition.

        Returns:
            Cleaned Pandas DataFrame.
        """
        pdf = pdf.copy()

        # -----------------------------
        # Normalize company name
        # -----------------------------
        if "company_name" in pdf.columns:
            pdf["company_name"] = (
                pdf["company_name"]
                .astype(str)
                .str.strip()                # normalize internal whitespace to single spaces
                .str.replace(r"\s+", " ", regex=True)                .replace({"": None})
            )

        # -----------------------------
        # Normalize form type
        # -----------------------------
        if "form_type" in pdf.columns:
            pdf["form_type"] = (
                pdf["form_type"]
                .astype(str)
                .str.strip()
                .str.upper()
            )

        # -----------------------------
        # Standardize date
        # -----------------------------
        if "date_filed" in pdf.columns:
            pdf["date_filed"] = pd.to_datetime(
                pdf["date_filed"],
                errors="coerce",
            )

        # -----------------------------
        # Derive year
        # -----------------------------
        if "year" not in pdf.columns and "date_filed" in pdf.columns:
            pdf["year"] = pdf["date_filed"].dt.year

        # -----------------------------
        # SIC placeholder (SAFE)
        # -----------------------------
        if "sic" not in pdf.columns:
            pdf["sic"] = "UNKNOWN"
        else:
            pdf["sic"] = pdf["sic"].fillna("UNKNOWN").astype(str)

        return pdf

    # 🔥 CRITICAL: Tell Dask schema includes `sic`
    meta = ddf._meta.copy()
    if "sic" not in meta.columns:
        meta["sic"] = ""

    return ddf.map_partitions(_clean_partition, meta=meta)
