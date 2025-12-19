"""Gold aggregation functions.

Functions in this module build Gold-layer tables from the Clean layer.
Gold tables are small, Dask-safe outputs that are computed, materialized to
pandas, and upserted into MongoDB.

Expectations:
- Input: a Dask DataFrame with normalized columns such as `year`, `form_type`,
  `date_filed`, `cik`, `company_name`, `sic` (optional)
- Outputs: DataFrames with descriptive columns documented on each function
  docstring.
"""
from __future__ import annotations

from typing import Any
import numpy as np
import pandas as pd
import dask.dataframe as dd
from typing import cast, Any as TypingAny
import certifi


# =========================================================
# GENERAL GOLD LAYER
# =========================================================

def gold_filings_by_form_month(ddf: Any) -> Any:
    """Return monthly filing counts grouped by form type.

    Args:
        ddf: Dask DataFrame with at least `date_filed` and `form_type` columns.

    Returns:
        Dask DataFrame with columns: `month`, `form_type`, `filings`.
        `month` is normalized to the month start timestamp.
    """
    x = ddf.copy()
    x["month"] = x["date_filed"].dt.to_period("M").dt.to_timestamp()

    return (
        x.groupby(["month", "form_type"])
        .size()
        .reset_index()
        .rename(columns={0: "filings"})
    )


def gold_forms_by_year(ddf: Any) -> Any:
    """Return yearly filing counts grouped by form type.

    Args:
        ddf: Dask DataFrame with `year` and `form_type` columns.

    Returns:
        Dask DataFrame with columns: `year`, `form_type`, `filings_count`.
    """
    return (
        ddf.groupby(["year", "form_type"]) 
        .size()
        .reset_index()
        .rename(columns={0: "filings_count"})
    )


def gold_top_ciks_by_volume(ddf: Any, top_n: int = 25) -> Any:
    """Return the top N companies by filing volume.

    Args:
        ddf: Dask DataFrame with `cik` and `company_name` columns.
        top_n: Number of top companies to return (default 25).

    Returns:
        Dask DataFrame with columns: `cik`, `company_name`, `filings`.
    """
    grouped = (
        ddf.groupby(["cik", "company_name"])
        .size()
        .reset_index()
        .rename(columns={0: "filings"})
    )
    return grouped.nlargest(top_n, "filings")


# =========================================================
# 10-K GOLD LAYER
# =========================================================

def _ddf_10k(ddf: Any) -> Any:
    """Return a DataFrame filtered to 10-K filings.

    Args:
        ddf: Input Dask DataFrame.

    Returns:
        A Dask DataFrame containing only rows where `form_type == '10-K'`.
    """
    ddf_mod = cast(TypingAny, ddf)
    return ddf_mod[ddf_mod["form_type"] == "10-K"]


def gold_10k_by_year(ddf: Any) -> Any:
    """Return counts of 10-K filings per year.

    Args:
        ddf: Dask DataFrame containing filing records.

    Returns:
        Dask DataFrame with columns: `year`, `filing_count`.
    """
    ddf_10k = _ddf_10k(ddf)
    ddf_mod = cast(TypingAny, ddf_10k)
    return (
        ddf_mod.groupby("year")
        .size()
        .reset_index()
        .rename(columns={0: "filing_count"})
    )


def gold_10k_top_ciks_by_volume(ddf: Any, top_n: int = 25) -> Any:
    """Return the top N companies by 10-K filing volume.

    Args:
        ddf: Dask DataFrame of filings.
        top_n: Number of top companies to return.

    Returns:
        Dask DataFrame with columns: `cik`, `company_name`, `filing_count`.
    """
    ddf_10k = _ddf_10k(ddf)
    grouped = (
        ddf_10k.groupby(["cik", "company_name"])
        .size()
        .reset_index()
        .rename(columns={0: "filing_count"})
    )
    return grouped.nlargest(top_n, "filing_count")


def gold_10k_recent(ddf: Any, years: int = 5) -> Any:
    """Return 10-K filings from the most recent N years.

    Args:
        ddf: Dask DataFrame of filings with a `year` column.
        years: Lookback window in years (default 5).

    Returns:
        Dask DataFrame with columns `cik`, `company_name`, `year`, `accession_number`,
        `edgar_url` and optionally `date_filed` if present.
    """
    max_year = int(ddf["year"].max().compute())
    cutoff_year = max_year - years

    ddf_10k = _ddf_10k(ddf)
    ddf_10k = ddf_10k[ddf_10k["year"] >= cutoff_year]

    # Include `date_filed` when available to allow sorting/display by filing date
    cols = ["cik", "company_name", "year", "accession_number", "edgar_url"]
    if "date_filed" in ddf_10k.columns:
        # Normalize to date-only ISO string (YYYY-MM-DD) so Gold layer doesn't include time
        ddf_10k = ddf_10k.assign(date_filed=ddf_10k["date_filed"].dt.strftime("%Y-%m-%d"))
        cols.append("date_filed")

    return ddf_10k[cols]


# =========================================================
# SIC BENCHMARKING (OPTIONAL ENRICHMENT)
# =========================================================

def gold_10k_by_sic(ddf: Any) -> Any:
    """Compute 10-K filing volumes grouped by SIC and company.

    Args:
        ddf: Dask DataFrame of filings; may include an optional `sic` column.

    Returns:
        Dask DataFrame with columns: `sic`, `company_name`, `filing_count`.
        If `sic` is missing it will be filled with "UNKNOWN".
    """
    ddf_10k = _ddf_10k(ddf)

    # Ensure SIC exists
    if "sic" not in ddf_10k.columns:
        ddf_10k = ddf_10k.assign(sic="UNKNOWN")

    grouped = (
        ddf_10k.groupby(["sic", "company_name"])
        .size()
        .reset_index()
        .rename(columns={0: "filing_count"})
    )
    return grouped

def gold_10k_by_sic_year(ddf: Any) -> Any:
    """Compute annual 10-K totals per SIC industry.

    Args:
        ddf: Dask DataFrame with `year`, `form_type`, and `sic` columns.

    Returns:
        Dask DataFrame with columns: `year`, `sic`, `industry_total`.
    """
    df = ddf[
        (ddf["form_type"] == "10-K") &
        (ddf["sic"].notnull())
    ]

    out = (
        df.groupby(["year", "sic"])
          .size()
          .reset_index()
          .rename(columns={0: "industry_total"})
    )

    return out



def gold_10k_company_vs_sic_share(ddf: Any) -> Any:
    """Compute company share of industry 10-K filings per SIC/year.

    Args:
        ddf: Dask DataFrame with `form_type`, `sic`, `company_name`, and `year`.

    Returns:
        Dask DataFrame where each row represents a company-year-sic with
        `filing_count` and the industry's `industry_total` for the same year and sic.
    """
    df = ddf[
        (ddf["form_type"] == "10-K") &
        (ddf["sic"].notnull()) &
        (ddf["company_name"].notnull())
    ]

    company = (
        df.groupby(["year", "sic", "company_name"])
          .size()
          .reset_index()
          .rename(columns={0: "filing_count"})
    )

    industry = (
        df.groupby(["year", "sic"])
          .size()
          .reset_index()
          .rename(columns={0: "industry_total"})
    )

    return company.merge(
        industry,
        on=["year", "sic"],
        how="left"
    )


# =========================================================
# OUTLIERS + TREND SIGNALS
# =========================================================

def gold_10k_anomalies(ddf: Any) -> Any:
    """Detect anomalies in yearly 10-K filing counts.

    Computes year-over-year percent change, rolling mean, z-score, and an
    `is_anomaly` boolean flag based on z-score threshold (abs(z) >= 2).

    Args:
        ddf: Dask DataFrame of filings suitable for `gold_10k_by_year`.

    Returns:
        Small Dask DataFrame (converted from pandas) with columns
        `year`, `filing_count`, `yoy_pct`, `rolling_mean`, `z_score`, `is_anomaly`.
    """
    # yearly table is tiny -> compute safely as pandas
    pdf = gold_10k_by_year(ddf).compute()
    if pdf.empty:
        dd_mod = cast(TypingAny, dd)
        return dd_mod.from_pandas(pd.DataFrame(), npartitions=1)

    pdf = pdf.sort_values("year").reset_index(drop=True)
    pdf["filing_count"] = pdf["filing_count"].astype(float)

    pdf["yoy_pct"] = pdf["filing_count"].pct_change() * 100.0
    pdf["rolling_mean"] = pdf["filing_count"].rolling(window=3, min_periods=1).mean()

    mean = pdf["filing_count"].mean()
    std = pdf["filing_count"].std(ddof=0)
    if std == 0 or np.isnan(std):
        pdf["z_score"] = 0.0
    else:
        pdf["z_score"] = (pdf["filing_count"] - mean) / std

    pdf["is_anomaly"] = pdf["z_score"].abs() >= 2.0
    dd_mod = cast(TypingAny, dd)
    return dd_mod.from_pandas(pdf, npartitions=1)
