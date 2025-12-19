from __future__ import annotations

import os
import pandas as pd
import streamlit as st
import altair as alt
from pymongo import MongoClient

import certifi
from dotenv import dotenv_values

# =====================================================
# Page config
# =====================================================
st.set_page_config(page_title="EDGAR Filings Analytics (2025)", layout="wide")
st.title("📊 EDGAR Filings Analytics Dashboard (2025)")

# =====================================================
# MongoDB connection (strict: read from .env only)
# =====================================================
# Load values explicitly from a .env file so we do not accidentally
# accept MONGO_URI from other environment sources.
_env = dotenv_values(".env")
MONGO_URI = _env.get("MONGO_URI")
MONGO_DB = _env.get("MONGO_DB") or "edgar"

if not MONGO_URI:
    st.error(
        "Missing `MONGO_URI` in `.env`. Please create a `.env` file with `MONGO_URI=<your mongodb uri>` (do not put secrets in source control)."
    )
    st.stop()

try:
    client = MongoClient(
        MONGO_URI,
        tls=True,
        tlsCAFile=certifi.where(),
        serverSelectionTimeoutMS=30000,
        socketTimeoutMS=30000,
        connectTimeoutMS=30000,
    )
    # fail fast: ensure the client can reach the server
    client.admin.command("ping")
    db = client[MONGO_DB]
except Exception as exc:  # pragma: no cover - runtime failure handling
    st.error(f"Unable to connect to MongoDB: {exc}")
    st.stop()

# =====================================================
# Helpers
# =====================================================
def load_collection(name: str) -> pd.DataFrame:
    """Load an entire Mongo collection into a pandas DataFrame for display.

    Args:
        name: Collection name in the configured Mongo database.

    Returns:
        pandas.DataFrame with the collection rows or an empty DataFrame.
    """
    docs = list(db[name].find({}, {"_id": 0}))
    return pd.DataFrame(docs) if docs else pd.DataFrame()


def kpi(label: str, value) -> None:
    """Display a simple KPI metric in the dashboard.

    Args:
        label: Metric label.
        value: Metric value (displayed as-is).
    """
    st.metric(label, value)

def center_dataframe(df: pd.DataFrame):
    """Center-align column headers and values for display."""
    return (
        df.style
        .set_properties(**{"text-align": "center"})
        .set_table_styles(
            [{"selector": "th", "props": [("text-align", "center")]}]
        )
    )

# =====================================================
# SECTION 0 — EXECUTIVE OVERVIEW
# =====================================================
st.header("📌 Executive Overview")

clean_count = db.clean_filings.count_documents({})
gold_count = db.gold_filings_by_form_month.count_documents({})

latest_year_doc = db.clean_filings.find_one(
    sort=[("year", -1)], projection={"year": 1}
)
latest_year = int(latest_year_doc["year"]) if latest_year_doc else "N/A"

c1, c2, c3 = st.columns(3)
with c1:
    kpi("Clean Filings (2025)", clean_count)
with c2:
    kpi("Gold Rows (Month × Form)", gold_count)
with c3:
    kpi("Latest Filing Year", latest_year)

st.caption(
    "Note: Raw filings are internal to the pipeline and not exposed at the dashboard layer."
)

st.divider()

# =====================================================
# SECTION 1 — FILING DISTRIBUTION (2025)
# =====================================================
st.header("📈 Filing Distribution")

df_trend = load_collection("gold_filings_by_form_month")

if df_trend.empty:
    st.warning("Gold trend data not available. Run the gold pipeline.")
else:
    df_trend["month"] = pd.to_datetime(df_trend["month"], errors="coerce")
    df_trend = df_trend.dropna(subset=["month"])
    df_trend["filings"] = pd.to_numeric(df_trend["filings"], errors="coerce").fillna(0)

    forms = sorted(df_trend["form_type"].dropna().unique())
    selected_forms = st.multiselect(
        "Select Form Types",
        forms,
        default=forms[: min(6, len(forms))],
    )

    # Quarter filter (All / Q1-Q4)
    df_trend["quarter"] = df_trend["month"].dt.quarter
    quarter_choice = st.radio(
        "Quarter",
        ["All", "Q1", "Q2", "Q3", "Q4"],
        horizontal=True,
    )
    df_plot = df_trend[df_trend["form_type"].isin(selected_forms)]
    if quarter_choice != "All":
        q = int(quarter_choice[1])
        df_plot = df_plot[df_plot["quarter"] == q]

    chart = (
        alt.Chart(df_plot)
        .mark_line()
        .encode(
            x=alt.X("month:T", title="Month (2025)"),
            y=alt.Y("filings:Q", title="Number of Filings"),
            color=alt.Color("form_type:N", title="Form Type"),
            tooltip=["month:T", "form_type:N", "filings:Q"],
        )
        .properties(height=320)
    )

    st.altair_chart(chart, width="stretch")

st.divider()

# =====================================================
# SECTION 2 — TOP FILERS
# =====================================================
st.header("🏢 Top Filing Companies")

# Single control to toggle dataset (replaces previous tabs)
selection = st.selectbox("Dataset", ["All Forms", "10-K Only"], index=0)

if selection == "All Forms":
    df_top = load_collection("gold_top_ciks")
    if df_top.empty:
        st.info("Top filer data not available.")
    else:
        df_top["filings"] = pd.to_numeric(
            df_top["filings"], errors="coerce"
        ).fillna(0).astype(int)

        df_top = df_top.sort_values("filings", ascending=False).head(20)

        # Altair chart explicitly sorted descending by filings
        chart_top = (
            alt.Chart(df_top)
            .mark_bar()
            .encode(
                x=alt.X("company_name:N", sort=alt.SortField("filings", order="descending"), title=None),
                y=alt.Y("filings:Q", title="Filings"),
                tooltip=["company_name:N", "filings:Q"],
            )
            .properties(height=320)
        )
        st.altair_chart(chart_top, width="stretch")
        st.dataframe(
            center_dataframe(df_top),
            width="stretch",
        )

else:
    df_10k_top = load_collection("gold_10k_top_ciks")
    if df_10k_top.empty:
        st.info("10-K filer data not available.")
    else:
        df_10k_top["filing_count"] = pd.to_numeric(
            df_10k_top["filing_count"], errors="coerce"
        ).fillna(0).astype(int)

        df_10k_top = df_10k_top.sort_values(
            "filing_count", ascending=False
        ).head(20)

        # Altair chart explicitly sorted descending by filing_count
        chart_10k = (
            alt.Chart(df_10k_top)
            .mark_bar()
            .encode(
                x=alt.X("company_name:N", sort=alt.SortField("filing_count", order="descending"), title=None),
                y=alt.Y("filing_count:Q", title="10-K Filings"),
                tooltip=["company_name:N", "filing_count:Q"],
            )
            .properties(height=320)
        )
        st.altair_chart(chart_10k, width="stretch")
        st.dataframe(
            center_dataframe(df_10k_top),
            width="stretch",
        )

st.divider()

# =====================================================
# SECTION 3 — RECENT 10-K FILINGS
# =====================================================
st.header("📘 Recent 10-K Filings")

df_recent_10k = load_collection("gold_10k_recent")

if df_recent_10k.empty:
    st.info("Recent 10-K data not available.")
else:
    date_col = None
    for candidate in [
        "date_filed",
        "filing_date",
        "accepted_datetime",
        "report_date",
        "filed_as_of_date",
    ]:
        if candidate in df_recent_10k.columns:
            date_col = candidate
            break

    if date_col:
        # parse and format date column to date-only string for clean display
        df_recent_10k[date_col] = pd.to_datetime(
            df_recent_10k[date_col], errors="coerce"
        ).dt.strftime("%Y-%m-%d")
        df_recent_10k = df_recent_10k.sort_values(date_col, ascending=False)
        st.caption(f"Sorted by `{date_col}`")
    else:
        st.warning("Showing most recent records (no date column found).")

    st.dataframe(
        center_dataframe(df_recent_10k.head(25)),
        width="stretch",
    )

# =====================================================
# Footer
# =====================================================
st.caption(
    "SEC EDGAR • MongoDB Atlas • Dask • Streamlit • Gold-Layer Analytics | Big Data Tools Capstone Project"
)
