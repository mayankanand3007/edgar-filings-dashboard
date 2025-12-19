EDGAR Filings Big Data Analytics

MongoDB • Docker • Dask • uv • Pydantic • mypy • PyTest • Streamlit

Project Overview

This project implements an end-to-end Big Data pipeline using SEC EDGAR filings and MongoDB deployed as a Docker-based replica set.
The pipeline ingests 750,000+ filing records, performs data cleaning and schema validation, builds gold-layer analytical aggregations, and visualizes insights through an interactive Streamlit dashboard.

The solution follows industry-style data engineering best practices and satisfies all capstone rubric requirements.

Big Data Platform & Architecture
Platform Choice

MongoDB deployed via Docker Compose

3-node Replica Set (rs0)

mongo1 (PRIMARY)

mongo2 (SECONDARY)

mongo3 (SECONDARY)

Why MongoDB?

Native support for large semi-structured datasets

Horizontal scalability

Replica sets demonstrate distributed system behavior

Excellent fit for JSON-like EDGAR filings

Architecture Diagram

See:

diagrams/architecture.mmd

High-Level Data Flow
SEC EDGAR Index
      ↓
Ingest (Python + Dask)
      ↓
MongoDB Raw Layer
      ↓
Clean & Validate (Pydantic)
      ↓
MongoDB Clean Layer
      ↓
Gold Aggregations
      ↓
MongoDB Gold Collections
      ↓
Streamlit Dashboard

Dataset Description

Source
SEC EDGAR Quarterly Master Index

https://www.sec.gov/Archives/edgar/full-index/

Volume

750,000+ rows (configurable by year range)

Easily scalable to millions of records

Core Columns (8+)

cik

company_name

form_type

date_filed

year

quarter

accession_number

file_name

edgar_url

ingest_ts

Processing Stack (Rubric Aligned)
Requirement	Tool
Big Data Processing	Dask
Environment Management	uv
Schema Validation	Pydantic
Type Checking	mypy
Logging	Python logging
Testing	PyTest (3+ tests)
Visualization	Streamlit
Project Structure
edgar-mongo-bigdata/
│
├── docker/
│   └── mongo-init/
│       └── init.js
│
├── src/
│   └── edgar_pipeline/
│       ├── ingest/
│       ├── clean/
│       ├── aggregate/
│       ├── enrich/
│       ├── models.py
│       ├── db.py
│       ├── cli.py
│
├── streamlit_app/
│   └── app.py
│
├── tests/
│   ├── test_models.py
│   ├── test_cleaning.py
│   └── test_aggregation.py
│
├── diagrams/
│   └── architecture.mmd
│
├── docker-compose.yml
├── pyproject.toml
├── uv.lock
├── README.md

Setup & Installation
1️⃣ Environment Variables
cp .env.example .env


Edit .env:

MONGO_URI="mongodb+srv://user_name:password@cluster0.afgqezq.mongodb.net/?appName=Cluster0"
MONGO_DB="edgar"
SEC_USER_AGENT=YourName your.email@rowan.edu
EDGAR_DATA_DIR="data/edgar_cache"

2️⃣ Start MongoDB Replica Set
docker compose up -d


Verify replica set:

docker exec -it mongo1 mongosh --eval "rs.status()"

3️⃣ Install Dependencies
uv sync --all-extras

Running the Pipeline

Important: Global arguments must be specified before the command.

🔹 Ingest (Raw Layer)
PYTHONPATH=src uv run python -m edgar_pipeline.cli \
  --from-year 2025 \
  --to-year 2025 \
  ingest


Raw Layer Verification

db.raw_filings.countDocuments()

🔹 Clean & Validate (Clean Layer)
PYTHONPATH=src uv run python -m edgar_pipeline.cli clean


Cleaning Includes

Missing value handling

Text normalization

Date standardization

Deduplication

Pydantic schema validation

🔹 Aggregations (Gold Layer)
PYTHONPATH=src uv run python -m edgar_pipeline.cli gold


Gold Collections Created

gold_filings_by_form_month

gold_forms_by_year

gold_top_ciks

gold_10k_by_year

gold_10k_top_ciks

gold_10k_recent

gold_10k_by_sic (optional enrichment)

🔹 Run Everything (Optional)
PYTHONPATH=src uv run python -m edgar_pipeline.cli all

Streamlit Dashboard
uv run streamlit run streamlit_app/app.py


Open in browser:

http://localhost:8501

Dashboard Sections

Executive Overview (KPIs)

Filing Trends (Monthly / Yearly)

Top Filing Companies (All Forms & 10-K)

Long-Term 10-K Trends

Outlier Detection (Spikes / Drops)

Industry Benchmarking (optional enrichment)

Note: Raw-layer data is intentionally not exposed in the dashboard.

Industry Benchmarking (Optional Enrichment)

SIC / NAICS codes are not included in the EDGAR master index.

To enable benchmarking:

Integrate SEC Company Facts API

Map CIK → SIC / NAICS

Aggregate into gold_10k_by_sic

The current implementation demonstrates architectural extensibility.

Quality Gates
uv run pytest
uv run mypy src

Indexing & Performance

Indexes are defined in:

docker/mongo-init/init.js


Including:

Unique index on accession_number

Compound index on (cik, date_filed)

Index on form_type

These indexes ensure efficient querying at scale.