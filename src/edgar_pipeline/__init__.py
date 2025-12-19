"""edgar_pipeline package.

Contains modules for ingesting SEC EDGAR master index files, loading raw documents
into MongoDB, cleaning & validating filings, optional enrichment, building
Gold-layer aggregations, and utilities for serving a Streamlit dashboard.

Architecture:
- Raw → Clean → Gold layers stored in MongoDB
- Dask is used for partitioned/scalable transforms
- Pydantic models validate the Clean layer
"""

__all__ = ["__version__"]
__version__ = "0.1.0"
