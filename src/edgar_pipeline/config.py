"""Configuration helpers and Settings container.

This module provides a small `Settings` dataclass and `get_settings` which
reads required environment variables (including a check that `SEC_USER_AGENT`
is present).
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os

from dotenv import load_dotenv

# Explicitly load .env from project root
PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")

@dataclass(frozen=True)
class Settings:
    """Container for pipeline configuration read from the environment.

    Attributes:
        mongo_uri: MongoDB connection URI.
        mongo_db: Target MongoDB database name.
        sec_user_agent: Required SEC User-Agent header for API calls.
        edgar_data_dir: Local cache directory for downloaded indexes.
    """
    mongo_uri: str
    mongo_db: str
    sec_user_agent: str
    edgar_data_dir: Path



def get_settings() -> Settings:
    """Read environment variables and return a frozen `Settings` object.

    Raises:
        RuntimeError: if `SEC_USER_AGENT` is not set in the environment.
    """
    mongo_uri = os.getenv(
        "MONGO_URI",
        "mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=rs0",
    )
    mongo_db = os.getenv("MONGO_DB", "edgar")
    sec_user_agent = os.getenv("SEC_USER_AGENT", "").strip()
    edgar_data_dir = Path(os.getenv("EDGAR_DATA_DIR", "data/edgar_cache"))

    if not sec_user_agent:
        raise RuntimeError(
            "SEC_USER_AGENT is required. Set it in .env "
            "(example: 'Your Name your.email@example.com')."
        )

    return Settings(
        mongo_uri=mongo_uri,
        mongo_db=mongo_db,
        sec_user_agent=sec_user_agent,
        edgar_data_dir=edgar_data_dir,
    )
