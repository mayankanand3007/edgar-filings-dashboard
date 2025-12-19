"""Utilities to configure consistent logging across the pipeline."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

def configure_logging(log_path: Path | None = None, level: int = logging.INFO) -> None:
    """Configure root logging handlers and formatting.

    Args:
        log_path: Optional path to a file where logs will be written.
        level: Logging level (defaults to INFO).
    """
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if log_path is not None:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_path, encoding="utf-8"))

    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=handlers,
    )
