from __future__ import annotations

import logging
from edgar_pipeline.logging_config import configure_logging


def test_configure_logging_adds_stream_handler() -> None:
    # Ensure configuring logging doesn't raise and attaches a StreamHandler
    configure_logging(None)
    root = logging.getLogger()
    assert any(isinstance(h, logging.StreamHandler) for h in root.handlers)
