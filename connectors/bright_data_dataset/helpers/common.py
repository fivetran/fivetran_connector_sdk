"""Shared constants and utilities for Bright Data Dataset helpers."""

import os
from typing import Any

from dotenv import load_dotenv
from requests import Response

load_dotenv()

_DEFAULT_BASE_URL = "https://api.brightdata.com"
BRIGHT_DATA_BASE_URL = (
    os.getenv("BRIGHT_DATA_BASE_URL", _DEFAULT_BASE_URL) or _DEFAULT_BASE_URL
)
DEFAULT_TIMEOUT_SECONDS = 300
RETRY_STATUS_CODES = {408, 429, 500, 502, 503, 504}
MAX_SNAPSHOT_WAIT_SECONDS = 300
SNAPSHOT_POLL_INTERVAL_SECONDS = 5


def parse_response_payload(response: Response) -> Any:
    """Return JSON payload when available, otherwise raw text."""
    try:
        return response.json()
    except ValueError:
        return response.text


def extract_error_detail(response: Response) -> str:
    """Extract error detail from a failed Bright Data response."""
    try:
        payload = response.json()
        if isinstance(payload, dict):
            for key in ("error", "message", "detail", "details"):
                if key in payload:
                    return str(payload[key])
            return str(payload)
        return str(payload)
    except ValueError:
        return response.text
