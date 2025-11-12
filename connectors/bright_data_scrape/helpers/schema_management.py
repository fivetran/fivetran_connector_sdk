"""Manage fields.yaml for the Bright Data scrape connector."""

from pathlib import Path
from typing import Set

import yaml

from fivetran_connector_sdk import Logging as log

FIELDS_FILE = Path("fields.yaml")
TABLE_NAME = "scrape_results"
TABLE_DESCRIPTION = (
    "Dynamically created table 'scrape_results' from Bright Data results. "
    "Fields are inferred from the API response structure."
)


def update_fields_yaml(fields: Set[str]) -> None:
    """Persist discovered scrape fields to fields.yaml."""
    sorted_fields = sorted(fields)
    payload = {
        "tables": {
            TABLE_NAME: {
                "description": TABLE_DESCRIPTION,
                "fields": sorted_fields,
            }
        }
    }

    try:
        with FIELDS_FILE.open("w", encoding="utf-8") as yaml_file:
            yaml.safe_dump(
                payload,
                yaml_file,
                default_flow_style=False,
                sort_keys=True,
                allow_unicode=True,
            )
        log.info(
            f"Updated fields.yaml with {len(sorted_fields)} fields for table '{TABLE_NAME}'"
        )
    except (IOError, yaml.YAMLError) as exc:
        log.info(f"Warning: Could not update fields.yaml: {exc}")
