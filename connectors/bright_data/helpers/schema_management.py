"""Schema and fields management utilities."""

from typing import Set

import yaml
from fivetran_connector_sdk import Logging as log


def update_fields_yaml(fields: Set[str], table_name: str = "search_results") -> None:
    """
    Update fields.yaml with discovered fields from Bright Data search results.

    Documents all fields found in the search results for reference.

    Args:
        fields: Set of field names discovered from search results
        table_name: Name of the table these fields belong to
    """
    fields_data = {
        "tables": {
            table_name: {
                "fields": sorted(list(fields)),
                "description": "Dynamically created table from Bright Data search results. Fields are inferred from the API response structure.",
            }
        }
    }

    try:
        with open("fields.yaml", "w", encoding="utf-8") as yaml_file:
            yaml.dump(fields_data, yaml_file, default_flow_style=False, sort_keys=False)
        log.info(
            f"Updated fields.yaml with {len(fields)} fields for table '{table_name}'"
        )
    except (IOError, yaml.YAMLError) as e:
        log.info(f"Warning: Could not update fields.yaml: {str(e)}")
