"""Schema and fields management utilities."""

from typing import List, Set

from fivetran_connector_sdk import Logging as log


def _build_yaml_lines(fields: List[str], table_name: str) -> str:
    description = (
        "Dynamically created table from Bright Data search results. "
        "Fields are inferred from the API response structure."
    )
    lines = [
        "tables:",
        f"  {table_name}:",
        f"    description: {description}",
        "    fields:",
    ]
    for field in fields:
        lines.append(f"      - {field}")
    return "\n".join(lines) + "\n"


def update_fields_yaml(fields: Set[str], table_name: str = "search_results") -> None:
    """
    Update fields.yaml with discovered fields from Bright Data search results.

    Documents all fields found in the search results for reference.

    Args:
        fields: Set of field names discovered from search results
        table_name: Name of the table these fields belong to
    """
    sorted_fields = sorted(list(fields))
    yaml_content = _build_yaml_lines(sorted_fields, table_name)

    try:
        with open("fields.yaml", "w", encoding="utf-8") as yaml_file:
            yaml_file.write(yaml_content)
        log.info(
            f"Updated fields.yaml with {len(fields)} fields for table '{table_name}'"
        )
    except IOError as e:
        log.info(f"Warning: Could not update fields.yaml: {str(e)}")
