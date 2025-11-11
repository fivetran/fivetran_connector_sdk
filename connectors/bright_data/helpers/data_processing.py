"""Data processing utilities for flattening and normalizing Bright Data results."""

import json
from typing import Any, Dict, List, Set


def flatten_dict(
    data: Any, parent_key: str = "", separator: str = "_", max_depth: int = 10
) -> Dict[str, Any]:
    """
    Flatten a nested dictionary into a single-level dictionary.

    Converts nested structures like {"a": {"b": 1}} to {"a_b": 1}.
    Handles lists by converting them to JSON strings.

    Args:
        data: The dictionary or value to flatten
        parent_key: The parent key prefix for nested keys
        separator: Separator to use between nested keys
        max_depth: Maximum nesting depth to prevent infinite recursion

    Returns:
        Flattened dictionary with all nested keys as top-level keys
    """
    if max_depth <= 0:
        return {parent_key: json.dumps(data) if data else None}

    items: List[tuple] = []

    if isinstance(data, dict):
        for key, value in data.items():
            new_key = f"{parent_key}{separator}{key}" if parent_key else key
            if isinstance(value, dict):
                items.extend(
                    flatten_dict(value, new_key, separator, max_depth - 1).items()
                )
            elif isinstance(value, list):
                # Convert lists to JSON strings for storage
                items.append((new_key, json.dumps(value) if value else "[]"))
            else:
                items.append((new_key, value))
    elif isinstance(data, list):
        # Convert lists to JSON strings
        return {parent_key: json.dumps(data) if data else "[]"}
    else:
        # Primitive value
        return {parent_key: data}

    return dict(items)


def collect_all_fields(results: List[Dict[str, Any]]) -> Set[str]:
    """
    Collect all unique field names from a list of result dictionaries.

    Args:
        results: List of processed result dictionaries

    Returns:
        Set of all unique field names found across all results
    """
    all_fields: Set[str] = set()
    for result in results:
        all_fields.update(result.keys())
    return all_fields


def process_scrape_result(result: Any, url: str, result_index: int) -> Dict[str, Any]:
    """
    Process a single scrape result by flattening all nested dictionaries.

    Flattens the entire result structure into a flat key-value dictionary
    where nested keys become top-level columns (e.g., "metadata_title" from {"metadata": {"title": "..."}}).

    Args:
        result: Scrape result dictionary from Bright Data parse_content
        url: The URL that was scraped
        result_index: Index of this result in the result set

    Returns:
        Flattened dictionary with all fields as top-level keys
    """
    # Start with base fields that identify the record
    base_fields = {
        "url": url,
        "result_index": result_index,
        "position": result_index + 1,
    }

    if not isinstance(result, dict):
        base_fields["raw_data"] = str(result)
        return base_fields

    # Flatten the entire result dictionary
    flattened = flatten_dict(result)

    # Merge base fields with flattened fields (base fields take precedence)
    return {**flattened, **base_fields}


def process_unlocker_result(
    result: Any, requested_url: str, result_index: int
) -> Dict[str, Any]:
    """
    Process a single web unlocker result by flattening nested dictionaries.

    Args:
        result: Unlocker result dictionary returned by perform_web_unlocker
        requested_url: The URL that was requested
        result_index: Index of this result in the result set

    Returns:
        Flattened dictionary with all fields as top-level keys
    """
    base_fields = {
        "requested_url": requested_url,
        "result_index": result_index,
        "position": result_index + 1,
    }

    if not isinstance(result, dict):
        base_fields["raw_data"] = str(result)
        return base_fields

    flattened = flatten_dict(result)
    return {**flattened, **base_fields}
