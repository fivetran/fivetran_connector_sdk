"""Utilities for flattening Bright Data scrape results."""

import json
from typing import Any, Dict, List, Set


def flatten_dict(
    data: Any, parent_key: str = "", separator: str = "_", max_depth: int = 10
) -> Dict[str, Any]:
    """Flatten nested dictionaries and lists into a single level."""
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
                items.append((new_key, json.dumps(value) if value else "[]"))
            else:
                items.append((new_key, value))
    elif isinstance(data, list):
        return {parent_key: json.dumps(data) if data else "[]"}
    else:
        return {parent_key: data}

    return dict(items)


def collect_all_fields(results: List[Dict[str, Any]]) -> Set[str]:
    """Return the union of keys that appear across processed results."""
    all_fields: Set[str] = set()
    for result in results:
        all_fields.update(result.keys())
    return all_fields


def process_scrape_result(result: Any, url: str, result_index: int) -> Dict[str, Any]:
    """
    Flatten an individual scrape result and add metadata columns.
    """
    base_fields = {
        "url": url,
        "result_index": result_index,
        "position": result_index + 1,
    }

    if not isinstance(result, dict):
        base_fields["raw_data"] = str(result)
        return base_fields

    flattened = flatten_dict(result)
    flattened.update(base_fields)
    return flattened
