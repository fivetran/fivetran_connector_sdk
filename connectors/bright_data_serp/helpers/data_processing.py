"""Utility helpers for transforming Bright Data SERP responses."""

from typing import Any, Dict, Iterable, Set


def flatten_dict(data: Dict[str, Any], parent_key: str = "", sep: str = "_") -> Dict[str, Any]:
    """
    Flatten a nested dictionary into a single depth dictionary.
    Nested keys are concatenated using the provided separator.
    """
    items: Dict[str, Any] = {}
    for key, value in data.items():
        new_key = f"{parent_key}{sep}{key}" if parent_key else key
        if isinstance(value, dict):
            items.update(flatten_dict(value, new_key, sep=sep))
        else:
            items[new_key] = value
    return items


def collect_all_fields(results: Iterable[Dict[str, Any]]) -> Set[str]:
    """Collect the union of keys across all result dictionaries."""
    fields: Set[str] = set()
    for result in results:
        fields.update(result.keys())
    return fields


def process_search_result(result: Any, query: str, result_index: int) -> Dict[str, Any]:
    """
    Transform a raw search result into a flattened dictionary suitable for upsert.
    """
    base_fields: Dict[str, Any] = {
        "query": query,
        "result_index": result_index,
        "position": result_index + 1,
    }

    if not isinstance(result, dict):
        base_fields["raw_response"] = str(result)
        return base_fields

    flattened = flatten_dict(result)
    flattened.update(base_fields)
    return flattened

