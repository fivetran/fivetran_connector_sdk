"""
Bright Data SERP Connector

This connector fetches search results from Bright Data's SERP REST API and upserts
the flattened data to Fivetran. It supports single or multiple search queries and
documents discovered fields in fields.yaml for reference.
"""

import json
from typing import Any, Dict, List, Optional

from helpers import (collect_all_fields, perform_search, process_search_result,
                     update_fields_yaml, validate_configuration)

from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op


def schema(_config: dict) -> List[Dict[str, Any]]:
    """
    Define the schema delivered by the connector.

    Only the primary key is declared here; column types are inferred by Fivetran.
    """
    return [
        {
            "table": "search_results",
            "primary_key": [
                "query",
                "result_index",
            ],
        }
    ]


def update(
    configuration: dict, state: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Fetch data from Bright Data's SERP REST API and upsert it into Fivetran.
    """
    validate_configuration(configuration=configuration)

    api_token = configuration.get("api_token")
    new_state = dict(state) if state else {}

    try:
        search_query_input = configuration.get("search_query", "")
        if search_query_input:
            new_state = _process_search_endpoint(
                configuration, search_query_input, new_state, api_token
            )

        op.checkpoint(state=new_state)
        return new_state
    except Exception as exc:
        raise RuntimeError(f"Failed to sync data from Bright Data: {str(exc)}") from exc


def _process_search_endpoint(
    configuration: dict,
    search_query_input: str,
    state: Dict[str, Any],
    api_token: str,
) -> Dict[str, Any]:
    """
    Handle search queries: fetch results and upsert them to the destination.
    """
    queries = _parse_query_input(search_query_input)
    if not queries:
        raise ValueError("search_query cannot be empty")

    search_engine = configuration.get("search_engine")
    country = configuration.get("country")
    search_zone = configuration.get("search_zone") or configuration.get("zone")

    query_param = queries if len(queries) > 1 else queries[0]
    search_results = perform_search(
        api_token=api_token,
        query=query_param,
        search_engine=search_engine,
        country=country,
        zone=search_zone,
    )

    processed_results: List[Dict[str, Any]] = []

    if isinstance(search_results, list) and len(queries) > 1:
        for query_idx, query in enumerate(queries):
            query_results = (
                search_results[query_idx] if query_idx < len(search_results) else []
            )
            if isinstance(query_results, list):
                for result_idx, result in enumerate(query_results):
                    processed_results.append(
                        process_search_result(result, query, result_idx)
                    )
            elif isinstance(query_results, dict):
                processed_results.append(process_search_result(query_results, query, 0))
    elif isinstance(search_results, list):
        for idx, result in enumerate(search_results):
            processed_results.append(process_search_result(result, queries[0], idx))
    elif isinstance(search_results, dict):
        processed_results.append(process_search_result(search_results, queries[0], 0))

    if processed_results:
        log.info(f"Upserting {len(processed_results)} search results to Fivetran")
        all_fields = collect_all_fields(processed_results)
        update_fields_yaml(all_fields)

        for result in processed_results:
            columnar_data = {field: [result.get(field)] for field in all_fields}
            op.upsert("search_results", columnar_data)

        state.update(
            {
                "last_search_queries": queries,
                "last_search_count": len(processed_results),
            }
        )

    return state


def _parse_query_input(query_input: Any) -> List[str]:
    """
    Support multiple input formats for search_query.
    """
    if not query_input:
        return []

    if isinstance(query_input, list):
        return [
            item.strip()
            for item in query_input
            if isinstance(item, str) and item.strip()
        ]

    if isinstance(query_input, str):
        try:
            parsed = json.loads(query_input)
            if isinstance(parsed, list):
                return [
                    item.strip()
                    for item in parsed
                    if isinstance(item, str) and item.strip()
                ]
            if isinstance(parsed, str) and parsed.strip():
                return [parsed.strip()]
        except (json.JSONDecodeError, TypeError):
            pass

        if "," in query_input:
            return [item.strip() for item in query_input.split(",") if item.strip()]
        if "\n" in query_input:
            return [item.strip() for item in query_input.split("\n") if item.strip()]
        return [query_input.strip()] if query_input.strip() else []

    return []


# Initialize the connector
connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    with open("configuration.json", "r", encoding="utf-8") as f:
        local_configuration = json.load(f)
    connector.debug(configuration=local_configuration)
