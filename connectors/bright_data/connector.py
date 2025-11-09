"""
Bright Data Fivetran Connector

This connector demonstrates how to fetch data from Bright Data's APIs and upsert it
into Fivetran destination. Supports both SERP API (search) and Web Unlocker (scrape)
endpoints. The connector dynamically creates tables with flattened dictionary structures,
allowing Fivetran to infer column types automatically.

See the Technical Reference documentation:
https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update

See the Best Practices documentation:
https://fivetran.com/docs/connectors/connector-sdk/best-practices
"""

import json
from typing import Any, Dict, List, Optional

from brightdata import bdclient
from helpers import (
    collect_all_fields,
    perform_scrape,
    perform_search,
    process_scrape_result,
    process_search_result,
    update_fields_yaml,
    validate_configuration,
)

from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op


def schema(_config: dict) -> List[Dict[str, Any]]:
    """
    Define the schema function which lets you configure the schema your connector delivers.

    This connector uses dynamic schema discovery - only primary keys are defined here,
    and Fivetran infers column types automatically from the flattened search result data.
    All discovered fields are documented in fields.yaml.

    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema

    Args:
        _config: A dictionary that holds the configuration settings for the connector
                 (required by SDK interface but not used for dynamic schema)

    Returns:
        List of table schema definitions with primary keys only.
        Column types are inferred by Fivetran from the data.
    """
    tables = [
        {
            "table": "search_results",  # Name of the table in the destination
            "primary_key": [
                "query",
                "result_index",
            ],  # Primary key columns for the table
            # Note: columns are not defined here - Fivetran infers types from data
        }
    ]

    # Add scrape_results table if scrape endpoint is configured
    # Schema will be dynamically discovered from scrape results
    tables.append(
        {
            "table": "scrape_results",
            "primary_key": [
                "url",
                "result_index",
            ],
        }
    )

    return tables


def update(
    configuration: dict, state: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Define the update function, which is a required function, and is called by Fivetran during each sync.

    This function fetches data from Bright Data's SERP API (search) and/or Web Unlocker (scrape),
    flattens nested dictionaries into columns, and upserts the data into Fivetran.
    The connector dynamically discovers fields from the API response and documents them in fields.yaml.

    See the technical reference documentation for more details on the update function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update

    Args:
        configuration: A dictionary containing connection details (api_token, search_query, scrape_url, etc.)
        state: A dictionary containing state information from previous runs.
               The state dictionary is empty for the first sync or for any full re-sync.

    Returns:
        Updated state dictionary with sync information

    Raises:
        ValueError: If configuration validation fails
        RuntimeError: If data sync fails
    """
    # Validate the configuration to ensure it contains all required values
    validate_configuration(configuration=configuration)

    api_token = configuration.get("api_token")
    client = bdclient(api_token=api_token)

    new_state = dict(state) if state else {}

    try:
        # Process search endpoint if configured
        search_query_input = configuration.get("search_query", "")
        if search_query_input:
            new_state = _process_search_endpoint(
                configuration, search_query_input, new_state
            )

        # Process scrape endpoint if configured
        scrape_url_input = configuration.get("scrape_url", "")
        if scrape_url_input:
            new_state = _process_scrape_endpoint(
                client, configuration, scrape_url_input, new_state
            )

        # Checkpoint state after processing all endpoints
        # Save the progress by checkpointing the state. This is important for ensuring that
        # the sync process can resume from the correct position in case of next sync or interruptions.
        # Learn more about checkpointing:
        # https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation
        op.checkpoint(state=new_state)

        return new_state

    except Exception as e:
        # In case of an exception, raise a runtime error with a clear message
        raise RuntimeError(f"Failed to sync data from Bright Data: {str(e)}") from e


def _process_search_endpoint(
    configuration: dict,
    search_query_input: str,
    state: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Process search endpoint: fetch search results and upsert to Fivetran.

    Args:
        configuration: Connector configuration
        search_query_input: Search query string or list
        state: Current state dictionary

    Returns:
        Updated state dictionary
    """
    # Parse input - supports multiple formats for Fivetran UI:
    # 1. JSON array string: '["Ghana","Togo"]'
    # 2. Comma-separated: 'Ghana, Togo'
    # 3. Newline-separated: 'Ghana\nTogo'
    # 4. Single value: 'Ghana'
    if search_query_input:
        # Try JSON first
        try:
            parsed = json.loads(search_query_input)
            if isinstance(parsed, list):
                queries = parsed
            else:
                queries = [parsed]
        except (json.JSONDecodeError, TypeError):
            # Not JSON - check for comma or newline separators
            if isinstance(search_query_input, str):
                # Check for comma-separated values
                if "," in search_query_input:
                    queries = [
                        q.strip() for q in search_query_input.split(",") if q.strip()
                    ]
                # Check for newline-separated values
                elif "\n" in search_query_input:
                    queries = [
                        q.strip() for q in search_query_input.split("\n") if q.strip()
                    ]
                else:
                    # Single value
                    queries = (
                        [search_query_input.strip()]
                        if search_query_input.strip()
                        else []
                    )
            elif isinstance(search_query_input, list):
                queries = search_query_input
            else:
                queries = []
    else:
        queries = []

    if not queries:
        raise ValueError("search_query cannot be empty")

    # Get optional parameters
    search_engine = configuration.get("search_engine")
    country = configuration.get("country")

    # Perform Bright Data search
    query_param = queries if len(queries) > 1 else queries[0]
    search_zone = configuration.get("search_zone") or configuration.get("zone")
    api_token = configuration.get("api_token")
    search_results = perform_search(
        api_token=api_token,
        query=query_param,
        search_engine=search_engine,
        country=country,
        zone=search_zone,
    )

    # Process and normalize search results
    processed_results = []

    # Handle results from multiple queries (list of result sets)
    if isinstance(search_results, list) and len(queries) > 1:
        for query_idx, query in enumerate(queries):
            query_results = (
                search_results[query_idx] if query_idx < len(search_results) else []
            )

            if isinstance(query_results, list):
                for result_idx, result in enumerate(query_results):
                    processed_result = process_search_result(result, query, result_idx)
                    processed_results.append(processed_result)
            elif isinstance(query_results, dict):
                processed_result = process_search_result(query_results, query, 0)
                processed_results.append(processed_result)

    # Handle results from single query
    elif isinstance(search_results, list):
        for idx, result in enumerate(search_results):
            processed_result = process_search_result(result, queries[0], idx)
            processed_results.append(processed_result)
    elif isinstance(search_results, dict):
        processed_result = process_search_result(search_results, queries[0], 0)
        processed_results.append(processed_result)

    # Upsert search results to Fivetran
    # Upsert each result separately to ensure Fivetran creates separate rows
    # This avoids type inference issues where lists are stored as JSON arrays
    if processed_results:
        log.info(f"Upserting {len(processed_results)} search results to Fivetran")

        all_fields = collect_all_fields(processed_results)
        update_fields_yaml(all_fields, "search_results")

        # Upsert each result individually to ensure separate rows
        for result in processed_results:
            # Create columnar data for single record
            single_record_data = {}
            for field in all_fields:
                # Store as single-item list to maintain columnar format
                single_record_data[field] = [result.get(field)]

            # Upsert this single record
            op.upsert("search_results", single_record_data)

        state.update(
            {
                "last_search_queries": queries,
                "last_search_count": len(processed_results),
            }
        )

    return state


def _process_scrape_endpoint(
    client: bdclient,
    configuration: dict,
    scrape_url_input: str,
    state: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Process scrape endpoint: fetch scrape results and upsert to Fivetran.

    Args:
        configuration: Connector configuration
        scrape_url_input: URL string or list of URLs
        state: Current state dictionary

    Returns:
        Updated state dictionary
    """
    # Parse input - supports multiple formats for Fivetran UI:
    # 1. JSON array string: '["https://example.com","https://example2.com"]'
    # 2. Comma-separated: 'https://example.com, https://example2.com'
    # 3. Newline-separated: 'https://example.com\nhttps://example2.com'
    # 4. Single value: 'https://example.com'
    if scrape_url_input:
        # Try JSON first
        try:
            parsed = json.loads(scrape_url_input)
            if isinstance(parsed, list):
                urls = parsed
            else:
                urls = [parsed]
        except (json.JSONDecodeError, TypeError):
            # Not JSON - check for comma or newline separators
            if isinstance(scrape_url_input, str):
                # Check for comma-separated values
                if "," in scrape_url_input:
                    urls = [
                        url.strip()
                        for url in scrape_url_input.split(",")
                        if url.strip()
                    ]
                # Check for newline-separated values
                elif "\n" in scrape_url_input:
                    urls = [
                        url.strip()
                        for url in scrape_url_input.split("\n")
                        if url.strip()
                    ]
                else:
                    # Single value
                    urls = (
                        [scrape_url_input.strip()] if scrape_url_input.strip() else []
                    )
            elif isinstance(scrape_url_input, list):
                urls = scrape_url_input
            else:
                urls = []
    else:
        urls = []

    if not urls:
        raise ValueError("scrape_url cannot be empty")

    # Get optional parameters
    country = configuration.get("country")
    data_format = configuration.get("data_format")
    format_param = configuration.get("format")
    method = configuration.get("method")

    # Parse async_request from configuration (Fivetran requires all config values to be strings)
    # Defaults to True to match perform_scrape() function signature
    async_request = True
    async_request_str = configuration.get("async_request", "").lower()
    if async_request_str in ("false", "0", "no"):
        async_request = False

    # Perform Bright Data scrape
    # For multiple URLs, scrape them individually to ensure each URL gets scraped
    # The SDK's batch scraping might return empty results for some URLs
    if len(urls) > 1:
        scrape_results = []
        for url in urls:
            try:
                result = perform_scrape(
                    client=client,
                    url=url,
                    country=country,
                    data_format=data_format,
                    format_param=format_param,
                    method=method,
                    async_request=async_request,
                )
                # Normalize to list
                if isinstance(result, list):
                    scrape_results.extend(result)
                else:
                    scrape_results.append(result)
            except (RuntimeError, ValueError) as exc:
                log.info(
                    f"Error scraping {url}: {str(exc)}. Continuing with other URLs."
                )
                # Continue with other URLs even if one fails
    else:
        # Single URL - use as-is
        url_param = urls[0]
        scrape_results = perform_scrape(
            client=client,
            url=url_param,
            country=country,
            data_format=data_format,
            format_param=format_param,
            method=method,
            async_request=async_request,
        )
        # Normalize to list
        if not isinstance(scrape_results, list):
            scrape_results = [scrape_results]

    # Process and normalize scrape results
    processed_results = []

    # Handle results from multiple URLs
    if isinstance(scrape_results, list):
        # Check if we have one result per URL or multiple results
        if len(scrape_results) == len(urls):
            # One result per URL - map directly
            for url_idx, url in enumerate(urls):
                result = scrape_results[url_idx]
                if isinstance(result, list):
                    # Result is a list of items for this URL
                    for result_idx, item in enumerate(result):
                        processed_result = process_scrape_result(item, url, result_idx)
                        processed_results.append(processed_result)
                else:
                    # Single result for this URL
                    processed_result = process_scrape_result(result, url, 0)
                    processed_results.append(processed_result)
        else:
            # Results don't match URL count - map sequentially
            for idx, result in enumerate(scrape_results):
                # Map result to URL (cycle through URLs if more results than URLs)
                url = urls[idx % len(urls)]
                processed_result = process_scrape_result(result, url, idx)
                processed_results.append(processed_result)
    elif isinstance(scrape_results, dict):
        # Single result - use first URL
        processed_result = process_scrape_result(scrape_results, urls[0], 0)
        processed_results.append(processed_result)
    else:
        # Unexpected type - try to process as single result
        processed_result = process_scrape_result(scrape_results, urls[0], 0)
        processed_results.append(processed_result)

    # Upsert scrape results to Fivetran
    # Upsert each result separately to ensure Fivetran creates separate rows
    # This avoids type inference issues where lists are stored as JSON arrays
    if processed_results:
        log.info(f"Upserting {len(processed_results)} scrape results to Fivetran")

        all_fields = collect_all_fields(processed_results)
        update_fields_yaml(all_fields, "scrape_results")

        # Upsert each result individually to ensure separate rows
        for result in processed_results:
            # Create columnar data for single record
            single_record_data = {}
            for field in all_fields:
                # Store as single-item list to maintain columnar format
                single_record_data[field] = [result.get(field)]

            # Upsert this single record
            op.upsert("scrape_results", single_record_data)

        state.update(
            {
                "last_scrape_urls": urls,
                "last_scrape_count": len(processed_results),
            }
        )

    return state


# Initialize the connector
connector = Connector(update=update, schema=schema)


if __name__ == "__main__":
    with open("connectors/bright_data/configuration.json", "r", encoding="utf-8") as f:
        local_configuration = json.load(f)
    connector.debug(configuration=local_configuration)
