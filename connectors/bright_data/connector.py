"""
Bright Data Fivetran Connector

This connector demonstrates how to fetch data from Bright Data's Web Unlocker API
and upsert it into the Fivetran destination. The connector
dynamically creates tables with flattened dictionary structures,
allowing Fivetran to infer column types automatically.

See the Technical Reference documentation:
https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update

See the Best Practices documentation:
https://fivetran.com/docs/connectors/connector-sdk/best-practices
"""

import json
from typing import Any, Dict, List, Optional
from helpers import (
    collect_all_fields,
    perform_web_unlocker,
    process_unlocker_result,
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
    and Fivetran infers column types automatically from the flattened API response data.
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
    return [
        {
            "table": "unlocker_results",
            "primary_key": [
                "requested_url",
                "result_index",
            ],
        }
    ]


def update(
    configuration: dict, state: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Define the update function, which is a required function, and is called by Fivetran during each sync.

    This function fetches data from Bright Data's Web Unlocker API,
    flattens nested dictionaries into columns, and upserts the data into Fivetran.
    The connector dynamically discovers fields from the API response and documents them in fields.yaml.

    See the technical reference documentation for more details on the update function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update

    Args:
        configuration: A dictionary containing connection details (api_token, unlocker_url, etc.)
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

    new_state = dict(state) if state else {}

    try:
        unlocker_url_input = configuration.get("unlocker_url", "")
        if unlocker_url_input:
            new_state = _process_unlocker_endpoint(
                configuration, unlocker_url_input, new_state
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


def _process_unlocker_endpoint(
    configuration: Dict[str, Any],
    unlocker_url_input: str,
    state: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Process unlocker endpoint: fetch unlocker results and upsert to Fivetran.
    """
    urls: List[str]
    if unlocker_url_input:
        try:
            parsed = json.loads(unlocker_url_input)
            if isinstance(parsed, list):
                urls = parsed
            else:
                urls = [parsed]
        except (json.JSONDecodeError, TypeError):
            if isinstance(unlocker_url_input, str):
                if "," in unlocker_url_input:
                    urls = [
                        url.strip()
                        for url in unlocker_url_input.split(",")
                        if url.strip()
                    ]
                elif "\n" in unlocker_url_input:
                    urls = [
                        url.strip()
                        for url in unlocker_url_input.split("\n")
                        if url.strip()
                    ]
                else:
                    urls = (
                        [unlocker_url_input.strip()]
                        if unlocker_url_input.strip()
                        else []
                    )
            elif isinstance(unlocker_url_input, list):
                urls = unlocker_url_input
            else:
                urls = []
    else:
        urls = []

    if not urls:
        raise ValueError("unlocker_url cannot be empty")

    api_token = configuration.get("api_token")
    country = configuration.get("country")
    data_format = configuration.get("data_format")
    format_param = configuration.get("format")
    method = configuration.get("method")
    unlocker_zone = configuration.get("unlocker_zone") or configuration.get("zone")

    unlocker_payload = urls if len(urls) > 1 else urls[0]
    unlocker_results = perform_web_unlocker(
        api_token=api_token,
        url=unlocker_payload,
        zone=unlocker_zone,
        country=country,
        method=method,
        format_param=format_param,
        data_format=data_format,
    )

    if not isinstance(unlocker_results, list):
        unlocker_results = [unlocker_results]

    processed_results: List[Dict[str, Any]] = []
    for idx, result in enumerate(unlocker_results):
        requested_url = result.get("requested_url")
        if not requested_url:
            requested_url = urls[idx % len(urls)]
        processed_result = process_unlocker_result(result, requested_url, idx)
        processed_results.append(processed_result)

    if processed_results:
        log.info(f"Upserting {len(processed_results)} unlocker results to Fivetran")

        all_fields = collect_all_fields(processed_results)
        update_fields_yaml(all_fields, "unlocker_results")

        for result in processed_results:
            single_record_data: Dict[str, List[Any]] = {}
            for field in all_fields:
                single_record_data[field] = [result.get(field)]

            op.upsert("unlocker_results", single_record_data)

        state.update(
            {
                "last_unlocker_urls": urls,
                "last_unlocker_count": len(processed_results),
            }
        )

    return state


# Initialize the connector
connector = Connector(update=update, schema=schema)
