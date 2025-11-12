"""
Bright Data Scrape Connector

This connector fetches data from Bright Data's Web Scraper API
and upserts the flattened results into Fivetran.
"""

import json
from typing import Any, Dict, List, Optional

from brightdata import bdclient
from helpers import (collect_all_fields, perform_scrape, process_scrape_result,
                     update_fields_yaml, validate_configuration)

from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op


def schema(_config: dict) -> List[Dict[str, Any]]:
    """
    Define the schema delivered by the connector.
    """
    return [
        {
            "table": "scrape_results",
            "primary_key": [
                "url",
                "result_index",
            ],
        }
    ]


def update(
    configuration: dict, state: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Fetch data from Bright Data's Web Scraper API and upsert it into Fivetran.
    """
    validate_configuration(configuration=configuration)

    api_token = configuration.get("api_token")
    client = bdclient(api_token=api_token)

    new_state = dict(state) if state else {}

    try:
        scrape_url_input = configuration.get("scrape_url", "")
        if scrape_url_input:
            new_state = _process_scrape_endpoint(
                client, configuration, scrape_url_input, new_state
            )

        op.checkpoint(state=new_state)
        return new_state

    except Exception as exc:
        raise RuntimeError(f"Failed to sync data from Bright Data: {str(exc)}") from exc


def _process_scrape_endpoint(
    client: bdclient,
    configuration: dict,
    scrape_url_input: str,
    state: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Fetch Bright Data scrape results for one or more URLs and upsert them.
    """
    urls = _parse_url_input(scrape_url_input)
    if not urls:
        raise ValueError("scrape_url cannot be empty")

    country = configuration.get("country")
    data_format = configuration.get("data_format")
    format_param = configuration.get("format")
    method = configuration.get("method")

    async_request = True
    async_request_str = str(configuration.get("async_request", "")).lower()
    if async_request_str in ("false", "0", "no"):
        async_request = False

    if len(urls) > 1:
        scrape_results: List[Dict[str, Any]] = []
        for single_url in urls:
            try:
                result = perform_scrape(
                    client=client,
                    url=single_url,
                    country=country,
                    data_format=data_format,
                    format_param=format_param,
                    method=method,
                    async_request=async_request,
                )
                if isinstance(result, list):
                    scrape_results.extend(result)
                else:
                    scrape_results.append(result)
            except (RuntimeError, ValueError) as exc:
                log.info(
                    f"Error scraping {single_url}: {str(exc)}. Continuing with other URLs."
                )
    else:
        result = perform_scrape(
            client=client,
            url=urls[0],
            country=country,
            data_format=data_format,
            format_param=format_param,
            method=method,
            async_request=async_request,
        )
        scrape_results = result if isinstance(result, list) else [result]

    processed_results: List[Dict[str, Any]] = []

    if len(scrape_results) == len(urls):
        for url_idx, url in enumerate(urls):
            result = scrape_results[url_idx]
            if isinstance(result, list):
                for result_idx, item in enumerate(result):
                    processed_results.append(
                        process_scrape_result(item, url, result_idx)
                    )
            else:
                processed_results.append(process_scrape_result(result, url, 0))
    else:
        for idx, result in enumerate(scrape_results):
            url = urls[idx % len(urls)]
            processed_results.append(process_scrape_result(result, url, idx))

    if processed_results:
        log.info(f"Upserting {len(processed_results)} scrape results to Fivetran")

        all_fields = collect_all_fields(processed_results)
        update_fields_yaml(all_fields)

        for result in processed_results:
            columnar_data = {field: [result.get(field)] for field in all_fields}
            op.upsert("scrape_results", columnar_data)

        state.update(
            {
                "last_scrape_urls": urls,
                "last_scrape_count": len(processed_results),
            }
        )

    return state


def _parse_url_input(url_input: Any) -> List[str]:
    """Support multiple input formats for scrape_url."""
    if not url_input:
        return []

    if isinstance(url_input, list):
        return [
            item.strip() for item in url_input if isinstance(item, str) and item.strip()
        ]

    if isinstance(url_input, str):
        try:
            parsed = json.loads(url_input)
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

        if "," in url_input:
            return [item.strip() for item in url_input.split(",") if item.strip()]
        if "\n" in url_input:
            return [item.strip() for item in url_input.split("\n") if item.strip()]
        return [url_input.strip()] if url_input.strip() else []

    return []


# Initialize the connector
connector = Connector(update=update, schema=schema)


if __name__ == "__main__":
    with open("configuration.json", "r", encoding="utf-8") as f:
        local_configuration = json.load(f)
    connector.debug(configuration=local_configuration)
