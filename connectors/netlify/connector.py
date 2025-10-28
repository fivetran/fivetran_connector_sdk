"""Netlify API Connector for Fivetran Connector SDK.
This connector demonstrates how to fetch data from Netlify API including sites, deploys, forms, and form submissions.
See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

# For reading configuration from a JSON file
import json

# For time-based operations and exponential backoff in retries
import time

# For type hints with optional values
from typing import Optional

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# For making HTTP API requests (provided by SDK runtime)
import requests

# Constants for API configuration
__BASE_URL = "https://api.netlify.com/api/v1"
__MAX_RETRIES = 3
__BASE_DELAY_SECONDS = 1
__CHECKPOINT_INTERVAL = 100
__PAGINATION_LIMIT = 100


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    This function is called at the start of the update method to ensure that the connector has all necessary configuration values.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing.
    """
    required_configs = ["api_token"]
    for key in required_configs:
        if key not in configuration:
            raise ValueError(f"Missing required configuration value: {key}")


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    return [
        {
            "table": "site",
            "primary_key": ["id"],
        },
        {
            "table": "deploy",
            "primary_key": ["id"],
        },
        {
            "table": "form",
            "primary_key": ["id"],
        },
        {
            "table": "submission",
            "primary_key": ["id"],
        },
    ]


def should_skip_record(last_timestamp: Optional[str], current_timestamp: Optional[str]):
    """
    Determine if a record should be skipped based on timestamp comparison.
    Args:
        last_timestamp: The last synced timestamp from state.
        current_timestamp: The current record's timestamp.
    Returns:
        True if the record should be skipped, False otherwise.
    """
    return last_timestamp and current_timestamp and current_timestamp <= last_timestamp


def update_timestamp(last_timestamp: Optional[str], current_timestamp: Optional[str]):
    """
    Update the last timestamp if the current timestamp is newer.
    Args:
        last_timestamp: The last synced timestamp.
        current_timestamp: The current record's timestamp.
    Returns:
        The more recent timestamp.
    """
    if not current_timestamp:
        return last_timestamp
    if not last_timestamp or current_timestamp > last_timestamp:
        return current_timestamp
    return last_timestamp


def update(configuration: dict, state: dict):
    """
    Define the update function which lets you configure how your connector fetches data.
    See the technical reference documentation for more details on the update function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
        state: a dictionary that holds the state of the connector.
    """
    log.warning("Example: Source Connector : Netlify API Connector")

    validate_configuration(configuration=configuration)

    api_token = configuration.get("api_token")

    try:
        sync_sites(api_token, state)
        sync_deploys(api_token, state)
        sync_forms(api_token, state)
        sync_submissions(api_token, state)

    except (requests.exceptions.RequestException, RuntimeError) as e:
        log.severe(f"Failed to sync data: {str(e)}")
        raise


def process_records_with_pagination(
    endpoint: str,
    api_token: str,
    table_name: str,
    state_key: str,
    state: dict,
    flatten_function,
    timestamp_field: str,
):
    """
    Generic function to process paginated records from an endpoint.
    Args:
        endpoint: The API endpoint to fetch data from.
        api_token: The API token for authentication.
        table_name: The destination table name.
        state_key: The state dictionary key for tracking sync progress.
        state: The state dictionary.
        flatten_function: Function to flatten API records.
        timestamp_field: The field name containing the record timestamp.
    Returns:
        The number of records processed.
    """
    initial_last_timestamp = state.get(state_key)
    last_timestamp = initial_last_timestamp
    page = 1
    record_count = 0

    while True:
        records = fetch_paginated_data(
            endpoint=endpoint,
            api_token=api_token,
            page=page,
            per_page=__PAGINATION_LIMIT,
        )

        if not records:
            break

        for record in records:
            record_timestamp = record.get(timestamp_field)

            if should_skip_record(initial_last_timestamp, record_timestamp):
                continue

            flattened_record = flatten_function(record)

            # The 'upsert' operation is used to insert or update data in the destination table.
            # The first argument is the name of the destination table.
            # The second argument is a dictionary containing the record to be upserted.
            op.upsert(table=table_name, data=flattened_record)

            record_count += 1

            last_timestamp = update_timestamp(last_timestamp, record_timestamp)

            if record_count % __CHECKPOINT_INTERVAL == 0:
                state[state_key] = last_timestamp
                # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                # from the correct position in case of next sync or interruptions.
                # Learn more about how and where to checkpoint by reading our best practices documentation
                # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
                op.checkpoint(state)
        page += 1

    if last_timestamp:
        state[state_key] = last_timestamp

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
    op.checkpoint(state)

    return record_count


def sync_sites(api_token: str, state: dict):
    """
    Sync all sites from Netlify API.
    Args:
        api_token: The API token for authentication.
        state: The state dictionary to track sync progress.
    """
    log.info("Starting sites sync")

    record_count = process_records_with_pagination(
        endpoint="/sites",
        api_token=api_token,
        table_name="site",
        state_key="sites_last_updated_at",
        state=state,
        flatten_function=flatten_site_record,
        timestamp_field="updated_at",
    )

    log.info(f"Completed sites sync: {record_count} records processed")


def process_site_child_records_with_pagination(
    endpoint_template: str,
    api_token: str,
    table_name: str,
    state_key: str,
    state: dict,
    flatten_function,
    timestamp_field: str,
):
    """
    Generic function to process paginated child records for each site.
    Args:
        endpoint_template: The API endpoint template with {site_id} placeholder.
        api_token: The API token for authentication.
        table_name: The destination table name.
        state_key: The state dictionary key for tracking sync progress.
        state: The state dictionary.
        flatten_function: Function to flatten API records.
        timestamp_field: The field name containing the record timestamp.
    Returns:
        The number of records processed.
    """
    sites = fetch_all_sites(api_token)
    initial_last_timestamp = state.get(state_key)
    last_timestamp = initial_last_timestamp
    record_count = 0

    for site in sites:
        site_id = site.get("id")
        endpoint = endpoint_template.format(site_id=site_id)
        page = 1

        while True:
            records = fetch_paginated_data(
                endpoint=endpoint,
                api_token=api_token,
                page=page,
                per_page=__PAGINATION_LIMIT,
            )

            if not records:
                break

            for record in records:
                record_timestamp = record.get(timestamp_field)

                if should_skip_record(initial_last_timestamp, record_timestamp):
                    continue

                flattened_record = flatten_function(record)

                # The 'upsert' operation is used to insert or update data in the destination table.
                # The first argument is the name of the destination table.
                # The second argument is a dictionary containing the record to be upserted.
                op.upsert(table=table_name, data=flattened_record)

                record_count += 1

                if record_count % __CHECKPOINT_INTERVAL == 0:
                    state[state_key] = record_timestamp
                    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                    # from the correct position in case of next sync or interruptions.
                    # Learn more about how and where to checkpoint by reading our best practices documentation
                    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
                    op.checkpoint(state)

                last_timestamp = update_timestamp(last_timestamp, record_timestamp)

            page += 1

    if last_timestamp:
        state[state_key] = last_timestamp

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
    op.checkpoint(state)

    return record_count


def sync_deploys(api_token: str, state: dict):
    """
    Sync deploys for all sites from Netlify API.
    Args:
        api_token: The API token for authentication.
        state: The state dictionary to track sync progress.
    """
    log.info("Starting deploys sync")

    record_count = process_site_child_records_with_pagination(
        endpoint_template="/sites/{site_id}/deploys",
        api_token=api_token,
        table_name="deploy",
        state_key="deploys_last_updated_at",
        state=state,
        flatten_function=flatten_deploy_record,
        timestamp_field="updated_at",
    )

    log.info(f"Completed deploys sync: {record_count} records processed")


def process_site_child_records_non_paginated(
    endpoint_template: str,
    api_token: str,
    table_name: str,
    state_key: str,
    state: dict,
    flatten_function,
    timestamp_field: str,
):
    """
    Generic function to process non-paginated child records for each site.
    Args:
        endpoint_template: The API endpoint template with {site_id} placeholder.
        api_token: The API token for authentication.
        table_name: The destination table name.
        state_key: The state dictionary key for tracking sync progress.
        state: The state dictionary.
        flatten_function: Function to flatten API records.
        timestamp_field: The field name containing the record timestamp.
    Returns:
        The number of records processed.
    """
    sites = fetch_all_sites(api_token)
    initial_last_timestamp = state.get(state_key)
    last_timestamp = initial_last_timestamp
    record_count = 0

    for site in sites:
        site_id = site.get("id")
        endpoint = endpoint_template.format(site_id=site_id)

        records = fetch_data(endpoint=endpoint, api_token=api_token)

        if not records:
            continue

        for record in records:
            record_timestamp = record.get(timestamp_field)

            if should_skip_record(initial_last_timestamp, record_timestamp):
                continue

            flattened_record = flatten_function(record)

            # The 'upsert' operation is used to insert or update data in the destination table.
            # The first argument is the name of the destination table.
            # The second argument is a dictionary containing the record to be upserted.
            op.upsert(table=table_name, data=flattened_record)

            record_count += 1

            if record_count % __CHECKPOINT_INTERVAL == 0:
                state[state_key] = record_timestamp
                # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                # from the correct position in case of next sync or interruptions.
                # Learn more about how and where to checkpoint by reading our best practices documentation
                # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
                op.checkpoint(state)

            last_timestamp = update_timestamp(last_timestamp, record_timestamp)

    if last_timestamp:
        state[state_key] = last_timestamp

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
    op.checkpoint(state)

    return record_count


def sync_forms(api_token: str, state: dict):
    """
    Sync forms for all sites from Netlify API.
    Args:
        api_token: The API token for authentication.
        state: The state dictionary to track sync progress.
    """
    log.info("Starting forms sync")

    record_count = process_site_child_records_non_paginated(
        endpoint_template="/sites/{site_id}/forms",
        api_token=api_token,
        table_name="form",
        state_key="forms_last_created_at",
        state=state,
        flatten_function=flatten_form_record,
        timestamp_field="created_at",
    )

    log.info(f"Completed forms sync: {record_count} records processed")


def sync_submissions(api_token: str, state: dict):
    """
    Sync form submissions for all sites from Netlify API.
    Args:
        api_token: The API token for authentication.
        state: The state dictionary to track sync progress.
    """
    log.info("Starting submissions sync")

    record_count = process_site_child_records_non_paginated(
        endpoint_template="/sites/{site_id}/submissions",
        api_token=api_token,
        table_name="submission",
        state_key="submissions_last_created_at",
        state=state,
        flatten_function=flatten_submission_record,
        timestamp_field="created_at",
    )

    log.info(f"Completed submissions sync: {record_count} records processed")


def fetch_all_sites(api_token: str):
    """
    Fetch all sites from Netlify API.
    Args:
        api_token: The API token for authentication.
    Returns:
        A list of all sites.
    """
    all_sites = []
    page = 1

    while True:
        sites = fetch_paginated_data(
            endpoint="/sites",
            api_token=api_token,
            page=page,
            per_page=__PAGINATION_LIMIT,
        )

        if not sites:
            break

        all_sites.extend(sites)
        page += 1

    return all_sites


def fetch_paginated_data(endpoint: str, api_token: str, page: int, per_page: int):
    """
    Fetch paginated data from Netlify API.
    Args:
        endpoint: The API endpoint to fetch data from.
        api_token: The API token for authentication.
        page: The page number to fetch.
        per_page: The number of records per page.
    Returns:
        A list of records from the API response.
    """
    url = f"{__BASE_URL}{endpoint}?page={page}&per_page={per_page}"
    headers = get_headers(api_token)

    for attempt in range(__MAX_RETRIES):
        try:
            response = requests.get(url, headers=headers)

            if response.status_code == 200:
                return response.json()
            elif response.status_code in [429, 500, 502, 503, 504]:
                if attempt < __MAX_RETRIES - 1:
                    delay = __BASE_DELAY_SECONDS * (2**attempt)
                    log.warning(
                        f"Request failed with status {response.status_code}, retrying in {delay} seconds (attempt {attempt + 1}/{__MAX_RETRIES})"
                    )
                    time.sleep(delay)
                    continue
                else:
                    log.severe(
                        f"Failed to fetch data after {__MAX_RETRIES} attempts. Last status: {response.status_code} - {response.text}"
                    )
                    raise RuntimeError(
                        f"API returned {response.status_code} after {__MAX_RETRIES} attempts: {response.text}"
                    )
            elif response.status_code == 404:
                return []
            else:
                log.severe(
                    f"API request failed with status {response.status_code}: {response.text}"
                )
                raise RuntimeError(
                    f"API request failed with status {response.status_code}: {response.text}"
                )

        except requests.exceptions.RequestException as e:
            if attempt < __MAX_RETRIES - 1:
                delay = __BASE_DELAY_SECONDS * (2**attempt)
                log.warning(
                    f"Request exception occurred, retrying in {delay} seconds (attempt {attempt + 1}/{__MAX_RETRIES}): {str(e)}"
                )
                time.sleep(delay)
                continue
            else:
                log.severe(f"Failed to fetch data after {__MAX_RETRIES} attempts: {str(e)}")
                raise RuntimeError(
                    f"Failed to fetch data after {__MAX_RETRIES} attempts: {str(e)}"
                )

    return []


def fetch_data(endpoint: str, api_token: str):
    """
    Fetch non-paginated data from Netlify API.
    Args:
        endpoint: The API endpoint to fetch data from.
        api_token: The API token for authentication.
    Returns:
        A list of records from the API response.
    """
    url = f"{__BASE_URL}{endpoint}"
    headers = get_headers(api_token)

    for attempt in range(__MAX_RETRIES):
        try:
            response = requests.get(url, headers=headers)

            if response.status_code == 200:
                return response.json()
            elif response.status_code in [429, 500, 502, 503, 504]:
                if attempt < __MAX_RETRIES - 1:
                    delay = __BASE_DELAY_SECONDS * (2**attempt)
                    log.warning(
                        f"Request failed with status {response.status_code}, retrying in {delay} seconds (attempt {attempt + 1}/{__MAX_RETRIES})"
                    )
                    time.sleep(delay)
                    continue
                else:
                    log.severe(
                        f"Failed to fetch data after {__MAX_RETRIES} attempts. Last status: {response.status_code} - {response.text}"
                    )
                    raise RuntimeError(
                        f"API returned {response.status_code} after {__MAX_RETRIES} attempts: {response.text}"
                    )
            elif response.status_code == 404:
                return []
            else:
                log.severe(
                    f"API request failed with status {response.status_code}: {response.text}"
                )
                raise RuntimeError(
                    f"API request failed with status {response.status_code}: {response.text}"
                )

        except requests.exceptions.RequestException as e:
            if attempt < __MAX_RETRIES - 1:
                delay = __BASE_DELAY_SECONDS * (2**attempt)
                log.warning(
                    f"Request exception occurred, retrying in {delay} seconds (attempt {attempt + 1}/{__MAX_RETRIES}): {str(e)}"
                )
                time.sleep(delay)
                continue
            else:
                log.severe(f"Failed to fetch data after {__MAX_RETRIES} attempts: {str(e)}")
                raise RuntimeError(
                    f"Failed to fetch data after {__MAX_RETRIES} attempts: {str(e)}"
                )

    return []


def get_headers(api_token: str):
    """
    Generate HTTP headers for API requests.
    Args:
        api_token: The API token for authentication.
    Returns:
        A dictionary of HTTP headers.
    """
    return {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json",
    }


def flatten_site_record(site: dict):
    """
    Flatten a site record by extracting relevant fields.
    Args:
        site: The site record from the API.
    Returns:
        A flattened dictionary with site data.
    """
    return {
        "id": site.get("id"),
        "site_id": site.get("site_id"),
        "name": site.get("name"),
        "custom_domain": site.get("custom_domain"),
        "url": site.get("url"),
        "admin_url": site.get("admin_url"),
        "screenshot_url": site.get("screenshot_url"),
        "created_at": site.get("created_at"),
        "updated_at": site.get("updated_at"),
        "user_id": site.get("user_id"),
        "state": site.get("state"),
        "plan": site.get("plan"),
        "account_name": site.get("account_name"),
        "account_slug": site.get("account_slug"),
    }


def flatten_deploy_record(deploy: dict):
    """
    Flatten a deploy record by extracting relevant fields.
    Args:
        deploy: The deploy record from the API.
    Returns:
        A flattened dictionary with deploy data.
    """
    return {
        "id": deploy.get("id"),
        "site_id": deploy.get("site_id"),
        "name": deploy.get("name"),
        "state": deploy.get("state"),
        "url": deploy.get("url"),
        "deploy_url": deploy.get("deploy_url"),
        "admin_url": deploy.get("admin_url"),
        "created_at": deploy.get("created_at"),
        "updated_at": deploy.get("updated_at"),
        "published_at": deploy.get("published_at"),
        "branch": deploy.get("branch"),
        "commit_ref": deploy.get("commit_ref"),
        "commit_url": deploy.get("commit_url"),
        "context": deploy.get("context"),
        "review_url": deploy.get("review_url"),
        "screenshot_url": deploy.get("screenshot_url"),
    }


def flatten_form_record(form: dict):
    """
    Flatten a form record by extracting relevant fields.
    Args:
        form: The form record from the API.
    Returns:
        A flattened dictionary with form data.
    """
    paths = form.get("paths", [])
    paths_str = ",".join(paths) if paths else None

    fields = form.get("fields", [])
    fields_json = json.dumps(fields) if fields else None

    return {
        "id": form.get("id"),
        "site_id": form.get("site_id"),
        "name": form.get("name"),
        "paths": paths_str,
        "submission_count": form.get("submission_count"),
        "fields": fields_json,
        "created_at": form.get("created_at"),
    }


def flatten_submission_record(submission: dict):
    """
    Flatten a submission record by extracting relevant fields.
    Args:
        submission: The submission record from the API.
    Returns:
        A flattened dictionary with submission data.
    """
    data = submission.get("data", {})
    data_json = json.dumps(data) if data else None

    return {
        "id": submission.get("id"),
        "number": submission.get("number"),
        "email": submission.get("email"),
        "name": submission.get("name"),
        "first_name": submission.get("first_name"),
        "last_name": submission.get("last_name"),
        "company": submission.get("company"),
        "summary": submission.get("summary"),
        "body": submission.get("body"),
        "data": data_json,
        "created_at": submission.get("created_at"),
        "site_url": submission.get("site_url"),
    }


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents
    with open("configuration.json", "r") as f:
        configuration = json.load(f)

    # Test the connector locally
    connector.debug(configuration=configuration)
