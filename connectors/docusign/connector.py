"""DocuSign connector for syncing data including envelopes, documents, users, and templates.
This connector demonstrates how to fetch data from DocuSign eSign REST API and upsert it into destination using memory-efficient streaming patterns.
See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

# For reading configuration from a JSON file
import json

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

""" ADD YOUR SOURCE-SPECIFIC IMPORTS HERE
Example: import pandas, boto3, etc.
Add comment for each import to explain its purpose for users to follow.
"""
# For making HTTP requests to DocuSign API
import requests

# For handling dates and timestamps
from datetime import datetime, timedelta, timezone

# For type hints to improve code readability
from typing import Dict, Optional

# For handling JSON data processing
import time

# For random number generation (jitter in retries)
import random

# Private constants (use __ prefix)
__INVALID_LITERAL_ERROR = "invalid literal"
__API_BASE_URL = "https://demo.docusign.net/restapi/v2.1"  # DocuSign demo environment
__PROD_API_BASE_URL = "https://na1.docusign.net/restapi/v2.1"  # Production base URL


def __get_config_int(
    configuration: dict,
    key: str,
    default: int,
    min_val: Optional[int] = None,
    max_val: Optional[int] = None,
) -> int:
    """
    Centralized configuration parameter parsing with validation.
    This function safely extracts integer values from configuration with bounds checking.
    Args:
        configuration: Configuration dictionary
        key: Configuration key to extract
        default: Default value if key is missing or invalid
        min_val: Optional minimum allowed value
        max_val: Optional maximum allowed value
    Returns:
        Integer value from configuration or default
    """
    try:
        value = int(configuration.get(key, default))
        if min_val is not None and value < min_val:
            log.warning(f"Configuration {key}={value} below minimum {min_val}, using minimum")
            return min_val
        if max_val is not None and value > max_val:
            log.warning(f"Configuration {key}={value} above maximum {max_val}, using maximum")
            return max_val
        return value
    except (ValueError, TypeError):
        log.warning(f"Invalid {key} configuration, using default: {default}")
        return default


def __get_config_bool(configuration: dict, key: str, default: bool) -> bool:
    """
    Centralized boolean configuration parameter parsing.
    This function safely extracts boolean values from configuration.
    Args:
        configuration: Configuration dictionary
        key: Configuration key to extract
        default: Default value if key is missing
    Returns:
        Boolean value from configuration or default
    """
    value = configuration.get(key, default)
    if isinstance(value, str):
        return value.lower() in ("true", "1", "yes", "on")
    return bool(value)


def __validate_required_fields(configuration: dict):
    """
    Validate required fields using dictionary mapping approach.
    This function checks that all required configuration fields are present.
    Args:
        configuration: Configuration dictionary to validate
    Raises:
        ValueError: if any required fields are missing
    """
    required_fields = {
        "access_token": "DocuSign access token",
        "account_id": "DocuSign account ID",
    }

    missing_fields = []
    for field, description in required_fields.items():
        if not configuration.get(field):
            missing_fields.append(f"{field} ({description})")

    if missing_fields:
        raise ValueError(f"Missing required configuration fields: {', '.join(missing_fields)}")


def __validate_numeric_ranges(configuration: dict):
    """
    Validate numeric parameters using iteration pattern.
    This function validates numeric configuration parameters against their allowed ranges.
    Args:
        configuration: Configuration dictionary to validate
    """
    numeric_validations = [
        ("sync_frequency_hours", 1, 24),
        ("initial_sync_days", 1, 365),
        ("max_records_per_page", 10, 1000),
        ("request_timeout_seconds", 5, 300),
        ("retry_attempts", 1, 10),
    ]

    for param, min_val, max_val in numeric_validations:
        value = __get_config_int(configuration, param, min_val, min_val, max_val)
        configuration[param] = str(value)  # Ensure it's stored back as string


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    This function is called at the start of the update method to ensure that the connector has all necessary configuration values.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing.
    """
    __validate_required_fields(configuration)
    __validate_numeric_ranges(configuration)


def __calculate_wait_time(
    attempt: int, response_headers: dict = None, base_delay: int = 1, max_delay: int = 60
) -> int:
    """
    Calculate wait time with jitter for rate limiting and retries.
    This function implements exponential backoff with jitter for retry logic.
    Args:
        attempt: Current attempt number (0-based)
        response_headers: Optional HTTP response headers
        base_delay: Base delay in seconds
        max_delay: Maximum delay in seconds
    Returns:
        Wait time in seconds
    """
    if response_headers and "retry-after" in response_headers:
        try:
            return min(int(response_headers["retry-after"]), max_delay)
        except (ValueError, TypeError):
            pass

    # Exponential backoff with jitter
    exponential_delay = min(base_delay * (2**attempt), max_delay)
    jitter = random.uniform(0.1, 0.3) * exponential_delay
    return int(exponential_delay + jitter)


def __handle_rate_limit(attempt: int, response: requests.Response):
    """
    Handle HTTP 429 rate limiting.
    This function handles rate limiting responses by waiting for the specified time.
    Args:
        attempt: Current attempt number
        response: HTTP response object containing rate limit headers
    """
    wait_time = __calculate_wait_time(attempt, response.headers)
    log.warning(f"Rate limited (attempt {attempt + 1}), waiting {wait_time} seconds")
    time.sleep(wait_time)


def __handle_request_error(attempt: int, retry_attempts: int, error: Exception, endpoint: str):
    """
    Handle request errors with retry logic.
    This function handles request errors and implements retry logic with exponential backoff.
    Args:
        attempt: Current attempt number
        retry_attempts: Total number of retry attempts allowed
        error: Exception that occurred
        endpoint: API endpoint that failed
    Raises:
        RuntimeError: if all retry attempts are exhausted
    """
    if attempt >= retry_attempts - 1:
        raise RuntimeError(
            f"Failed to fetch data from {endpoint} after {retry_attempts} attempts: {str(error)}"
        )

    wait_time = __calculate_wait_time(attempt)
    log.warning(
        f"Request error for {endpoint} (attempt {attempt + 1}/{retry_attempts}): {str(error)}, retrying in {wait_time}s"
    )
    time.sleep(wait_time)


def execute_api_request(
    endpoint: str,
    access_token: str,
    params: Optional[Dict] = None,
    configuration: Optional[Dict] = None,
) -> Dict:
    """
    Main API request orchestrator with retry logic and error handling.
    This function handles HTTP requests to DocuSign API with exponential backoff
    and rate limiting support.
    Args:
        endpoint: API endpoint to call
        access_token: DocuSign API access token for authentication
        params: Optional query parameters for the request
        configuration: Optional configuration dictionary
    Returns:
        Dictionary containing the API response data
    Raises:
        RuntimeError: if the request fails after all retry attempts
    """
    url = f"{__API_BASE_URL}/{endpoint.lstrip('/')}"

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

    timeout = __get_config_int(configuration or {}, "request_timeout_seconds", 30)
    retry_attempts = __get_config_int(configuration or {}, "retry_attempts", 3)

    for attempt in range(retry_attempts):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=timeout)

            if response.status_code == 429:
                __handle_rate_limit(attempt, response)
                continue

            if response.status_code == 401:
                raise RuntimeError("Unauthorized: Invalid access token or expired credentials")

            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            __handle_request_error(attempt, retry_attempts, e, endpoint)
            continue

    raise RuntimeError("Unexpected error in API request execution")


def get_time_range(
    last_sync_time: Optional[str] = None, configuration: Optional[Dict] = None
) -> Dict[str, str]:
    """
    Generate dynamic time range for API queries.
    This function calculates the start and end time for data synchronization
    based on the last sync time or initial sync configuration.
    Args:
        last_sync_time: Optional timestamp of the last successful sync
        configuration: Optional configuration dictionary
    Returns:
        Dictionary containing start and end timestamps
    """
    end_time = datetime.now(timezone.utc).isoformat()

    if last_sync_time:
        start_time = last_sync_time
    else:
        initial_sync_days = __get_config_int(configuration or {}, "initial_sync_days", 90)
        start_time = (datetime.now(timezone.utc) - timedelta(days=initial_sync_days)).isoformat()

    return {"start": start_time, "end": end_time}


def __map_envelope_data(envelope: Dict, account_id: str) -> Dict:
    """
    Extract field mapping logic for envelope data maintainability.
    This function maps DocuSign envelope API response to standardized format.
    Args:
        envelope: Raw envelope data from DocuSign API
        account_id: DocuSign account ID
    Returns:
        Dictionary containing mapped envelope data
    """
    return {
        "envelope_id": envelope.get("envelopeId", ""),
        "account_id": account_id,
        "subject": envelope.get("subject", ""),
        "status": envelope.get("status", ""),
        "created_date_time": envelope.get("createdDateTime", ""),
        "sent_date_time": envelope.get("sentDateTime", ""),
        "completed_date_time": envelope.get("completedDateTime", ""),
        "declined_date_time": envelope.get("declinedDateTime", ""),
        "voided_date_time": envelope.get("voidedDateTime", ""),
        "sender_email": envelope.get("sender", {}).get("email", ""),
        "sender_name": envelope.get("sender", {}).get("userName", ""),
        "envelope_uri": envelope.get("envelopeUri", ""),
        "documents_combined_uri": envelope.get("documentsCombinedUri", ""),
        "certificate_uri": envelope.get("certificateUri", ""),
        "templates_uri": envelope.get("templatesUri", ""),
        "custom_fields": json.dumps(envelope.get("customFields", {})),
        "notification": json.dumps(envelope.get("notification", {})),
        "email_settings": json.dumps(envelope.get("emailSettings", {})),
        "synced_at": datetime.now(timezone.utc).isoformat(),
    }


def __map_recipient_data(
    recipient: Dict, envelope_id: str, account_id: str, recipient_type: str
) -> Dict:
    """
    Extract field mapping logic for recipient data maintainability.
    This function maps DocuSign recipient API response to standardized format.
    Args:
        recipient: Raw recipient data from DocuSign API
        envelope_id: Associated envelope ID
        account_id: DocuSign account ID
        recipient_type: Type of recipient (signer, cc, etc.)
    Returns:
        Dictionary containing mapped recipient data
    """
    return {
        "recipient_id": recipient.get("recipientId", ""),
        "envelope_id": envelope_id,
        "account_id": account_id,
        "recipient_type": recipient_type,
        "email": recipient.get("email", ""),
        "name": recipient.get("name", ""),
        "status": recipient.get("status", ""),
        "routing_order": recipient.get("routingOrder", ""),
        "signed_date_time": recipient.get("signedDateTime", ""),
        "delivered_date_time": recipient.get("deliveredDateTime", ""),
        "declined_date_time": recipient.get("declinedDateTime", ""),
        "declined_reason": recipient.get("declinedReason", ""),
        "recipient_id_guid": recipient.get("recipientIdGuid", ""),
        "user_id": recipient.get("userId", ""),
        "client_user_id": recipient.get("clientUserId", ""),
        "require_id_lookup": recipient.get("requireIdLookup", ""),
        "phone_number": recipient.get("phoneNumber", {}),
        "tabs": json.dumps(recipient.get("tabs", {})),
        "custom_fields": json.dumps(recipient.get("customFields", {})),
        "synced_at": datetime.now(timezone.utc).isoformat(),
    }


def __map_document_data(document: Dict, envelope_id: str, account_id: str) -> Dict:
    """
    Extract field mapping logic for document data maintainability.
    This function maps DocuSign document API response to standardized format.
    Args:
        document: Raw document data from DocuSign API
        envelope_id: Associated envelope ID
        account_id: DocuSign account ID
    Returns:
        Dictionary containing mapped document data
    """
    return {
        "document_id": document.get("documentId", ""),
        "envelope_id": envelope_id,
        "account_id": account_id,
        "name": document.get("name", ""),
        "type": document.get("type", ""),
        "uri": document.get("uri", ""),
        "order": document.get("order", ""),
        "pages": document.get("pages", ""),
        "display": document.get("display", ""),
        "include_in_download": document.get("includeInDownload", ""),
        "signature_locations": json.dumps(document.get("signatureLocations", [])),
        "synced_at": datetime.now(timezone.utc).isoformat(),
    }


def __map_template_data(template: Dict, account_id: str) -> Dict:
    """
    Extract field mapping logic for template data maintainability.
    This function maps DocuSign template API response to standardized format.
    Args:
        template: Raw template data from DocuSign API
        account_id: DocuSign account ID
    Returns:
        Dictionary containing mapped template data
    """
    return {
        "template_id": template.get("templateId", ""),
        "account_id": account_id,
        "name": template.get("name", ""),
        "description": template.get("description", ""),
        "shared": template.get("shared", ""),
        "uri": template.get("uri", ""),
        "created": template.get("created", ""),
        "last_modified": template.get("lastModified", ""),
        "owner": json.dumps(template.get("owner", {})),
        "folder_id": template.get("folderId", ""),
        "folder_name": template.get("folderName", ""),
        "folder_uri": template.get("folderUri", ""),
        "synced_at": datetime.now(timezone.utc).isoformat(),
    }


def get_envelopes(
    access_token: str,
    account_id: str,
    params: Dict,
    last_sync_time: Optional[str] = None,
    configuration: Optional[Dict] = None,
):
    """
    Fetch envelope data using streaming approach to prevent memory issues.
    This function retrieves envelope data from DocuSign API and yields individual records
    to avoid loading all data into memory at once.
    Args:
        access_token: DocuSign API access token for authentication
        account_id: DocuSign account ID
        params: Dictionary of query parameters for the API request
        last_sync_time: Optional timestamp for incremental sync
        configuration: Optional configuration dictionary
    Yields:
        Dictionary containing envelope, recipient, and document data
    """
    time_range = get_time_range(last_sync_time, configuration)
    endpoint = f"accounts/{account_id}/envelopes"
    max_records = __get_config_int(configuration or {}, "max_records_per_page", 100)

    params = {
        "count": max_records,
        "start_position": 0,
        "from_date": time_range["start"],
        "to_date": time_range["end"],
        "status": "created,sent,delivered,signed,completed,declined,voided",
        "include": "recipients,documents,custom_fields",
    }

    start_position = 0
    while True:
        params["start_position"] = start_position
        response = execute_api_request(endpoint, access_token, params, configuration)

        envelopes = response.get("envelopes", [])
        if not envelopes:
            break

        # Yield individual envelope records instead of accumulating
        for envelope in envelopes:
            yield __map_envelope_data(envelope, account_id)

            # Also yield recipients if included
            recipients = envelope.get("recipients", {})
            for recipient_type, recipient_list in recipients.items():
                if isinstance(recipient_list, list):
                    for recipient in recipient_list:
                        yield __map_recipient_data(
                            recipient, envelope.get("envelopeId", ""), account_id, recipient_type
                        )

            # Also yield documents if included
            documents = envelope.get("documents", [])
            for document in documents:
                yield __map_document_data(document, envelope.get("envelopeId", ""), account_id)

        if len(envelopes) < max_records:
            break
        start_position += max_records


def get_templates(
    access_token: str,
    account_id: str,
    params: Dict,
    last_sync_time: Optional[str] = None,
    configuration: Optional[Dict] = None,
):
    """
    Fetch template data using streaming approach to prevent memory issues.
    This function retrieves template data from DocuSign API and yields individual records
    to avoid loading all data into memory at once.
    Args:
        access_token: DocuSign API access token for authentication
        account_id: DocuSign account ID
        params: Dictionary of query parameters for the API request
        last_sync_time: Optional timestamp for incremental sync
        configuration: Optional configuration dictionary
    Yields:
        Dictionary containing template data
    """
    endpoint = f"accounts/{account_id}/templates"
    max_records = __get_config_int(configuration or {}, "max_records_per_page", 100)

    params = {
        "count": max_records,
        "start_position": 0,
        "include": "recipients,documents,custom_fields",
    }

    # If we have last_sync_time, filter by modified date
    if last_sync_time:
        params["modified_from_date"] = last_sync_time

    start_position = 0
    while True:
        params["start_position"] = start_position
        response = execute_api_request(endpoint, access_token, params, configuration)

        templates = response.get("envelopeTemplates", [])
        if not templates:
            break

        # Yield individual template records instead of accumulating
        for template in templates:
            yield __map_template_data(template, account_id)

        if len(templates) < max_records:
            break
        start_position += max_records


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Returns:
        A list of dictionaries defining table schemas with primary keys.
    """
    return [
        {"table": "envelopes", "primary_key": ["envelope_id"]},
        {"table": "recipients", "primary_key": ["recipient_id", "envelope_id"]},
        {"table": "documents", "primary_key": ["document_id", "envelope_id"]},
        {"table": "templates", "primary_key": ["template_id"]},
    ]


def update(configuration: dict, state: dict):
    """
    Define the update function, which is a required function, and is called by Fivetran during each sync.
    See the technical reference documentation for more details on the update function
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    Args:
        configuration: A dictionary containing connection details
        state: A dictionary containing state information from previous runs
        The state dictionary is empty for the first sync or for any full re-sync
    """
    log.warning("Starting DocuSign connector sync")

    # Validate configuration
    validate_configuration(configuration)

    # Extract configuration parameters
    access_token = str(configuration.get("access_token", ""))
    account_id = str(configuration.get("account_id", ""))
    enable_templates = __get_config_bool(configuration, "enable_templates", True)

    # Get state for incremental sync
    last_sync_time = state.get("last_sync_time")
    last_template_sync = state.get("last_template_sync")

    try:
        # Fetch envelope data using generator (NO MEMORY ACCUMULATION)
        log.info("Fetching envelope data...")
        envelope_count = 0
        recipient_count = 0
        document_count = 0

        for record in get_envelopes(access_token, account_id, {}, last_sync_time, configuration):
            if (
                "envelope_id" in record
                and "recipient_type" not in record
                and "document_id" not in record
            ):
                # The 'upsert' operation is used to insert or update data in the destination table.
                # The op.upsert method is called with two arguments:
                # - The first argument is the name of the table to upsert the data into.
                # - The second argument is a dictionary containing the data to be upserted,
                op.upsert(table="envelopes", data=record)
                envelope_count += 1
            elif "recipient_type" in record:
                # The 'upsert' operation is used to insert or update data in the destination table.
                # The op.upsert method is called with two arguments:
                # - The first argument is the name of the table to upsert the data into.
                # - The second argument is a dictionary containing the data to be upserted,
                op.upsert(table="recipients", data=record)
                recipient_count += 1
            elif "document_id" in record:
                # The 'upsert' operation is used to insert or update data in the destination table.
                # The op.upsert method is called with two arguments:
                # - The first argument is the name of the table to upsert the data into.
                # - The second argument is a dictionary containing the data to be upserted,
                op.upsert(table="documents", data=record)
                document_count += 1

        log.info(
            f"Processed {envelope_count} envelopes, {recipient_count} recipients, {document_count} documents"
        )

        # Fetch template data if enabled
        if enable_templates:
            log.info("Fetching template data...")
            template_count = 0
            for record in get_templates(
                access_token, account_id, {}, last_template_sync, configuration
            ):
                # The 'upsert' operation is used to insert or update data in the destination table.
                # The op.upsert method is called with two arguments:
                # - The first argument is the name of the table to upsert the data into.
                # - The second argument is a dictionary containing the data to be upserted,
                op.upsert(table="templates", data=record)
                template_count += 1

            log.info(f"Processed {template_count} templates")

        # Update state and checkpoint
        new_state = {
            "last_sync_time": datetime.now(timezone.utc).isoformat(),
            "last_template_sync": (
                datetime.now(timezone.utc).isoformat() if enable_templates else last_template_sync
            ),
        }

        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        op.checkpoint(new_state)

        log.info("DocuSign sync completed successfully")

    except Exception as e:
        log.severe(f"DocuSign sync failed: {str(e)}")
        raise RuntimeError(f"Failed to sync DocuSign data: {str(e)}")


# Instantiate the connector object with the update and schema functions
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
