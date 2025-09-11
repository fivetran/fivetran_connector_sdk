"""
ACT Web API Connector

This connector fetches data from ACT Web API endpoints (contacts, companies, opportunities)
and loads it into Fivetran. It handles JWT authentication, pagination, rate limiting,
and flattens nested JSON structures into tabular format.
"""

from fivetran_connector_sdk import Connector, Logging as log, Operations as op
import json
import requests
import time
import base64
from datetime import datetime
import pytz
from dateutil import parser
from typing import Dict, List, Any, Generator, Optional

# Constants
MAX_RETRIES = 3
RETRY_DELAY = 1
DEFAULT_BATCH_SIZE = 10000
DEFAULT_RATE_LIMIT_PAUSE = 0.25

# Available endpoints
ENDPOINTS = {
    "contacts": "/api/contacts",
    "companies": "/api/companies",
    "opportunities": "/api/opportunities",
    "activities": "/api/activities",
    "activity_types": "/api/activity-types",
    "products": "/api/products",
}


def validate_configuration(configuration: dict) -> None:
    """
    Validate the configuration dictionary to ensure it contains all required parameters.

    Args:
        configuration: Dictionary containing connector configuration settings

    Raises:
        ValueError: If any required configuration parameter is missing
    """
    required_configs = [
        "protocol",
        "hostname",
        "api_path",
        "username",
        "password",
        "database_name",
    ]
    for key in required_configs:
        if key not in configuration:
            raise ValueError(f"Missing required configuration value: {key}")

    # Validate protocol
    if configuration["protocol"].lower() not in ["http", "https"]:
        raise ValueError("Protocol must be 'http' or 'https'")

    # Validate port if provided
    if "port" in configuration and configuration["port"]:
        try:
            port = int(configuration["port"])
            if not (1 <= port <= 65535):
                raise ValueError("Port must be between 1 and 65535")
        except (ValueError, TypeError):
            raise ValueError("Port must be a valid integer between 1 and 65535")

    # Validate default_start_date format if provided
    if "default_start_date" in configuration:
        try:
            parser.parse(configuration["default_start_date"])
        except (ValueError, TypeError):
            raise ValueError("default_start_date must be a valid ISO 8601 datetime string")


def build_api_url(protocol: str, hostname: str, port: str, api_path: str) -> str:
    """
    Build the complete API URL from configuration components.

    Args:
        protocol: HTTP or HTTPS protocol
        hostname: Server hostname or IP address
        port: Port number (if not specified, defaults to 80 for HTTP, 443 for HTTPS)
        api_path: API path (e.g., /act.web.api)

    Returns:
        Complete API URL
    """
    # Validate protocol
    if protocol.lower() not in ["http", "https"]:
        raise ValueError("Protocol must be 'http' or 'https'")

    # Set default ports if not specified
    if not port:
        port = "80" if protocol.lower() == "http" else "443"

    # Build URL with port (only add port if it's not the default)
    if (protocol.lower() == "http" and port != "80") or (
        protocol.lower() == "https" and port != "443"
    ):
        return f"{protocol.lower()}://{hostname}:{port}{api_path}"
    else:
        return f"{protocol.lower()}://{hostname}{api_path}"


def flatten_dict(d: Dict[str, Any], parent_key: str = "", sep: str = "_") -> Dict[str, Any]:
    """
    Flatten a nested dictionary structure.

    Args:
        d: Dictionary to flatten
        parent_key: Parent key for nested items
        sep: Separator to use between keys

    Returns:
        Flattened dictionary
    """
    items: List[tuple] = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            # Convert lists to strings to maintain tabular structure
            items.append((new_key, json.dumps(v)))
        else:
            items.append((new_key, v))
    return dict(items)


def authenticate_with_act(
    api_base_url: str, username: str, password: str, database_name: str
) -> Optional[str]:
    """
    Authenticate with ACT API using Basic Auth to get JWT token.

    Args:
        api_base_url: Base URL for ACT Web API
        username: ACT API username
        password: ACT API password
        database_name: ACT Database Name

    Returns:
        JWT token if successful, None otherwise
    """
    try:
        # Create basic auth header
        credentials = f"{username}:{password}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()

        headers = {
            "Authorization": f"Basic {encoded_credentials}",
            "Act-Database-Name": database_name,
            "Content-Type": "application/json",
        }

        # Log authentication request details
        log.fine(f"Authenticating with ACT API at: {api_base_url}/authorize")
        log.fine(f"Database name: {database_name}")
        log.fine(f"Username: {username}")
        log.fine(f"Authorization header: Basic {encoded_credentials[:10]}...")

        # Get JWT token
        response = requests.get(f"{api_base_url}/authorize", headers=headers, timeout=30)

        # Log authentication response details
        log.fine(f"Authentication response status: {response.status_code}")
        log.fine(f"Authentication response headers: {dict(response.headers)}")

        if response.status_code == 200:
            jwt_token = response.text.strip('"')  # Remove quotes from token
            log.info("Successfully authenticated with ACT API")
            log.fine(f"JWT token length: {len(jwt_token)} characters")
            log.fine(f"JWT token preview: {jwt_token[:50]}...")
            return jwt_token
        else:
            log.severe(f"Authentication failed: {response.status_code} - {response.text}")
            return None

    except Exception as e:
        log.severe(f"Authentication error: {str(e)}")
        return None


def fetch_data(
    url: str,
    params: Dict[str, Any],
    jwt_token: str,
    database_name: str,
    api_base_url: str = None,
    username: str = None,
    password: str = None,
) -> Dict[str, Any]:
    """
    Fetch data from the ACT API with retry logic, rate limiting, and comprehensive error handling.

    Args:
        url: API endpoint URL
        params: Query parameters
        jwt_token: JWT token for authentication
        database_name: ACT Database Name
        api_base_url: Base URL for token refresh (optional)
        username: Username for token refresh (optional)
        password: Password for token refresh (optional)

    Returns:
        API response data

    Raises:
        RuntimeError: If API request fails after retries
    """
    headers = {
        "Authorization": f"Bearer {jwt_token}",
        "Act-Database-Name": database_name,
        "Content-Type": "application/json",
    }

    # Log request details
    log.fine(f"Making API request to: {url}")
    log.fine(f"Request parameters: {params}")
    log.fine(f"Request headers: {dict(headers)}")

    # Define common HTTP error messages
    error_messages = {
        400: "Bad Request - Often missing a required parameter or malformed request.",
        401: "Unauthorized - JWT token expired or invalid credentials.",
        403: "Forbidden - Access denied. Check API permissions.",
        404: "Not Found - The requested endpoint or resource does not exist.",
        429: "Too Many Requests - Rate limit exceeded. Consider increasing rate_limit_pause.",
        500: "Internal Server Error - ACT API server error.",
        502: "Bad Gateway - ACT API server error.",
        503: "Service Unavailable - ACT API service temporarily unavailable.",
        504: "Gateway Timeout - ACT API server timeout.",
    }

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, params=params, headers=headers, timeout=30)

            # Log response details
            log.fine(f"Response status code: {response.status_code}")
            log.fine(f"Response headers: {dict(response.headers)}")

            # Check for rate limiting headers
            if "X-RateLimit-Limit" in response.headers:
                log.fine(
                    f"Rate limit - Limit: {response.headers.get('X-RateLimit-Limit')}, "
                    f"Remaining: {response.headers.get('X-RateLimit-Remaining')}, "
                    f"Reset: {response.headers.get('X-RateLimit-Reset')}"
                )

            # Handle specific HTTP status codes
            status_code = response.status_code

            if status_code == 200:
                # Success - parse and return data
                response_data = response.json()

                # Log response structure
                if isinstance(response_data, dict):
                    log.fine(f"Response data keys: {list(response_data.keys())}")

                    # Log specific data counts if available
                    if "value" in response_data and isinstance(response_data["value"], list):
                        log.fine(
                            f"Number of records in 'value' array: {len(response_data['value'])}"
                        )
                        # Log sample data structure (first record only)
                        if response_data["value"]:
                            sample_record = response_data["value"][0]
                            sample_keys = (
                                list(sample_record.keys())
                                if isinstance(sample_record, dict)
                                else "Not a dict"
                            )
                            log.fine(f"Sample record structure: {sample_keys}")

                elif isinstance(response_data, list):
                    log.fine(f"Number of records in response array: {len(response_data)}")
                    # Log sample data structure (first record only)
                    if response_data:
                        sample_record = response_data[0]
                        sample_keys = (
                            list(sample_record.keys())
                            if isinstance(sample_record, dict)
                            else "Not a dict"
                        )
                        log.fine(f"Sample record structure: {sample_keys}")
                else:
                    log.fine(f"Response data type: {type(response_data)}")
                    log.fine(
                        f"Response data: {str(response_data)[:500]}..."
                    )  # Truncate for logging

                return response_data

            elif status_code == 401:
                # Unauthorized - JWT token expired
                error_msg = error_messages.get(401, f"HTTP {status_code} - Unauthorized")
                log.warning(
                    f"JWT token expired (401 Unauthorized) on attempt {attempt + 1}: {error_msg}"
                )

                if api_base_url and username and password:
                    log.info("Attempting to refresh JWT token...")
                    new_jwt_token = authenticate_with_act(
                        api_base_url, username, password, database_name
                    )
                    if new_jwt_token:
                        jwt_token = new_jwt_token
                        headers["Authorization"] = f"Bearer {jwt_token}"
                        log.info("JWT token refreshed successfully, retrying request...")
                        continue
                    else:
                        log.severe("Failed to refresh JWT token")
                        raise RuntimeError("JWT token expired and refresh failed")
                else:
                    log.severe("JWT token expired but no refresh credentials provided")
                    raise RuntimeError("JWT token expired and no refresh mechanism available")

            elif status_code == 429:
                # Rate limited - wait and retry
                error_msg = error_messages.get(429, f"HTTP {status_code} - Too Many Requests")
                log.warning(f"Rate limited on attempt {attempt + 1}: {error_msg}")

                # Extract retry-after header if available
                retry_after = response.headers.get("Retry-After")
                if retry_after:
                    wait_time = int(retry_after)
                    log.info(f"Waiting {wait_time} seconds as specified by Retry-After header")
                    time.sleep(wait_time)
                else:
                    # Exponential backoff for rate limiting
                    wait_time = RETRY_DELAY * (2**attempt)
                    log.info(f"Waiting {wait_time} seconds before retry")
                    time.sleep(wait_time)

                continue

            elif status_code in [500, 502, 503, 504]:
                # Server errors - retry with exponential backoff
                error_msg = error_messages.get(status_code, f"HTTP {status_code} - Server Error")
                log.warning(f"Server error on attempt {attempt + 1}: {error_msg}")

                if attempt < MAX_RETRIES - 1:
                    wait_time = RETRY_DELAY * (2**attempt)
                    log.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                    continue
                else:
                    log.severe(f"Server error after {MAX_RETRIES} attempts: {error_msg}")
                    raise RuntimeError(f"Server error after {MAX_RETRIES} attempts: {error_msg}")

            elif status_code in [400, 403, 404]:
                # Client errors - don't retry
                error_msg = error_messages.get(status_code, f"HTTP {status_code} - Client Error")
                log.severe(f"Client error: {error_msg}")
                log.severe(f"Response body: {response.text[:500]}...")
                raise RuntimeError(f"Client error: {error_msg}")

            else:
                # Other status codes
                error_msg = f"HTTP {status_code} - Unexpected response"
                log.warning(f"Unexpected status code on attempt {attempt + 1}: {error_msg}")

                if attempt < MAX_RETRIES - 1:
                    wait_time = RETRY_DELAY * (attempt + 1)
                    log.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                    continue
                else:
                    log.severe(f"Unexpected status code after {MAX_RETRIES} attempts: {error_msg}")
                    raise RuntimeError(
                        f"Unexpected status code after {MAX_RETRIES} attempts: {error_msg}"
                    )

        except requests.exceptions.Timeout:
            log.warning(f"Request timeout on attempt {attempt + 1}")
            if attempt < MAX_RETRIES - 1:
                wait_time = RETRY_DELAY * (attempt + 1)
                log.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
                continue
            else:
                log.severe(f"Request timeout after {MAX_RETRIES} attempts")
                raise RuntimeError(f"Request timeout after {MAX_RETRIES} attempts")

        except requests.exceptions.ConnectionError:
            log.warning(f"Connection error on attempt {attempt + 1}")
            if attempt < MAX_RETRIES - 1:
                wait_time = RETRY_DELAY * (attempt + 1)
                log.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
                continue
            else:
                log.severe(f"Connection error after {MAX_RETRIES} attempts")
                raise RuntimeError(f"Connection error after {MAX_RETRIES} attempts")

        except requests.exceptions.RequestException as e:
            log.severe(f"Request exception on attempt {attempt + 1}: {str(e)}")
            if attempt < MAX_RETRIES - 1:
                wait_time = RETRY_DELAY * (attempt + 1)
                log.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
                continue
            else:
                log.severe(f"Request exception after {MAX_RETRIES} attempts: {str(e)}")
                raise RuntimeError(f"Request exception after {MAX_RETRIES} attempts: {str(e)}")

        except Exception as e:
            log.severe(f"Unexpected error on attempt {attempt + 1}: {str(e)}")
            if attempt < MAX_RETRIES - 1:
                wait_time = RETRY_DELAY * (attempt + 1)
                log.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
                continue
            else:
                log.severe(f"Unexpected error after {MAX_RETRIES} attempts: {str(e)}")
                raise RuntimeError(f"Unexpected error after {MAX_RETRIES} attempts: {str(e)}")

    raise RuntimeError("Unexpected error in fetch_data")


def schema(configuration: dict) -> List[Dict[str, Any]]:
    """
    Define the schema for the ACT Web API data.
    Only primary keys are defined, letting Fivetran infer other column types.

    Args:
        configuration: Dictionary containing connector configuration settings

    Returns:
        List of table definitions
    """
    return [
        {"table": "contacts", "primary_key": ["id"]},
        {"table": "companies", "primary_key": ["id"]},
        {"table": "opportunities", "primary_key": ["id"]},
        {"table": "activities", "primary_key": ["id"]},
        {"table": "activity_types", "primary_key": ["id"]},
        {"table": "products", "primary_key": ["id"]},
    ]


def update(configuration: dict, state: dict) -> Generator[Any, None, None]:
    """
    Main update function that fetches and processes ACT Web API data.

    Args:
        configuration: Dictionary containing connector configuration settings
        state: Dictionary containing state from previous sync

    Yields:
        Operations for Fivetran to process
    """
    log.info("Starting ACT Web API sync")

    # Validate configuration
    validate_configuration(configuration)

    # Extract configuration parameters
    protocol = configuration["protocol"]
    hostname = configuration["hostname"]
    port = configuration.get("port", "")  # Optional port, defaults to empty string
    api_path = configuration["api_path"]
    username = configuration["username"]
    password = configuration["password"]
    database_name = configuration["database_name"]
    batch_size = int(configuration.get("batch_size", str(DEFAULT_BATCH_SIZE)))
    rate_limit_pause = float(configuration.get("rate_limit_pause", str(DEFAULT_RATE_LIMIT_PAUSE)))
    default_start_date = configuration.get("default_start_date", "1970-01-01T00:00:00Z")

    # Build complete API URL from components
    complete_api_url = build_api_url(protocol, hostname, port, api_path)
    log.info(f"Using API URL: {complete_api_url}")

    # Authenticate with ACT API
    jwt_token = authenticate_with_act(complete_api_url, username, password, database_name)
    if not jwt_token:
        raise RuntimeError("Failed to authenticate with ACT API")

    # Get table cursors and batch tracking from state
    table_cursors = state.get("table_cursors", {})  # Make a copy to avoid overwriting
    table_batch_tracking = state.get("table_batch_tracking", {})  # Track current batch progress

    # Set sync start time - this will be used for all tables
    sync_start_time = datetime.now(pytz.UTC).isoformat()
    log.info(f"Sync start time: {sync_start_time}")

    # Process each table
    log.info(f"Processing {len(ENDPOINTS)} tables: {list(ENDPOINTS.keys())}")
    for table_name, endpoint in ENDPOINTS.items():
        log.info(f"Starting sync for table: {table_name} (endpoint: {endpoint})")

        # Get cursor for this table - use current time if no cursor exists
        table_cursor = table_cursors.get(table_name)
        if not table_cursor:
            # No previous cursor, start from current time for incremental sync
            table_cursor = default_start_date
            log.info(
                f"No previous cursor for {table_name}, starting from current time: {table_cursor}"
            )

        # Get batch tracking for this table
        batch_tracking = table_batch_tracking.get(table_name, {})
        skip = batch_tracking.get("skip", 0)
        batch_count = batch_tracking.get("batch_count", 0)

        log.info(f"Current table_cursors state before processing {table_name}: {table_cursors}")
        log.info(f"Resuming {table_name} from skip={skip}, batch_count={batch_count}")

        try:
            # Safety mechanism to prevent infinite loops on empty responses
            max_empty_batches = 3  # Maximum number of consecutive empty batches before stopping
            empty_batch_count = 0  # Track consecutive empty batches

            while True:
                # Prepare query parameters
                params = {"$top": batch_size, "$skip": skip}

                # Add OData filter for incremental sync
                # Activity Types endpoint has no OData filtering (full pull every time)
                if table_name != "activity_types":
                    # Parse cursor to get start time (table_cursor is guaranteed to exist from above logic)
                    try:
                        cursor_dt = parser.parse(table_cursor)
                        start_time = cursor_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
                    except (ValueError, TypeError):
                        # If cursor parsing fails, use default start date from configuration
                        start_time = default_start_date
                        log.warning(
                            f"Failed to parse cursor for {table_name}, using default start date: {start_time}"
                        )

                    # Use sync start time for the upper bound of the filter
                    filter_end_time = sync_start_time

                    # Different endpoints use different date fields
                    if table_name == "products":
                        params["$filter"] = (
                            f"(editdate ge {start_time} and editdate le {filter_end_time})"
                        )
                        params["$orderby"] = "editdate asc"
                    else:
                        params["$filter"] = (
                            f"(edited ge {start_time} and edited le {filter_end_time})"
                        )
                        params["$orderby"] = "edited asc"

                # Fetch data from API
                log.info(
                    f"Fetching batch of {batch_size} records for {table_name} "
                    f"starting at offset {skip}"
                )
                response_data = fetch_data(
                    f"{complete_api_url}{endpoint}",
                    params,
                    jwt_token,
                    database_name,
                    complete_api_url,
                    username,
                    password,
                )

                # Process results
                # Handle different response formats
                if isinstance(response_data, dict):
                    results = response_data.get("value", [])
                elif isinstance(response_data, list):
                    results = response_data
                else:
                    log.warning(
                        f"Unexpected response data type for {table_name}: {type(response_data)}"
                    )
                    results = []

                # Check for empty results - stop on first empty but track as fail-safe
                if not results:

                    empty_batch_count += 1
                    log.info(
                        f"Empty result {empty_batch_count}/{max_empty_batches} for {table_name}"
                    )

                    # Fail-safe: if too many empty results, fail and stop
                    if empty_batch_count >= max_empty_batches:
                        log.severe(
                            f"FAILED: Reached maximum empty results ({max_empty_batches}) "
                            f"for {table_name} - stopping sync"
                        )
                        raise RuntimeError(
                            f"Too many empty results for {table_name}: "
                            f"{empty_batch_count}/{max_empty_batches}"
                        )

                    # Stop on first empty result (normal behavior)
                    log.info(f"Stopping sync for {table_name} after empty result")
                    break
                else:
                    # Reset empty batch counter when we get data
                    empty_batch_count = 0

                # Process each record
                log.info(f"Processing {len(results)} records for {table_name}")
                for i, record in enumerate(results):
                    # Log sample record processing
                    if i < 3:  # Log first 3 records for debugging
                        record_keys = (
                            list(record.keys()) if isinstance(record, dict) else "Not a dict"
                        )
                        log.fine(f"Processing record {i+1} for {table_name}: {record_keys}")

                    # Flatten the record
                    flattened_record = flatten_dict(record)

                    # Log flattening results for first few records
                    if i < 3:
                        log.fine(f"Flattened record {i+1} keys: {list(flattened_record.keys())}")
                        log.fine(
                            f"Flattened record {i+1} sample values: {dict(list(flattened_record.items())[:5])}"
                        )

                    # Ensure ID field exists
                    if "id" not in flattened_record:
                        # Try to find an ID field
                        for key in ["Id", "ID", "contactId", "companyId", "opportunityId"]:
                            if key in flattened_record:
                                flattened_record["id"] = flattened_record[key]
                                log.fine(
                                    f"Found ID field '{key}' for record {i+1}: {flattened_record['id']}"
                                )
                                break

                        # If no ID found, generate one
                        if "id" not in flattened_record:
                            flattened_record["id"] = f"{table_name}_{skip}_{i}"
                            log.warning(f"Generated ID for record {i+1}: {flattened_record['id']}")

                    # Convert date strings to ISO format
                    date_conversions = 0
                    for key, value in flattened_record.items():
                        if isinstance(value, str) and key.endswith(
                            ("_date", "_time", "edited", "created", "editdate")
                        ):
                            try:
                                dt = parser.parse(value)
                                if dt.tzinfo is None:
                                    dt = pytz.UTC.localize(dt)
                                flattened_record[key] = dt.isoformat()
                                date_conversions += 1
                            except (ValueError, TypeError):
                                pass

                    if date_conversions > 0 and i < 3:
                        log.fine(f"Converted {date_conversions} date fields for record {i+1}")

                    # Yield upsert operation
                    yield op.upsert(table_name, flattened_record)

                # All records in this batch have been processed - now update tracking and checkpoint
                # Update skip for next batch
                skip += len(results)
                batch_count += 1

                # Log batch summary
                log.info(
                    f"Batch {batch_count} completed for {table_name}: {len(results)} records processed"
                )
                log.fine(f"Total records processed for {table_name}: {skip}")

                # Update batch tracking for this table (but not the cursor yet)
                table_batch_tracking[table_name] = {"skip": skip, "batch_count": batch_count}

                # Checkpoint progress for this table (include batch tracking but not cursor update)
                new_state = {
                    "table_cursors": table_cursors,  # Keep existing cursors
                    "table_batch_tracking": table_batch_tracking,  # Update batch tracking
                }
                yield op.checkpoint(new_state)
                log.info(
                    f"Checkpoint saved for {table_name} with batch progress: "
                    f"skip={skip}, batch_count={batch_count}"
                )

                # Log checkpoint details
                log.fine(
                    f"Batch tracking updated for {table_name}: {table_batch_tracking[table_name]}"
                )

                # Respect rate limits
                log.fine(f"Rate limiting pause: {rate_limit_pause} seconds")
                time.sleep(rate_limit_pause)

                # Check if we've processed all records
                if len(results) < batch_size:
                    log.info(f"Completed sync for {table_name} - reached end of data")
                    # Only now update the cursor since the table is fully processed
                    # Use sync start time for consistency across all tables
                    table_cursors[table_name] = sync_start_time
                    # Clear batch tracking for this table since it's complete
                    if table_name in table_batch_tracking:
                        del table_batch_tracking[table_name]
                    log.info(
                        f"Updated cursor for {table_name} to sync start time: {table_cursors[table_name]}"
                    )

                    # Final checkpoint with updated cursor for this table
                    final_state = {
                        "table_cursors": table_cursors,
                        "table_batch_tracking": table_batch_tracking,
                    }
                    yield op.checkpoint(final_state)
                    log.info(
                        f"Final checkpoint saved for {table_name} with updated cursor: "
                        f"{table_cursors[table_name]}"
                    )
                    break

                # Note: No need for max_batches check since we now track empty batches instead

        except Exception as e:
            log.severe(f"Error syncing {table_name}: {str(e)}")
            # Clear batch tracking for this table on error so it starts fresh next time
            if table_name in table_batch_tracking:
                del table_batch_tracking[table_name]
                log.info(f"Cleared batch tracking for {table_name} due to error")
            continue  # Continue to next table

        log.info(f"Completed sync for table: {table_name}")

    # Final sync summary
    log.info("=" * 50)
    log.info("ACT Web API sync completed successfully")
    log.info(f"Processed tables: {list(ENDPOINTS.keys())}")
    log.info(f"Final table_cursors state: {table_cursors}")
    log.info(f"Remaining batch tracking: {table_batch_tracking}")
    log.info(f"Batch size: {batch_size}")
    log.info(f"Rate limit pause: {rate_limit_pause} seconds")
    log.info("=" * 50)


# Initialize the connector
connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    with open("/configuration.json", "r") as f:
        configuration = json.load(f)
    connector.debug(configuration=configuration)
