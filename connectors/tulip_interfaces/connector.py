"""Tulip Fivetran Connector.

This connector syncs data from Tulip Tables to Fivetran destinations using a
two-phase synchronization strategy with cursor-based pagination.
"""

from datetime import datetime, timedelta
import json
import logging
import re
import time
import threading

import requests
from fivetran_connector_sdk import Connector, Logging as log
from fivetran_connector_sdk import Operations as op

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Private constants following Fivetran standards
__API_VERSION = "v3"
__DEFAULT_LIMIT = 100
__CHECKPOINT_INTERVAL = 500
__RATE_LIMIT_RETRY_BASE_SECONDS = 5
__MAX_RETRY_ATTEMPTS = 3
__CURSOR_OVERLAP_SECONDS = 60
__RATE_LIMIT_REQUESTS_PER_SECOND = 50  # Tulip API rate limit


class RateLimiter:
    """Token bucket rate limiter for API requests.

    Implements a token bucket algorithm to limit requests to a specified rate.
    Tulip API allows 50 requests per second.
    """

    def __init__(self, requests_per_second):
        """Initialize rate limiter.

        Args:
            requests_per_second (int): Maximum number of requests allowed per second.
        """
        self.rate = requests_per_second
        self.min_interval = 1.0 / requests_per_second
        self.last_request_time = None
        self.lock = threading.Lock()

    def acquire(self):
        """Acquire permission to make a request.

        Blocks if necessary to maintain the rate limit using a fixed-window approach
        to ensure we never exceed the specified requests per second.
        """
        with self.lock:
            now = time.time()

            # Allow first request immediately
            if self.last_request_time is None:
                self.last_request_time = now
                return

            time_since_last = now - self.last_request_time

            # If not enough time has passed, sleep
            if time_since_last < self.min_interval:
                sleep_time = self.min_interval - time_since_last
                time.sleep(sleep_time)

            # Update last request time
            self.last_request_time = time.time()


# Global rate limiter instance
_rate_limiter = RateLimiter(__RATE_LIMIT_REQUESTS_PER_SECOND)


def generate_column_name(field_id, field_label=None):
    """Generate Snowflake-friendly column name from Tulip field ID and label.

    Combines the human-readable label with the field ID to create unique,
    descriptive column names in the format: label__id (e.g., customer_name__rqoqm).

    Args:
        field_id (str): Tulip field ID (required, unique identifier).
        field_label (str, optional): Tulip field label (human-readable name).

    Returns:
        str: Snowflake-compatible column name with format label__id.

    Example:
        >>> generate_column_name("rqoqm", "Customer Name")
        'customer_name__rqoqm'
    """
    if field_label and field_label.strip():
        label = field_label.strip().lower()
        label = label.replace(' ', '_').replace('-', '_')
        label = re.sub(r'[^a-z0-9_]', '', label)
        label = re.sub(r'_+', '_', label)
        label = label.strip('_')

        if label and label[0].isdigit():
            label = f"field_{label}"

        if not label:
            label = field_id.lower()
    else:
        label = field_id.lower()

    clean_id = field_id.lower()
    clean_id = re.sub(r'[^a-z0-9_]', '', clean_id)

    column_name = f"{label}__{clean_id}"

    if not column_name or column_name[0].isdigit():
        column_name = f"field_{column_name}"

    return column_name


def _build_api_url(subdomain, workspace_id, table_id, endpoint_type="tables"):
    """Build Tulip API URL with optional workspace routing.

    Args:
        subdomain (str): Tulip instance subdomain.
        workspace_id (str, optional): Workspace ID for workspace-scoped requests.
        table_id (str): Table ID.
        endpoint_type (str): Either 'tables' for metadata or 'records' for data.

    Returns:
        str: Fully constructed API URL.
    """
    base_url = f"https://{subdomain}.tulip.co/api/{__API_VERSION}"
    if workspace_id:
        return f"{base_url}/w/{workspace_id}/tables/{table_id}/{endpoint_type}".rstrip('/')
    return f"{base_url}/tables/{table_id}/{endpoint_type}".rstrip('/')


def _map_tulip_type_to_fivetran(tulip_type):
    """Map Tulip data types to Fivetran data types.

    Args:
        tulip_type (str): Tulip field data type.

    Returns:
        str: Corresponding Fivetran data type.
    """
    type_mapping = {
        'integer': 'INT',
        'float': 'DOUBLE',
        'boolean': 'BOOLEAN',
        'timestamp': 'UTC_DATETIME',
        'datetime': 'UTC_DATETIME',
        'interval': 'INT',
        'user': 'STRING'
    }
    return type_mapping.get(tulip_type, 'STRING')


def schema(configuration):
    """Discover schema from Tulip Table API.

    Fetches table metadata from Tulip and constructs a Fivetran-compatible
    schema definition including system fields (id, _createdAt, _updatedAt,
    _sequenceNumber) and all custom table fields with human-readable column names.

    Args:
        configuration (dict): Connector configuration containing:
            - subdomain (str): Tulip instance subdomain
            - api_key (str): API authentication key
            - api_secret (str): API authentication secret
            - table_id (str): Target Tulip table ID
            - workspace_id (str, optional): Workspace ID for scoping

    Returns:
        list: Schema definition with table name, primary key, and columns.

    Raises:
        requests.exceptions.HTTPError: If API request fails.
        KeyError: If required configuration fields are missing.
        Exception: For other schema discovery failures.
    """
    try:
        subdomain = configuration['subdomain']
        api_key = configuration['api_key']
        api_secret = configuration['api_secret']
        table_id = configuration['table_id']
        workspace_id = configuration.get('workspace_id')

        url = _build_api_url(subdomain, workspace_id, table_id, endpoint_type="")
        logger.info(f"Fetching schema from {url}")

        response = requests.get(url, auth=(api_key, api_secret))

        if response.status_code != 200:
            logger.error(f"Tulip API returned {response.status_code}: {response.text}")
            response.raise_for_status()

        table_metadata = response.json()
        columns = {
            'id': 'STRING',
            '_createdAt': 'UTC_DATETIME',
            '_updatedAt': 'UTC_DATETIME',
            '_sequenceNumber': 'INT'
        }

        logger.info(f"Discovered {len(table_metadata.get('columns', []))} fields")

        for field in table_metadata.get('columns', []):
            field_id = field['name']
            if field_id in ['id', '_createdAt', '_updatedAt', '_sequenceNumber']:
                logger.info(f"Skipping system field '{field_id}' from columns list")
                continue

            field_label = field.get('label', '')
            tulip_type = field['dataType']['type']
            column_name = generate_column_name(field_id, field_label)
            fivetran_type = _map_tulip_type_to_fivetran(tulip_type)

            columns[column_name] = fivetran_type
            logger.info(f"Mapped field '{field_label}' ({field_id}) -> {column_name} ({fivetran_type})")

        table_label = table_metadata.get('label', table_id)
        table_name = generate_column_name(table_id, table_label)
        logger.info(f"Table name: {table_name} (from '{table_label}')")

        return [
            {
                "table": table_name,
                "primary_key": ["id"],
                "columns": columns
            }
        ]

    except KeyError as e:
        logger.error(f"Missing required configuration field: {e}")
        raise
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error during schema discovery: {e}")
        raise
    except Exception as e:
        logger.critical(f"Schema discovery failed: {str(e)}")
        raise

def _fetch_with_retry(url, auth, params=None, max_retries=__MAX_RETRY_ATTEMPTS):
    """Fetch data from API with exponential backoff retry logic.

    Applies rate limiting before each request to comply with Tulip API limits.

    Args:
        url (str): API endpoint URL.
        auth (tuple): Authentication tuple (api_key, api_secret).
        params (dict, optional): Query parameters.
        max_retries (int): Maximum number of retry attempts.

    Returns:
        requests.Response: Successful HTTP response.

    Raises:
        requests.exceptions.HTTPError: If all retry attempts fail.
    """
    for attempt in range(max_retries):
        try:
            # Apply rate limiting before making the request
            _rate_limiter.acquire()

            response = requests.get(url, auth=auth, params=params)

            if response.status_code == 429:
                wait_time = __RATE_LIMIT_RETRY_BASE_SECONDS * (2 ** attempt)
                logger.warning(f"Rate limited. Retrying in {wait_time}s (attempt {attempt + 1}/{max_retries})")
                time.sleep(wait_time)
                continue

            if response.status_code != 200:
                logger.error(f"API returned {response.status_code}: {response.text}")
                response.raise_for_status()

            return response

        except requests.exceptions.RequestException as e:
            if attempt == max_retries - 1:
                logger.error(f"All retry attempts failed: {e}")
                raise
            wait_time = __RATE_LIMIT_RETRY_BASE_SECONDS * (2 ** attempt)
            logger.warning(f"Request failed, retrying in {wait_time}s: {e}")
            time.sleep(wait_time)

    raise requests.exceptions.HTTPError("Maximum retry attempts exceeded")


def _build_field_mapping(table_metadata):
    """Build mapping from Tulip field IDs to Fivetran column names.

    Args:
        table_metadata (dict): Table metadata from Tulip API.

    Returns:
        dict: Mapping of field_id -> column_name.
    """
    field_mapping = {}
    for field in table_metadata.get('columns', []):
        field_id = field['name']
        if field_id in ['id', '_createdAt', '_updatedAt', '_sequenceNumber']:
            continue
        field_label = field.get('label', '')
        column_name = generate_column_name(field_id, field_label)
        field_mapping[field_id] = column_name
    return field_mapping


def _build_allowed_fields(table_metadata):
    """Build list of non-tableLink field IDs for API requests.

    Excludes tableLink fields to reduce database load on Tulip API.
    Always includes system fields required for sync operations.

    Args:
        table_metadata (dict): Table metadata from Tulip API.

    Returns:
        list: Field IDs to request, excluding tableLink types.
    """
    # Always include system fields
    allowed_fields = ['id', '_createdAt', '_updatedAt', '_sequenceNumber']
    excluded_fields = []

    for field in table_metadata.get('columns', []):
        field_id = field['name']
        field_type = field.get('dataType', {}).get('type')

        # Skip system fields (already added)
        if field_id in allowed_fields:
            continue

        # Exclude tableLink fields
        if field_type == 'tableLink':
            excluded_fields.append(field_id)
            continue

        allowed_fields.append(field_id)

    if excluded_fields:
        logger.info(f"Excluding {len(excluded_fields)} Linked Table Fields of type tableLink: {excluded_fields}")

    return allowed_fields


def _transform_record(record, field_mapping):
    """Transform Tulip record field IDs to Fivetran column names.

    Args:
        record (dict): Raw record from Tulip API.
        field_mapping (dict): Mapping of field_id -> column_name.

    Returns:
        dict: Transformed record with Fivetran column names.
    """
    transformed_record = {}
    for field_id, value in record.items():
        if field_id in field_mapping:
            column_name = field_mapping[field_id]
        else:
            column_name = field_id
        transformed_record[column_name] = value
    return transformed_record


def _adjust_cursor_for_overlap(cursor):
    """Adjust cursor timestamp to ensure overlap and prevent data loss.

    Subtracts overlap seconds from cursor to account for concurrent updates
    that may have occurred during previous sync.

    Args:
        cursor (str): ISO 8601 timestamp string with optional Z suffix.

    Returns:
        str: Adjusted cursor timestamp.
    """
    if not cursor:
        return None

    clean_cursor = cursor.replace("Z", "+00:00")
    cursor_dt = datetime.fromisoformat(clean_cursor)
    cursor_dt = cursor_dt - timedelta(seconds=__CURSOR_OVERLAP_SECONDS)
    return cursor_dt.isoformat().replace("+00:00", "Z")


def _initialize_state(state, sync_from_date):
    """Initialize or migrate connector state for two-phase sync.

    Handles backward compatibility by migrating old state format to new format.
    New connectors start in BOOTSTRAP mode, while existing connectors with
    last_updated_at migrate to INCREMENTAL mode.

    Args:
        state (dict): Current connector state.
        sync_from_date (str, optional): Initial sync start date from configuration.

    Returns:
        dict: Normalized state with cursor_mode, last_sequence, and last_updated_at.
    """
    # If state already has cursor_mode, validate and normalize it
    if 'cursor_mode' in state:
        normalized_state = dict(state)
        # Ensure last_sequence is valid (never None)
        if normalized_state.get('last_sequence') is None:
            normalized_state['last_sequence'] = 0
        return normalized_state

    # Handle old state format (backward compatibility)
    if 'last_updated_at' in state and state['last_updated_at']:
        logger.info("Migrating existing state to INCREMENTAL mode")
        return {
            'cursor_mode': 'INCREMENTAL',
            'last_sequence': 0,
            'last_updated_at': state['last_updated_at']
        }

    # Initialize new connector state
    logger.info("Initializing new connector in BOOTSTRAP mode")
    return {
        'cursor_mode': 'BOOTSTRAP',
        'last_sequence': 0,
        'last_updated_at': sync_from_date
    }


def _build_bootstrap_filters(last_sequence, sync_from_date, custom_filters):
    """Build filters for bootstrap phase using _sequenceNumber.

    Args:
        last_sequence (int): Last processed sequence number.
        sync_from_date (str, optional): Initial sync start date.
        custom_filters (list): User-defined custom filters.

    Returns:
        list: Combined filter array for Tulip API.
    """
    api_filters = [{"field": "_sequenceNumber", "functionType": "greaterThan", "arg": last_sequence}]

    # Apply sync_from_date filter if specified
    if sync_from_date:
        api_filters.append({"field": "_updatedAt", "functionType": "greaterThan", "arg": sync_from_date})

    # Append custom filters
    api_filters.extend(custom_filters)

    return api_filters


def _build_incremental_filters(last_sequence, last_updated_at, custom_filters):
    """Build filters for incremental phase using _sequenceNumber and _updatedAt with lookback.

    Args:
        last_sequence (int): Last processed sequence number.
        last_updated_at (str): Last processed _updatedAt timestamp.
        custom_filters (list): User-defined custom filters.

    Returns:
        list: Combined filter array for Tulip API.
    """
    # Primary cursor: _sequenceNumber
    api_filters = [{"field": "_sequenceNumber", "functionType": "greaterThan", "arg": last_sequence}]

    # Apply 60-second lookback window on _updatedAt to catch late commits
    start_time = _adjust_cursor_for_overlap(last_updated_at)
    if start_time:
        api_filters.append({"field": "_updatedAt", "functionType": "greaterThan", "arg": start_time})

    # Append custom filters
    api_filters.extend(custom_filters)

    return api_filters


def update(configuration, state):
    """Perform two-phase sync of Tulip table data.

    Implements a two-phase synchronization strategy:
    - Phase 1 (BOOTSTRAP): Historical load using _sequenceNumber for efficiency
    - Phase 2 (INCREMENTAL): Incremental updates using _sequenceNumber with _updatedAt lookback

    Both phases use _sequenceNumber as the primary cursor to avoid offset pagination.
    INCREMENTAL mode adds a 60-second lookback window on _updatedAt to catch late commits.

    Args:
        configuration (dict): Connector configuration containing:
            - subdomain (str): Tulip instance subdomain
            - api_key (str): API authentication key
            - api_secret (str): API authentication secret
            - table_id (str): Target Tulip table ID
            - workspace_id (str, optional): Workspace ID for scoping
            - sync_from_date (str, optional): Initial sync start date
            - custom_filter_json (str, optional): JSON array of custom filters
        state (dict): Connector state containing:
            - cursor_mode (str): Either 'BOOTSTRAP' or 'INCREMENTAL'
            - last_sequence (int): Last processed _sequenceNumber
            - last_updated_at (str): Last processed _updatedAt timestamp

    Raises:
        KeyError: If required configuration fields are missing.
        json.JSONDecodeError: If custom_filter_json is invalid.
        requests.exceptions.HTTPError: If API requests fail.
        Exception: For other sync failures.
    """
    try:
        # Extract configuration
        subdomain = configuration['subdomain']
        api_key = configuration['api_key']
        api_secret = configuration['api_secret']
        table_id = configuration['table_id']
        workspace_id = configuration.get('workspace_id')
        sync_from_date = configuration.get('sync_from_date')

        # Fetch table metadata and build field mapping
        schema_url = _build_api_url(subdomain, workspace_id, table_id, endpoint_type="")
        schema_response = _fetch_with_retry(schema_url, (api_key, api_secret))
        table_metadata = schema_response.json()

        field_mapping = _build_field_mapping(table_metadata)
        table_label = table_metadata.get('label', table_id)
        table_name = generate_column_name(table_id, table_label)

        # Build allowed fields list (excludes tableLink fields to reduce database load)
        allowed_fields = _build_allowed_fields(table_metadata)
        fields_json = json.dumps(allowed_fields)

        # Parse custom filters
        raw_filter = configuration.get('custom_filter_json')
        custom_filters = json.loads(raw_filter) if raw_filter and raw_filter.strip() else []

        # Initialize or migrate state
        current_state = _initialize_state(state, sync_from_date)
        cursor_mode = current_state['cursor_mode']
        last_sequence = current_state.get('last_sequence', 0)
        last_updated_at = current_state.get('last_updated_at')

        url = _build_api_url(subdomain, workspace_id, table_id, endpoint_type="records")
        logger.info(f"Starting sync in {cursor_mode} mode")

        records_processed = 0
        highest_updated_at = last_updated_at

        # Ensure highest_updated_at respects sync_from_date as minimum bound
        # This prevents syncing records older than sync_from_date if state has old timestamps
        if sync_from_date and highest_updated_at and highest_updated_at < sync_from_date:
            logger.info(f"State has old timestamp {highest_updated_at}, using sync_from_date {sync_from_date} as minimum")
            highest_updated_at = sync_from_date

        # Execute appropriate sync logic based on mode
        if cursor_mode == 'BOOTSTRAP':
            logger.info(f"BOOTSTRAP: Starting from _sequenceNumber > {last_sequence}")

            # Build bootstrap filters
            api_filters = _build_bootstrap_filters(last_sequence, sync_from_date, custom_filters)

            # Bootstrap phase: Use _sequenceNumber cursor (no offset pagination)
            has_more = True
            batch_number = 0

            while has_more:
                batch_number += 1
                logger.info(f"BOOTSTRAP: Fetching batch #{batch_number} (filtering _sequenceNumber > {last_sequence})")

                params = {
                    'limit': __DEFAULT_LIMIT,
                    'offset': 0,  # Always 0 - we use cursor-based filtering
                    'filters': json.dumps(api_filters),
                    'sortOptions': json.dumps([{"sortBy": "_sequenceNumber", "sortDir": "asc"}]),
                    'fields': fields_json  # Always included to exclude tableLink fields
                }

                response = _fetch_with_retry(url, (api_key, api_secret), params)
                records = response.json()

                if not records:
                    logger.info("BOOTSTRAP: No more records, switching to INCREMENTAL mode")
                    cursor_mode = 'INCREMENTAL'
                    break

                logger.info(f"BOOTSTRAP: Batch #{batch_number} received {len(records)} records")

                # Process records and track cursors
                for record in records:
                    transformed_record = _transform_record(record, field_mapping)
                    op.upsert(table=table_name, data=transformed_record)

                    # Track highest sequence number for next batch
                    current_sequence = record.get('_sequenceNumber')
                    if current_sequence and current_sequence > last_sequence:
                        last_sequence = current_sequence

                    # Track highest _updatedAt to seed incremental phase
                    current_updated_at = record.get('_updatedAt')
                    if current_updated_at and (not highest_updated_at or current_updated_at > highest_updated_at):
                        highest_updated_at = current_updated_at

                    records_processed += 1

                    # Checkpoint every 500 records
                    if records_processed % __CHECKPOINT_INTERVAL == 0:
                        op.checkpoint(state={
                            'cursor_mode': cursor_mode,
                            'last_sequence': last_sequence,
                            'last_updated_at': highest_updated_at
                        })
                        logger.info(f"BOOTSTRAP: Checkpointed at {records_processed} records (seq: {last_sequence})")

                # Check if we've reached end of historical data
                if len(records) < __DEFAULT_LIMIT:
                    logger.info(f"BOOTSTRAP: Batch #{batch_number} received {len(records)} records (< {__DEFAULT_LIMIT}), switching to INCREMENTAL mode")
                    logger.info(f"BOOTSTRAP: Completed after {batch_number} batches, {records_processed} total records, final sequence: {last_sequence}")
                    cursor_mode = 'INCREMENTAL'
                    has_more = False
                else:
                    # Update filter for next batch
                    api_filters = _build_bootstrap_filters(last_sequence, sync_from_date, custom_filters)
                    logger.info(f"BOOTSTRAP: Batch #{batch_number} complete, continuing with next batch (new cursor: _sequenceNumber > {last_sequence})")

        # Incremental phase: Use _sequenceNumber with _updatedAt lookback window
        if cursor_mode == 'INCREMENTAL':
            # If bootstrap completed with no records and we have no timestamp, use sync_from_date
            if not highest_updated_at and sync_from_date:
                logger.info(f"INCREMENTAL: No timestamp from bootstrap, using sync_from_date: {sync_from_date}")
                highest_updated_at = sync_from_date

            logger.info(f"INCREMENTAL: Starting from _sequenceNumber > {last_sequence}, _updatedAt > {highest_updated_at} (with 60s lookback)")

            # Build incremental filters
            api_filters = _build_incremental_filters(last_sequence, highest_updated_at, custom_filters)

            has_more = True
            while has_more:
                params = {
                    'limit': __DEFAULT_LIMIT,
                    'offset': 0,  # No offset pagination - use cursor-based filtering
                    'filters': json.dumps(api_filters),
                    'sortOptions': json.dumps([{"sortBy": "_sequenceNumber", "sortDir": "asc"}]),
                    'fields': fields_json  # Always included to exclude tableLink fields
                }

                response = _fetch_with_retry(url, (api_key, api_secret), params)
                records = response.json()

                if not records:
                    break

                logger.info(f"INCREMENTAL: Fetching batch with {len(records)} records (filtering _sequenceNumber > {last_sequence})")

                # Process records and track cursors
                for record in records:
                    transformed_record = _transform_record(record, field_mapping)
                    op.upsert(table=table_name, data=transformed_record)

                    # Track highest sequence number for next batch
                    current_sequence = record.get('_sequenceNumber')
                    if current_sequence and current_sequence > last_sequence:
                        last_sequence = current_sequence

                    # Track highest _updatedAt for lookback calculation
                    current_updated_at = record.get('_updatedAt')
                    if current_updated_at and (not highest_updated_at or current_updated_at > highest_updated_at):
                        highest_updated_at = current_updated_at

                    records_processed += 1

                    # Checkpoint every 500 records
                    if records_processed % __CHECKPOINT_INTERVAL == 0:
                        op.checkpoint(state={
                            'cursor_mode': 'INCREMENTAL',
                            'last_sequence': last_sequence,
                            'last_updated_at': highest_updated_at
                        })
                        logger.info(f"INCREMENTAL: Checkpointed at {records_processed} records (seq: {last_sequence})")

                # Check if we've processed all available updates
                if len(records) < __DEFAULT_LIMIT:
                    has_more = False
                else:
                    # Update filter for next batch
                    api_filters = _build_incremental_filters(last_sequence, highest_updated_at, custom_filters)

        # Final checkpoint
        final_state = {
            'cursor_mode': cursor_mode,
            'last_sequence': last_sequence,
            'last_updated_at': highest_updated_at
        }
        op.checkpoint(state=final_state)
        logger.info(f"Sync completed in {cursor_mode} mode. Total records processed: {records_processed}")

    except KeyError as e:
        logger.error(f"Missing required configuration field: {e}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Invalid custom_filter_json format: {e}")
        raise
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error during sync: {e}")
        raise
    except Exception as e:
        logger.critical(f"Update failed: {str(e)}")
        raise

connector = Connector(schema=schema, update=update)

if __name__ == "__main__":
    with open("configuration.json", 'r') as f:
        configuration = json.load(f)
    connector.debug(configuration=configuration)