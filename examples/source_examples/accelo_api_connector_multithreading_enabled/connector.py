"""
Accelo API Connector for Fivetran

This module implements a connector for syncing data from the Accelo API.
It handles OAuth2 authentication, rate limiting, and data synchronization for companies,
invoices, payments, prospects, jobs, and staff.
It is an example of multithreading the extraction of data from the source to improve connector performance.
Multithreading helps to make api calls in parallel to pull data faster.
It is also an example of using OAuth 2.0 client credentials flow.
Requires Accelo OAuth credentials to be passed in to work.

Author: Example submitted by our amazing community member Ahmed Zedan
Date: 2024-09-20
"""
import json
from fivetran_connector_sdk import Connector, Logging as log, Operations as op
import requests
import time
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from threading import local
from copy import deepcopy

from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Constants
BASE_URL = "https://{deployment}.api.accelo.com/api/v0"
RATE_LIMIT = 5000  # Requests per hour
RATE_LIMIT_INTERVAL = 3600  # 1 hour in seconds
CHECKPOINT_INTERVAL = 1000  # Number of records to process before creating a checkpoint
MAX_WORKERS = 5  # Number of concurrent API requests
REQUEST_TIMEOUT = 30  # Default timeout in seconds
SYNC_TIMEOUT = 3600  # Timeout for the entire sync operation in seconds
BATCH_SIZE = 100  # Maximum number of records per page in Accelo API (API limit)
RETRIES = 3

# Create a thread-local state object
thread_local_state = local()


def get_access_token(client_id, client_secret, deployment):
    """
    Obtain an access token from the Accelo API using OAuth 2.0 client credentials flow.

    Args:
        client_id (str): The OAuth 2.0 client ID.
        client_secret (str): The OAuth 2.0 client secret.
        deployment (str): The Accelo deployment name.

    Returns:
        str: The access token if successful.

    Raises:
        Exception: If authentication fails.
    """
    log.info("Obtaining access token")
    token_url = f"https://{deployment}.api.accelo.com/oauth2/v0/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "read(all)"
    }
    response = requests.post(token_url, data=data)
    if response.status_code == 200:
        log.info("Access token obtained successfully")
        return response.json()["access_token"]
    else:
        log.severe(f"Failed to obtain access token: {response.text}")
        raise Exception("Failed to obtain access token")


def fetch_data(endpoint, access_token, params=None, timeout=REQUEST_TIMEOUT, retries=RETRIES):
    """
    Fetch data from the Accelo API with retry logic.

    Args:
        endpoint (str): The API endpoint to fetch data from.
        access_token (str): The OAuth 2.0 access token.
        params (dict, optional): Query parameters for the API request.
        timeout (int, optional): Request timeout in seconds.
        retries (int, optional): Number of retry attempts.

    Returns:
        list: The JSON response data or None if all retries fail.
    """
    url = f"{BASE_URL}/{endpoint}"
    headers = {"Authorization": f"Bearer {access_token}"}

    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=timeout)
            response.raise_for_status()
            return response.json().get("response", [])
        except requests.exceptions.RequestException as e:
            log.warning(f"Error fetching data from {endpoint} (attempt {attempt + 1}/{retries}): {str(e)}")
            if attempt == retries - 1:
                log.severe(f"Failed to fetch data from {endpoint} after {retries} retries")
                return None
            time.sleep(2 ** attempt)  # Exponential backoff


def update(configuration: dict, state: dict):
    """
    Main update function for the connector.

    This function orchestrates the entire sync process:
    1. Authenticates with the Accelo API
    2. Syncs data for companies, invoices, payments, prospects, jobs, and staff
    3. Yields checkpoints to update the sync state

    Args:
        configuration (dict): The connector configuration.
        state (dict): The current state of the connector.

    Yields:
        dict: Checkpoint operations to update the sync state.
    """
    log.info(f"Starting update process. Initial state: {state}")
    thread_local_state.state = deepcopy(state)

    client_id = configuration.get("client_id")
    client_secret = configuration.get("client_secret")
    deployment = configuration.get("deployment")

    if not all([client_id, client_secret, deployment]):
        log.severe("Missing required configuration parameters")
        return

    try:
        access_token = get_access_token(client_id, client_secret, deployment)
        global BASE_URL
        BASE_URL = f"https://{deployment}.api.accelo.com/api/v0"

        update_start_time = time.time()

        for entity_sync in [sync_companies, sync_invoices, sync_payments, sync_prospects, sync_jobs, sync_staff]:
            yield from entity_sync(access_token)

        yield op.checkpoint(thread_local_state.state)

        update_duration = time.time() - update_start_time
        log.info(
            f"Update process completed. Total time: {update_duration:.2f} seconds. Final state: {thread_local_state.state}")
    except Exception as e:
        log.severe(f"Update process failed: {str(e)}")


def sync_entity(entity_name, access_token, fields, last_sync_key, process_record=None, date_field="date_modified",
                timeout=SYNC_TIMEOUT, batch_size=BATCH_SIZE, fetch_data_func=None):
    """
    Sync data for a specific entity from Accelo API.

    This function handles the core logic for syncing each entity type:
    - Fetches data in batches
    - Processes records
    - Yields upsert operations
    - Updates the sync state

    Args:
        entity_name (str): The name of the entity being synced.
        access_token (str): The OAuth 2.0 access token.
        fields (list): The fields to sync for this entity.
        last_sync_key (str): The key to use for storing the last sync time in the state.
        process_record (callable, optional): A function to process each record before upserting.
        date_field (str, optional): The field to use for incremental syncing.
        timeout (int, optional): Timeout for the entire sync operation in seconds.
        batch_size (int, optional): Number of records to fetch per API request.
        fetch_data_func (callable, optional): A custom function to fetch data for this entity.

    Yields:
        dict: Upsert operations for each synced record.
    """
    log.info(f"Starting sync for {entity_name}")
    try:
        last_sync = thread_local_state.state.get(last_sync_key, "1970-01-01T00:00:00Z")
        last_sync_unix = max(0, int(datetime.fromisoformat(last_sync.replace("Z", "+00:00")).timestamp()))
        log.info(f"Last sync time for {entity_name}: {last_sync} (Unix: {last_sync_unix})")

        params = {
            "_fields": ",".join(fields + [date_field]),
            "_filters": f"{date_field}_after({last_sync_unix})",
            "_limit": batch_size,
            "_page": 0,
            "_order_by": date_field
        }

        records_processed = 0
        entities_received = 0
        start_time = time.time()
        max_date_value = last_sync_unix

        def fetch_page(page):
            nonlocal entities_received, max_date_value
            current_params = params.copy()
            current_params['_page'] = page
            try:
                entities = fetch_data_func(current_params) if fetch_data_func else fetch_data(entity_name, access_token,
                                                                                              current_params)
                if entities:
                    entities_received += len(entities)
                    for entity in entities:
                        entity_id = entity.get('id', 'unknown')
                        entity_date = entity.get(date_field, 'unknown')

                        if entity_date != 'unknown':
                            try:
                                if isinstance(entity_date, str) and 'T' in entity_date:
                                    dt = datetime.fromisoformat(entity_date.replace('Z', '+00:00'))
                                else:
                                    timestamp = int(entity_date)
                                    dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
                                date_value_unix = int(dt.timestamp())
                                if date_value_unix > max_date_value:
                                    max_date_value = date_value_unix
                            except ValueError:
                                log.warning(f"Could not convert {date_field} '{entity_date}' to int for {entity_name}")

                return entities
            except Exception as e:
                log.severe(f"Error fetching {entity_name} data for page {page}: {str(e)}")
                return []

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            page = 0
            while True:
                futures = [executor.submit(fetch_page, p) for p in range(page, page + MAX_WORKERS)]
                results = [future.result() for future in futures]
                all_entities = [entity for page_entities in results for entity in page_entities if page_entities]

                for entity in all_entities:
                    if process_record:
                        entity = process_record(entity)
                    if entity:
                        yield op.upsert(entity_name, {field: entity.get(field) for field in fields})
                        records_processed += 1
                        if records_processed % 100 == 0:
                            log.info(f"Processed {records_processed} records for {entity_name}")

                if not any(results) or time.time() - start_time >= timeout:
                    break
                page += MAX_WORKERS

        new_last_sync = datetime.fromtimestamp(max(max_date_value, int(time.time())), tz=timezone.utc).strftime(
            '%Y-%m-%dT%H:%M:%SZ')
        thread_local_state.state[last_sync_key] = new_last_sync
        log.info(
            f"Sync completed for {entity_name}. Entities received: {entities_received}, Records processed: {records_processed}. Time taken: {time.time() - start_time:.2f} seconds")

    except Exception as e:
        log.severe(f"Unhandled exception during sync of {entity_name}: {str(e)}")
        log.severe(f"Exception details: {type(e).__name__}: {str(e)}")
        import traceback
        log.severe(f"Traceback: {traceback.format_exc()}")

    sync_duration = time.time() - start_time
    log.info(
        f"{entity_name.capitalize()} sync completed. Entities received: {entities_received}, Records processed: {records_processed}. Time taken: {sync_duration:.2f} seconds")


def sync_companies(access_token):
    """
    Sync company data from Accelo API.

    This function defines the fields to sync for companies and provides
    a custom record processing function to handle data type conversions.
    """
    company_fields = [
        "id", "name", "website", "phone", "date_created",
        "date_modified", "date_last_interacted", "comments", "standing", "status",
        "postal_address", "default_affiliation"
    ]

    def process_company_record(record):
        # Convert integer fields
        int_fields = ["id", "status", "postal_address", "default_affiliation"]
        for field in int_fields:
            value = record.get(field)
            if value is not None:
                try:
                    record[field] = int(value)
                except (ValueError, TypeError):
                    log.warning(f"Could not convert field {field} with value '{value}' to int")
                    record[field] = None

        date_fields = ["date_created", "date_modified", "date_last_interacted"]
        convertDateFields(date_fields, record)

        return record

    yield from sync_entity(
        entity_name="companies",
        access_token=access_token,
        fields=company_fields,
        last_sync_key="last_company_sync",
        process_record=process_company_record,
        date_field="date_modified",
        timeout=SYNC_TIMEOUT,
        batch_size=BATCH_SIZE
    )


def sync_invoices(access_token):
    """
    Sync invoice data from Accelo API.

    This function defines the fields to sync for invoices and provides
    a custom record processing function to handle data type conversions.
    """
    invoice_fields = [
        "id", "subject", "amount", "against_type", "against_id", "notes", "invoice_number",
        "currency_id", "owner_id", "tax", "outstanding", "modified_by",
        "date_raised", "date_due", "date_modified"
    ]

    def process_invoice_record(record):
        # Convert integer fields
        int_fields = ["id", "against_id", "invoice_number", "currency_id", "owner_id", "modified_by"]
        for field in int_fields:
            value = record.get(field)
            if value is not None:
                try:
                    record[field] = int(value)
                except (ValueError, TypeError):
                    log.warning(f"Could not convert field {field} with value '{value}' to int")
                    record[field] = None

        # Convert float fields
        float_fields = ["amount", "tax", "outstanding"]
        for field in float_fields:
            value = record.get(field)
            if value is not None:
                try:
                    record[field] = float(value)
                except (ValueError, TypeError):
                    log.warning(f"Could not convert field {field} with value '{value}' to float")
                    record[field] = None

        date_fields = ["date_raised", "date_due", "date_modified"]
        convertDateFields(date_fields, record)

        return record

    yield from sync_entity(
        entity_name="invoices",
        access_token=access_token,
        fields=invoice_fields,
        last_sync_key="last_invoice_sync",
        process_record=process_invoice_record,
        date_field="date_modified",
        timeout=SYNC_TIMEOUT,
        batch_size=BATCH_SIZE
    )


def sync_payments(access_token):
    """
    Sync payment data from Accelo API.

    This function defines the fields to sync for payments and provides
    a custom record processing function to handle data type conversions.
    """
    payment_fields = [
        "id", "receipt_id", "amount", "currency_id", "method_id", "against_id", "against_type",
        "date_created", "created_by_staff_id", "direction", "payment_currency", "payment_method", "payment_receipt"
    ]

    def process_payment_record(record):
        # Convert integer fields
        int_fields = [
            "id", "receipt_id", "currency_id", "method_id", "against_id",
            "created_by_staff_id", "payment_currency", "payment_method", "payment_receipt"
        ]
        for field in int_fields:
            value = record.get(field)
            if value is not None:
                try:
                    record[field] = int(value)
                except (ValueError, TypeError):
                    log.warning(f"Could not convert field {field} with value '{value}' to int")
                    record[field] = None

        # Convert float fields
        float_fields = ["amount"]
        for field in float_fields:
            value = record.get(field)
            if value is not None:
                try:
                    record[field] = float(value)
                except (ValueError, TypeError):
                    log.warning(f"Could not convert field {field} with value '{value}' to float")
                    record[field] = None

        date_fields = ["date_created"]
        convertDateFields(date_fields, record)

        return record

    yield from sync_entity(
        entity_name="payments",
        access_token=access_token,
        fields=payment_fields,
        last_sync_key="last_payment_sync",
        process_record=process_payment_record,
        date_field="date_created",
        timeout=SYNC_TIMEOUT,
        batch_size=BATCH_SIZE
    )


def sync_prospects(access_token):
    """
    Sync prospect data from Accelo API.

    This function defines the fields to sync for prospects and provides
    a custom record processing function to handle data type conversions.
    """
    prospect_fields = [
        "id", "title", "date_created", "date_actioned", "date_due", "date_last_interacted",
        "date_modified", "weighting", "value", "success", "comments", "progress",
        "value_weighted", "won_by_id", "cancelled_by_id", "abandoned_by_id",
        "contact", "manager", "prospect_type", "status", "standing", "prospect_probability", "affiliation"
    ]

    def process_prospect_record(record):
        # Convert integer fields
        int_fields = [
            "id", "weighting", "value_weighted", "won_by_id",
            "cancelled_by_id", "abandoned_by_id", "contact",
            "manager", "prospect_type", "status", "prospect_probability",
            "affiliation"
        ]
        for field in int_fields:
            value = record.get(field)
            if value is not None:
                try:
                    record[field] = int(value)
                except (ValueError, TypeError):
                    log.warning(f"Could not convert field {field} with value '{value}' to int")
                    record[field] = None

        # Convert float fields
        float_fields = ["value", "progress"]
        for field in float_fields:
            value = record.get(field)
            if value is not None:
                try:
                    record[field] = float(value)
                except (ValueError, TypeError):
                    log.warning(f"Could not convert field {field} with value '{value}' to float")
                    record[field] = None

        # Convert date fields
        date_fields = [
            "date_created", "date_actioned", "date_due",
            "date_last_interacted", "date_modified"
        ]
        convertDateFields(date_fields, record)

        # Convert 'success' field to boolean
        value = record.get('success')
        if value is not None:
            try:
                if isinstance(value, bool):
                    record['success'] = value
                elif isinstance(value, str):
                    if value.lower() == 'yes' or value.lower() == 'true':
                        record['success'] = True
                    elif value.lower() == 'no' or value.lower() == 'false':
                        record['success'] = False
                    else:
                        raise ValueError(f"Unexpected string value for success: {value}")
                else:
                    raise ValueError(f"Unexpected type for success: {type(value)}")
            except (ValueError, TypeError) as e:
                log.warning(f"Could not convert field 'success' with value '{value}' to bool: {e}")
                record['success'] = None

        return record

    yield from sync_entity(
        entity_name="prospects",
        access_token=access_token,
        fields=prospect_fields,
        last_sync_key="last_prospect_sync",
        process_record=process_prospect_record,
        date_field="date_modified",
        timeout=SYNC_TIMEOUT,
        batch_size=BATCH_SIZE
    )


def sync_jobs(access_token):
    """
    Sync job data from Accelo API.
    """
    job_fields = [
        "id", "title", "status", "standing", "date_created", "date_started",
        "date_due", "date_completed", "date_modified", "manager", "company",
        "contact", "description", "value"
    ]

    def process_job_record(record):
        # Convert integer fields
        int_fields = ["id", "status", "manager", "company", "contact"]
        for field in int_fields:
            value = record.get(field)
            if value is not None:
                try:
                    record[field] = int(value)
                except (ValueError, TypeError):
                    log.warning(f"Could not convert field {field} with value '{value}' to int")
                    record[field] = None

        # Convert float fields
        float_fields = ["value"]
        for field in float_fields:
            value = record.get(field)
            if value is not None:
                try:
                    record[field] = float(value)
                except (ValueError, TypeError):
                    log.warning(f"Could not convert field {field} with value '{value}' to float")
                    record[field] = None

        date_fields = ["date_created", "date_started", "date_due", "date_completed", "date_modified"]
        convertDateFields(date_fields, record)

        return record

    yield from sync_entity(
        entity_name="jobs",
        access_token=access_token,
        fields=job_fields,
        last_sync_key="last_job_sync",
        process_record=process_job_record,
        date_field="date_modified",
        timeout=SYNC_TIMEOUT,
        batch_size=BATCH_SIZE
    )


def sync_staff(access_token):
    """
    Sync staff data from Accelo API.
    """
    staff_fields = [
        "id", "firstname", "surname", "email", "position", "title",
        "date_created", "date_modified", "status"
    ]

    def process_staff_record(record):
        # Convert integer fields
        int_fields = ["id", "status"]
        for field in int_fields:
            value = record.get(field)
            if value is not None:
                try:
                    record[field] = int(value)
                except (ValueError, TypeError):
                    log.warning(f"Could not convert field {field} with value '{value}' to int")
                    record[field] = None

        # Convert date fields
        date_fields = ["date_created", "date_modified"]
        convertDateFields(date_fields, record)

        return record

    yield from sync_entity(
        entity_name="staff",
        access_token=access_token,
        fields=staff_fields,
        last_sync_key="last_staff_sync",
        process_record=process_staff_record,
        date_field="date_modified",
        timeout=SYNC_TIMEOUT,
        batch_size=BATCH_SIZE
    )

def convertDateFields(date_fields, record):
    for field in date_fields:
        value = record.get(field)
        if value is not None:
            try:
                if isinstance(value, str) and 'T' in value:
                    # ISO 8601 format
                    dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
                else:
                    # Unix timestamp
                    timestamp = int(value)
                    dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
                record[field] = dt.strftime('%Y-%m-%dT%H:%M:%SZ')
            except (ValueError, TypeError) as e:
                log.warning(f"Could not convert field {field} with value '{value}' to datetime: {e}")
                record[field] = None

def schema(configuration: dict):
    """
    Define the schema for the connector.

    This function returns the schema definition for all entities:
    - companies
    - invoices
    - payments
    - prospects
    - jobs
    - staff

    It also validates that all required configuration keys are present.

    Args:
        configuration (dict): The connector configuration.

    Returns:
        list: The schema definition for all entities.

    Raises:
        ValueError: If a required configuration key is missing.
    """
    required_keys = ['deployment', 'client_id', 'client_secret']
    for key in required_keys:
        if key not in configuration:
            raise ValueError(f"Missing required configuration key: {key}")

    schema_definition = [
        {
            "table": "companies",
            "primary_key": ["id"],
            "columns": {
                "id": "INT",
                "name": "STRING",
                "website": "STRING",
                "phone": "STRING",
                "date_created": "UTC_DATETIME",
                "date_modified": "UTC_DATETIME",
                "date_last_interacted": "UTC_DATETIME",
                "comments": "STRING",
                "standing": "STRING",
                "status": "INT",
                "postal_address": "INT",
                "default_affiliation": "INT"
            }
        },
        {
            "table": "invoices",
            "primary_key": ["id"],
            "columns": {
                "id": "INT",
                "subject": "STRING",
                "amount": "FLOAT",
                "against_type": "STRING",
                "against_id": "INT",
                "notes": "STRING",
                "invoice_number": "INT",
                "currency_id": "INT",
                "owner_id": "INT",
                "tax": "FLOAT",
                "outstanding": "FLOAT",
                "modified_by": "INT",
                "date_raised": "UTC_DATETIME",
                "date_due": "UTC_DATETIME",
                "date_modified": "UTC_DATETIME"
            }
        },
        {
            "table": "payments",
            "primary_key": ["id"],
            "columns": {
                "id": "INT",
                "receipt_id": "INT",
                "amount": "FLOAT",
                "currency_id": "INT",
                "method_id": "INT",
                "against_id": "INT",
                "against_type": "STRING",
                "date_created": "UTC_DATETIME",
                "created_by_staff_id": "INT",
                "direction": "STRING",
                "payment_currency": "INT",
                "payment_method": "INT",
                "payment_receipt": "INT"
            }
        },
        {
            "table": "prospects",
            "primary_key": ["id"],
            "columns": {
                "id": "INT",
                "title": "STRING",
                "date_created": "UTC_DATETIME",
                "date_actioned": "UTC_DATETIME",
                "date_due": "UTC_DATETIME",
                "date_last_interacted": "UTC_DATETIME",
                "date_modified": "UTC_DATETIME",
                "weighting": "INT",
                "value": "FLOAT",
                "success": "BOOLEAN",
                "comments": "STRING",
                "progress": "FLOAT",
                "value_weighted": "INT",
                "won_by_id": "INT",
                "cancelled_by_id": "INT",
                "abandoned_by_id": "INT",
                "contact": "INT",
                "manager": "INT",
                "prospect_type": "INT",
                "status": "INT",
                "standing": "STRING",
                "prospect_probability": "INT",
                "affiliation": "INT"
            }
        },
        {
            "table": "jobs",
            "primary_key": ["id"],
            "columns": {
                "id": "INT",
                "title": "STRING",
                "status": "INT",
                "standing": "STRING",
                "date_created": "UTC_DATETIME",
                "date_started": "UTC_DATETIME",
                "date_due": "UTC_DATETIME",
                "date_completed": "UTC_DATETIME",
                "date_modified": "UTC_DATETIME",
                "manager": "INT",
                "company": "INT",
                "contact": "INT",
                "description": "STRING",
                "value": "FLOAT"
            }
        },
        {
            "table": "staff",
            "primary_key": ["id"],
            "columns": {
                "id": "INT",
                "firstname": "STRING",
                "surname": "STRING",
                "email": "STRING",
                "position": "STRING",
                "title": "STRING",
                "date_created": "UTC_DATETIME",
                "date_modified": "UTC_DATETIME",
                "status": "INT"
            }
        }
    ]
    return schema_definition


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    # Open the configuration.json file and load its contents into a dictionary.
    with open("configuration.json", 'r') as f:
        configuration = json.load(f)
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from your IDE.
    connector.debug(configuration=configuration)
