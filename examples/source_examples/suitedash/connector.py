# This is an example for how to work with the fivetran_connector_sdk module.
# This is an example to show how to sync CRM data from SuiteDash's API by using Connector SDK. You need to provide your SuiteDash Public ID and Secret Key for this example to work.
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details


# Import required classes from fivetran_connector_sdk
# For supporting Connector operations like Update() and Schema()
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# Import requests for making HTTP API calls to SuiteDash
import requests

# Import json for configuration file handling
import json


_BASE_URL = "https://app.suitedash.com/secure-api"  # Base URL for SuiteDash API
_COMPANIES_ENDPOINT = "/companies"  # Endpoint for fetching companies
_CONTACTS_ENDPOINT = "/contacts"  # Endpoint for fetching contacts
_DEFAULT_PAGE_SIZE = 50  # Default page size for API requests


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    This function is called at the start of the update method to ensure that the connector has all necessary configuration values.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing.
    """

    # Validate required configuration parameters for SuiteDash API
    required_configs = ["public_id", "secret_key"]
    for key in required_configs:
        if key not in configuration:
            raise ValueError(f"Missing required configuration value: {key}")


def flatten_nested_object(obj: dict, prefix: str = "") -> dict:
    """
    Flatten nested dictionary objects into a flat structure with prefixed keys.
    Converts nested dictionaries like {"contact": {"name": "John"}} to {"contact_name": "John"}.
    Args:
        obj (dict): The nested dictionary to flatten
        prefix (str): The prefix to add to the flattened keys
    Returns:
        dict: A flattened dictionary with prefixed keys
    """
    flattened = {}
    if not obj:
        return flattened

    for key, value in obj.items():
        new_key = f"{prefix}_{key}" if prefix else key
        if isinstance(value, dict) and value:
            flattened.update(flatten_nested_object(value, new_key))
        else:
            flattened[new_key] = value
    return flattened


def process_company_record(company: dict) -> dict:
    """
    Process a company record by flattening nested objects and handling arrays.
    Transforms raw SuiteDash company data into a flat structure suitable for database storage.
    Args:
        company (dict): Raw company record from SuiteDash API
    Returns:
        dict: Processed company record ready for upsert operation
    """
    processed = {}

    # Copy basic fields from company record
    basic_fields = [
        "uid",
        "role",
        "name",
        "phone",
        "home_phone",
        "work_phone",
        "shop_phone",
        "full_address",
        "website",
        "background_info",
        "created",
    ]
    for field_name in basic_fields:
        processed[field_name] = company.get(field_name)

    # Flatten primaryContact object
    primary_contact = company.get("primaryContact", {})
    if primary_contact:
        flattened_contact = flatten_nested_object(primary_contact, "primaryContact")
        processed.update(flattened_contact)

    # Flatten category object
    category = company.get("category", {})
    if category:
        flattened_category = flatten_nested_object(category, "category")
        processed.update(flattened_category)

    # Flatten address object
    address = company.get("address", {})
    if address:
        flattened_address = flatten_nested_object(address, "address")
        processed.update(flattened_address)

    # Convert tags array to comma-separated string
    tags = company.get("tags", [])
    processed["tags"] = ",".join(tags) if tags else None

    return processed


def process_contact_record(contact: dict) -> dict:
    """
    Process a contact record by flattening nested objects and handling arrays.
    Transforms raw SuiteDash contact data into a flat structure suitable for database storage.
    Args:
        contact (dict): Raw contact record from SuiteDash API
    Returns:
        dict: Processed contact record ready for upsert operation
    """
    processed = {}

    # Copy basic fields from contact record
    basic_fields = [
        "uid",
        "first_name",
        "last_name",
        "email",
        "name_prefix",
        "home_email",
        "work_email",
        "phone",
        "home_phone",
        "work_phone",
        "shop_phone",
        "title",
        "active",
        "role",
        "full_address",
        "website",
        "background_info",
        "created",
    ]
    for field_name in basic_fields:
        processed[field_name] = contact.get(field_name)

    # Flatten address object
    address = contact.get("address", {})
    if address:
        flattened_address = flatten_nested_object(address, "address")
        processed.update(flattened_address)

    # Convert tags and circles arrays to comma-separated strings
    tags = contact.get("tags", [])
    processed["tags"] = ",".join(tags) if tags else None

    circles = contact.get("circles", [])
    processed["circles"] = (
        ",".join([circle.get("name", "") for circle in circles if isinstance(circle, dict)])
        if circles
        else None
    )

    return processed


def extract_contact_company_relationships(contact: dict) -> list:
    """
    Extract contact-company relationships from a contact record to create junction table records.
    Creates separate relationship records for each company associated with a contact.
    Args:
        contact (dict): Contact record from SuiteDash API
    Returns:
        list: List of contact-company relationship records for junction table
    """
    relationships = []
    contact_uid = contact.get("uid")
    companies = contact.get("companies", [])

    for company in companies:
        if isinstance(company, dict):
            relationship = {
                "contact_uid": contact_uid,
                "company_uid": company.get("uid"),
                "company_name": company.get("name"),
            }
            relationships.append(relationship)

    return relationships


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
            "table": "companies",  # Companies table from /companies endpoint
            "primary_key": ["uid"],  # Primary key is the unique company ID
        },
        {
            "table": "contacts",  # Contacts table from /contacts endpoint
            "primary_key": ["uid"],  # Primary key is the unique contact ID
        },
        {
            "table": "contact_company_relationships",  # Junction table for contact-company relationships
            "primary_key": ["contact_uid", "company_uid"],  # Composite primary key
        },
    ]


def get_api_headers(configuration: dict) -> dict:
    """
    Build authentication headers required for SuiteDash API requests.
    Creates headers with public ID and secret key for API authentication.
    Args:
        configuration (dict): Configuration dictionary containing API credentials
    Returns:
        dict: Headers dictionary for API requests including authentication
    """
    return {
        "X-Public-ID": configuration["public_id"],
        "X-Secret-Key": configuration["secret_key"],
        "Content-Type": "application/json",
    }


def make_api_request(url: str, headers: dict, params=None) -> dict:
    """
    Make an authenticated HTTP GET request to the SuiteDash API with error handling.
    Handles HTTP errors and provides meaningful error messages for API failures.
    Args:
        url (str): The API endpoint URL to request
        headers (dict): Authentication headers for the request
        params (dict, optional): Query parameters for the request
    Returns:
        dict: JSON response parsed as dictionary
    Raises:
        RuntimeError: If the API request fails or returns an error status
    """
    try:
        log.info(f"Making API call to: {url} with params: {params}")
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        log.severe(f"API request failed: {str(e)}")
        raise RuntimeError(f"Failed to fetch data from SuiteDash API: {str(e)}")


def sync_companies(configuration: dict, state: dict):
    """
    Sync companies data from SuiteDash /companies API endpoint with pagination support.
    Processes all company records, flattens nested data, and upserts to destination.
    Args:
        configuration (dict): Configuration dictionary containing API credentials
        state (dict): State dictionary for maintaining sync progress (unused after checkpoint removal)
    """
    log.info("Starting companies sync")
    headers = get_api_headers(configuration)
    page = 1
    more_data = True
    companies_processed = 0

    while more_data:
        url = f"{_BASE_URL}{_COMPANIES_ENDPOINT}"
        params = {"page": page}

        response_data = make_api_request(url, headers, params)

        if not response_data.get("success", False):
            log.severe(f"API returned error: {response_data.get('message', 'Unknown error')}")
            break

        companies = response_data.get("data", [])
        if not companies:
            log.info("No more companies to process")
            break

        # Process each company record
        for company in companies:
            processed_company = process_company_record(company)

            # The 'upsert' operation is used to insert or update data in the destination table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into.
            # - The second argument is a dictionary containing the data to be upserted
            op.upsert(table="companies", data=processed_company)

            companies_processed += 1

        # Check pagination metadata to determine if more pages exist
        pagination = response_data.get("meta", {}).get("pagination", {})
        next_page_url = pagination.get("nextPage")

        if next_page_url:
            page += 1
        else:
            more_data = False

        log.info(f"Processed page {page - 1} of companies, total processed: {companies_processed}")

    log.info(f"Companies sync completed. Total companies processed: {companies_processed}")


def sync_contacts(configuration: dict, state: dict):
    """
    Sync contacts data from SuiteDash /contacts API endpoint with pagination support.
    Processes contact records, extracts contact-company relationships, and upserts all data.
    Args:
        configuration (dict): Configuration dictionary containing API credentials
        state (dict): State dictionary for maintaining sync progress (unused after checkpoint removal)
    """
    log.info("Starting contacts sync")
    headers = get_api_headers(configuration)
    page = 1
    more_data = True
    contacts_processed = 0
    relationships_processed = 0

    while more_data:
        url = f"{_BASE_URL}{_CONTACTS_ENDPOINT}"
        params = {"page": page}

        response_data = make_api_request(url, headers, params)

        if not response_data.get("success", False):
            log.severe(f"API returned error: {response_data.get('message', 'Unknown error')}")
            break

        contacts = response_data.get("data", [])
        if not contacts:
            log.info("No more contacts to process")
            break

        # Process each contact record
        for contact in contacts:
            # Process main contact data
            processed_contact = process_contact_record(contact)

            # The 'upsert' operation is used to insert or update data in the destination table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into.
            # - The second argument is a dictionary containing the data to be upserted
            op.upsert(table="contacts", data=processed_contact)

            contacts_processed += 1

            # Extract and process contact-company relationships
            relationships = extract_contact_company_relationships(contact)
            for relationship in relationships:
                # The 'upsert' operation is used to insert or update data in the destination table.
                # The op.upsert method is called with two arguments:
                # - The first argument is the name of the table to upsert the data into.
                # - The second argument is a dictionary containing the data to be upserted
                op.upsert(table="contact_company_relationships", data=relationship)

                relationships_processed += 1

        # Check pagination metadata to determine if more pages exist
        pagination = response_data.get("meta", {}).get("pagination", {})
        next_page_url = pagination.get("nextPage")

        if next_page_url:
            page += 1
        else:
            more_data = False

        log.info(
            f"Processed page {page - 1} of contacts, total processed: {contacts_processed}, relationships: {relationships_processed}"
        )

    log.info(
        f"Contacts sync completed. Total contacts processed: {contacts_processed}, relationships: {relationships_processed}"
    )


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

    log.warning("Example: Source Examples : SuiteDash")

    # Validate the configuration to ensure it contains all required values.
    validate_configuration(configuration=configuration)

    try:
        # Sync companies data from /companies endpoint
        sync_companies(configuration, state)

        # Sync contacts data from /contacts endpoint
        sync_contacts(configuration, state)

        log.info("SuiteDash sync completed successfully")

    except Exception as e:
        # In case of an exception, raise a runtime error
        raise RuntimeError(f"Failed to sync data from SuiteDash: {str(e)}")


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
