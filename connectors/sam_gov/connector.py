# This is a Fivetran Connector SDK implementation for SAM.gov Opportunities API.
"""
This connector fetches government contracting opportunities from the SAM.gov API.
It supports pagination and incremental sync to efficiently replicate opportunity data
including nested contact information, place of performance details, and related links.
"""
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details


# Import required classes from fivetran_connector_sdk
# For supporting Connector operations like Update() and Schema()
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# Standard library imports for API requests and data handling
import json  # For reading configuration from JSON file
import requests  # For making HTTP requests to SAM.gov API
import time  # For implementing retry delays
from datetime import datetime, timedelta  # For date handling and state management
from typing import Dict, List, Any  # For type hints to improve code clarity


# Constants for SAM.gov API configuration and pagination
__BASE_URL = "https://api.sam.gov/opportunities/v2/search"
__DEFAULT_PAGE_SIZE = 1000  # Maximum allowed by API
__CHECKPOINT_INTERVAL = 100  # Checkpoint every 100 records
__DATE_FORMAT = "%m/%d/%Y"  # SAM.gov API date format
__MAX_RETRIES = 3  # Maximum number of retry attempts
__RETRY_DELAY_BASE = 2  # Base delay for exponential backoff (seconds)
__INCREMENTAL_WINDOW_DAYS = 30  # Default overlap window for incremental syncs to capture updates


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    This function is called at the start of the update method to ensure that the connector
    has all necessary configuration values for SAM.gov API access.

    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing.
    """
    # Validate required configuration parameters for SAM.gov API
    required_configs = ["api_key", "posted_from", "posted_to"]
    for key in required_configs:
        if key not in configuration:
            raise ValueError(f"Missing required configuration value: {key}")

    # Validate date format and range
    try:
        posted_from_date = datetime.strptime(configuration["posted_from"], __DATE_FORMAT)
        posted_to_date = datetime.strptime(configuration["posted_to"], __DATE_FORMAT)

        # Check that posted_to is after posted_from
        if posted_to_date <= posted_from_date:
            raise ValueError("posted_to date must be after posted_from date")

        # Check that date range does not exceed 1 year (365 days)
        date_diff = posted_to_date - posted_from_date
        if date_diff.days > 365:
            raise ValueError(
                f"Date range between posted_from and posted_to cannot exceed 1 year. "
                f"Current range is {date_diff.days} days. "
                f"SAM.gov API limitation: maximum 1 year (365 days) between dates."
            )

    except ValueError as e:
        if "time data" in str(e):
            raise ValueError(f"Invalid date format. Use MM/dd/yyyy format: {str(e)}")
        else:
            raise e


def flatten_dict(data: Dict[str, Any], prefix: str = "", separator: str = "_") -> Dict[str, Any]:
    """
    Flatten nested dictionaries by creating new keys with prefixes.

    Args:
        data: Dictionary to flatten (can be None)
        prefix: Prefix for new keys
        separator: Separator between prefix and key

    Returns:
        Flattened dictionary
    """
    flattened = {}

    # Handle None data
    if data is None:
        return flattened

    for key, value in data.items():
        new_key = f"{prefix}{separator}{key}" if prefix else key

        if isinstance(value, dict) and value:  # Non-empty dict
            flattened.update(flatten_dict(value, new_key, separator))
        else:
            # Handle empty dicts and other values
            flattened[new_key] = value if value != {} else None

    return flattened


def extract_point_of_contact_records(opportunity_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Extract point of contact records for the breakout table.

    Args:
        opportunity_data: Main opportunity record

    Returns:
        List of point of contact records with foreign key reference
    """
    contacts = []
    notice_id = opportunity_data.get("noticeId")

    if "pointOfContact" in opportunity_data and opportunity_data["pointOfContact"]:
        for i, contact in enumerate(opportunity_data["pointOfContact"]):
            contact_record = {
                "notice_id": notice_id,  # Foreign key to main table
                "contact_index": i,  # To maintain order
                **contact,
            }
            contacts.append(contact_record)

    return contacts


def extract_naics_codes_records(opportunity_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Extract NAICS codes records for the breakout table.

    Args:
        opportunity_data: Main opportunity record

    Returns:
        List of NAICS code records with foreign key reference
    """
    naics_records = []
    notice_id = opportunity_data.get("noticeId")

    if "naicsCodes" in opportunity_data and opportunity_data["naicsCodes"]:
        for i, naics_code in enumerate(opportunity_data["naicsCodes"]):
            naics_record = {
                "notice_id": notice_id,  # Foreign key to main table
                "naics_code": naics_code,
                "code_index": i,  # To maintain order
            }
            naics_records.append(naics_record)

    return naics_records


def extract_links_records(opportunity_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Extract links records for the breakout table.

    Args:
        opportunity_data: Main opportunity record

    Returns:
        List of link records with foreign key reference
    """
    links_records = []
    notice_id = opportunity_data.get("noticeId")

    if "links" in opportunity_data and opportunity_data["links"]:
        for i, link in enumerate(opportunity_data["links"]):
            link_record = {
                "notice_id": notice_id,  # Foreign key to main table
                "link_index": i,  # To maintain order
                **link,
            }
            links_records.append(link_record)

    return links_records


def extract_resource_links_records(opportunity_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Extract resource links records for the breakout table.

    Args:
        opportunity_data: Main opportunity record

    Returns:
        List of resource link records with foreign key reference
    """
    resource_links_records = []
    notice_id = opportunity_data.get("noticeId")

    if "resourceLinks" in opportunity_data and opportunity_data["resourceLinks"]:
        for i, resource_link in enumerate(opportunity_data["resourceLinks"]):
            resource_link_record = {
                "notice_id": notice_id,  # Foreign key to main table
                "resource_link": resource_link,
                "link_index": i,  # To maintain order
            }
            resource_links_records.append(resource_link_record)

    return resource_links_records


def extract_office_address_record(opportunity_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract and flatten office address data.

    Args:
        opportunity_data: Main opportunity record

    Returns:
        Flattened office address fields
    """
    office_address_fields = {}

    if "officeAddress" in opportunity_data and opportunity_data["officeAddress"]:
        office_addr = opportunity_data["officeAddress"]
        office_address_fields = {
            "office_address_zipcode": office_addr.get("zipcode"),
            "office_address_city": office_addr.get("city"),
            "office_address_country_code": office_addr.get("countryCode"),
            "office_address_state": office_addr.get("state"),
        }

    return office_address_fields


def process_main_opportunity_record(opportunity: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process and format the main opportunity record with flattened nested objects.

    Args:
        opportunity: Raw opportunity data from API

    Returns:
        Formatted opportunity record ready for upsert
    """
    # Extract and flatten main opportunity data
    main_record = opportunity.copy()

    # Remove array fields that will be processed separately
    arrays_to_remove = ["pointOfContact", "naicsCodes", "links", "resourceLinks"]
    for field in arrays_to_remove:
        main_record.pop(field, None)

    # Extract and flatten office address
    office_address_fields = extract_office_address_record(opportunity)

    # Flatten nested objects like placeOfPerformance and award
    if "placeOfPerformance" in main_record and main_record["placeOfPerformance"]:
        place_data = flatten_dict(main_record.pop("placeOfPerformance"), "place_of_performance")
        main_record.update(place_data)
    elif "placeOfPerformance" in main_record:
        # Remove null placeOfPerformance field
        main_record.pop("placeOfPerformance")

    if "award" in main_record and main_record["award"]:
        award_data = flatten_dict(main_record.pop("award"), "award")
        main_record.update(award_data)
    elif "award" in main_record:
        # Remove null award field
        main_record.pop("award")

    # Remove officeAddress as it's been processed separately
    main_record.pop("officeAddress", None)

    # Convert camelCase to snake_case for consistency
    formatted_record = {
        "notice_id": main_record.get("noticeId"),
        "title": main_record.get("title"),
        "solicitation_number": main_record.get("solicitationNumber"),
        "full_parent_path_name": main_record.get("fullParentPathName"),
        "full_parent_path_code": main_record.get("fullParentPathCode"),
        "posted_date": main_record.get("postedDate"),
        "type": main_record.get("type"),
        "base_type": main_record.get("baseType"),
        "archive_type": main_record.get("archiveType"),
        "archive_date": main_record.get("archiveDate"),
        "type_of_set_aside_description": main_record.get("typeOfSetAsideDescription"),
        "type_of_set_aside": main_record.get("typeOfSetAside"),
        "response_dead_line": main_record.get("responseDeadLine"),
        "naics_code": main_record.get("naicsCode"),
        "classification_code": main_record.get("classificationCode"),
        "active": main_record.get("active"),
        "description": main_record.get("description"),
        "organization_type": main_record.get("organizationType"),
        "additional_info_link": main_record.get("additionalInfoLink"),
        "ui_link": main_record.get("uiLink"),
    }

    # Add office address fields
    formatted_record.update(office_address_fields)

    # Add flattened place of performance and award fields to the main record
    for key, value in main_record.items():
        if key.startswith(("place_of_performance_", "award_")):
            formatted_record[key] = value

    return formatted_record


def process_breakout_tables(opportunity: Dict[str, Any]):
    """
    Process and upsert all breakout table records for an opportunity.

    Args:
        opportunity: Raw opportunity data from API
    """
    # Process point of contact breakout table
    # Each contact becomes a separate record with a foreign key to the main opportunity
    contact_records = extract_point_of_contact_records(opportunity)
    for contact in contact_records:
        # Convert camelCase to snake_case for contact fields
        formatted_contact = {
            "notice_id": contact.get("notice_id"),
            "contact_index": contact.get("contact_index"),
            "fax": contact.get("fax"),
            "type": contact.get("type"),
            "email": contact.get("email"),
            "phone": contact.get("phone"),
            "title": contact.get("title"),
            "full_name": contact.get("fullName"),
        }
        # The 'upsert' operation is used to insert or update data in the destination table.
        # The first argument is the name of the destination table.
        # The second argument is a dictionary containing the record to be upserted.
        op.upsert(table="point_of_contact", data=formatted_contact)

    # Process NAICS codes breakout table
    naics_records = extract_naics_codes_records(opportunity)
    for naics in naics_records:
        # The 'upsert' operation is used to insert or update data in the destination table.
        # The first argument is the name of the destination table.
        # The second argument is a dictionary containing the record to be upserted.
        op.upsert(table="naics_code", data=naics)

    # Process links breakout table
    links_records = extract_links_records(opportunity)
    for link in links_records:
        # The 'upsert' operation is used to insert or update data in the destination table.
        # The first argument is the name of the destination table.
        # The second argument is a dictionary containing the record to be upserted.
        op.upsert(table="link", data=link)

    # Process resource links breakout table
    resource_links_records = extract_resource_links_records(opportunity)
    for resource_link in resource_links_records:
        # The 'upsert' operation is used to insert or update data in the destination table.
        # The first argument is the name of the destination table.
        # The second argument is a dictionary containing the record to be upserted.
        op.upsert(table="resource_link", data=resource_link)


def make_api_request(url: str, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Make HTTP request to SAM.gov API with comprehensive error handling.

    Args:
        url: API endpoint URL
        params: Request parameters

    Returns:
        API response data

    Raises:
        RuntimeError: If API request fails
    """
    try:
        response = requests.get(url, params=params, timeout=30)

        # Handle specific HTTP status codes
        if response.status_code == 429:
            raise RuntimeError(
                "SAM.gov API rate limit exceeded (429). "
                "Please wait before making additional requests. "
                "Rate limits vary by user role (federal vs non-federal). "
                "Consider reducing request frequency or contact SAM.gov for higher limits."
            )
        elif response.status_code == 401:
            raise RuntimeError(
                "Authentication failed (401). "
                "Please verify your API key is valid and active. "
                "You can regenerate your API key from your SAM.gov Account Details page."
            )
        elif response.status_code == 403:
            raise RuntimeError(
                "Access forbidden (403). "
                "Your API key may not have permission to access this endpoint, "
                "or you may have exceeded your daily quota."
            )
        elif response.status_code == 404:
            raise RuntimeError(
                "API endpoint not found (404). "
                "Please verify the SAM.gov API URL is correct. "
                "The API may be temporarily unavailable."
            )
        elif response.status_code >= 500:
            raise RuntimeError(
                f"SAM.gov API server error ({response.status_code}). "
                "The SAM.gov service may be temporarily unavailable. "
                "Please try again later."
            )

        # Raise for any other HTTP errors
        response.raise_for_status()
        return response.json()

    except requests.exceptions.Timeout:
        raise RuntimeError(
            "Request timeout after 30 seconds. "
            "SAM.gov API may be experiencing slow response times. "
            "Please try again later."
        )
    except requests.exceptions.ConnectionError:
        raise RuntimeError(
            "Failed to connect to SAM.gov API. "
            "Please check your internet connection and verify the API endpoint URL."
        )
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Failed to fetch data from SAM.gov API: {str(e)}")
    except json.JSONDecodeError as e:
        raise RuntimeError(
            f"Failed to parse API response as JSON: {str(e)}. "
            "The SAM.gov API may have returned an unexpected response format."
        )


def make_api_request_with_retry(url: str, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Make HTTP request to SAM.gov API with retry logic for transient errors.

    Args:
        url: API endpoint URL
        params: Request parameters

    Returns:
        API response data

    Raises:
        RuntimeError: If API request fails after all retries
    """
    last_exception = None

    for attempt in range(__MAX_RETRIES):
        try:
            return make_api_request(url, params)

        except RuntimeError as e:
            last_exception = e
            error_msg = str(e).lower()

            # Don't retry for non-transient errors
            if any(code in error_msg for code in ["401", "403", "404", "rate limit"]):
                log.warning(f"Non-retryable error on attempt {attempt + 1}: {str(e)}")
                raise e

            # Don't retry on the last attempt
            if attempt == __MAX_RETRIES - 1:
                break

            # Calculate exponential backoff delay (capped at 60 seconds)
            delay = min(60, __RETRY_DELAY_BASE * (2**attempt))
            log.warning(f"API request failed on attempt {attempt + 1}/{__MAX_RETRIES}: {str(e)}")
            log.info(f"Retrying in {delay} seconds...")
            time.sleep(delay)

    # If we get here, all retries failed
    raise RuntimeError(
        f"API request failed after {__MAX_RETRIES} attempts. Last error: {str(last_exception)}"
    )


def fetch_opportunities_page(
    api_key: str,
    posted_from: str,
    posted_to: str,
    limit: int = __DEFAULT_PAGE_SIZE,
    offset: int = 0,
) -> Dict[str, Any]:
    """
    Fetch a single page of opportunities from SAM.gov API with retry logic.

    Args:
        api_key: SAM.gov API key
        posted_from: Start date for opportunity posting (MM/dd/yyyy)
        posted_to: End date for opportunity posting (MM/dd/yyyy)
        limit: Number of records per page
        offset: Page offset for pagination

    Returns:
        API response containing opportunities data
    """
    params = {
        "api_key": api_key,
        "postedFrom": posted_from,
        "postedTo": posted_to,
        "limit": limit,
        "offset": offset,
    }

    log.info(f"Fetching opportunities page: offset={offset}, limit={limit}")
    return make_api_request_with_retry(__BASE_URL, params)


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
            "table": "opportunity",
            "primary_key": ["notice_id"],
            "columns": {
                # Define award_amount with DECIMAL type for precision in financial data
                "award_amount": {"type": "DECIMAL", "precision": 15, "scale": 2},
            },
        },
        {
            "table": "point_of_contact",
            "primary_key": ["notice_id", "contact_index"],
        },
        {
            "table": "naics_code",
            "primary_key": ["notice_id", "code_index"],
        },
        {
            "table": "link",
            "primary_key": ["notice_id", "link_index"],
        },
        {
            "table": "resource_link",
            "primary_key": ["notice_id", "link_index"],
        },
    ]


def calculate_sync_date_range(configuration: dict, state: dict) -> tuple:
    """
    Calculate the date range for syncing based on sync mode (initial vs incremental).
    Args:
        configuration: Configuration dictionary with sync settings.
        state: State dictionary tracking sync progress.
    Returns:
        Tuple of (posted_from, posted_to) date strings in MM/dd/yyyy format.
    Raises:
        ValueError: If required configuration is missing.
    """
    sync_mode = state.get("sync_mode", configuration.get("sync_mode", "initial"))
    initial_sync_completed = state.get("initial_sync_completed", False)

    # Initial/Historical Sync Mode
    if sync_mode == "initial" or not initial_sync_completed:
        posted_from = configuration.get("posted_from")
        posted_to = configuration.get("posted_to")

        if not posted_from or not posted_to:
            raise ValueError(
                "For initial sync, posted_from and posted_to are required in configuration"
            )

        # Validate the configuration dates
        validate_configuration(configuration=configuration)
        log.info(f"Initial sync mode: syncing from {posted_from} to {posted_to}")
        return posted_from, posted_to

    # Incremental Sync Mode
    last_posted_to = state.get("last_posted_to")
    if not last_posted_to:
        raise ValueError(
            "Incremental sync requires last_posted_to in state. Run initial sync first."
        )

    # Use default incremental window for overlap
    incremental_window_days = __INCREMENTAL_WINDOW_DAYS

    # Calculate new window with overlap to capture updates
    last_posted_to_date = datetime.strptime(last_posted_to, __DATE_FORMAT)
    posted_from_date = last_posted_to_date - timedelta(days=incremental_window_days)
    posted_from = posted_from_date.strftime(__DATE_FORMAT)

    # Use current date as end date
    posted_to_date = datetime.now()
    posted_to = posted_to_date.strftime(__DATE_FORMAT)

    # Check if window exceeds 1-year API limit
    date_diff = posted_to_date - posted_from_date
    if date_diff.days >= 365:
        # Adjust to 364-day window (API requires LESS than 365 days)
        posted_to_date = posted_from_date + timedelta(days=364)
        posted_to = posted_to_date.strftime(__DATE_FORMAT)
        date_diff = posted_to_date - posted_from_date
        log.warning(
            f"Date range exceeded 1-year limit. Adjusted to: {posted_from} to {posted_to} "
            f"(364 days - will continue in next sync)"
        )

    log.info(
        f"Incremental sync mode: syncing from {posted_from} to {posted_to} "
        f"(overlap: {incremental_window_days} days, window: {date_diff.days} days)"
    )
    return posted_from, posted_to


def process_single_opportunity(opportunity: dict, current_offset: int, index: int):
    """
    Process a single opportunity record and all its breakout tables.
    Args:
        opportunity: Raw opportunity data from API.
        current_offset: Current pagination offset for logging.
        index: Index of opportunity in current page for logging.
    Raises:
        Exception: If processing fails (caller should handle).
    """
    # Validate that we have the required notice_id
    if not opportunity.get("noticeId"):
        log.warning(
            f"Skipping opportunity {index + 1} at offset {current_offset}: missing noticeId"
        )
        return

    # Process and format the main opportunity record
    formatted_record = process_main_opportunity_record(opportunity)

    # The 'upsert' operation is used to insert or update data in the destination table.
    # The first argument is the name of the destination table.
    # The second argument is a dictionary containing the record to be upserted.
    op.upsert(table="opportunity", data=formatted_record)

    # Process all breakout tables for this opportunity
    process_breakout_tables(opportunity)


def update(configuration: dict, state: dict):
    """
    Define the update function which lets you configure how your connector fetches data.
    See the technical reference documentation for more details on the update function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
        state: a dictionary that holds the state of the connector.
    """
    log.info("SAM.gov Opportunities Connector: Starting sync")

    # Extract API key (always required)
    api_key = configuration.get("api_key")
    if not api_key:
        raise ValueError("Missing required configuration parameter: api_key")

    # Calculate date range based on sync mode (initial vs incremental)
    posted_from, posted_to = calculate_sync_date_range(configuration, state)

    # Get the state variable for pagination (resuming interrupted syncs)
    last_offset = state.get("last_offset", 0)
    total_records_processed = state.get("total_records_processed", 0)

    log.info(f"Resuming from offset: {last_offset}, total processed: {total_records_processed}")

    try:
        current_offset = last_offset

        while True:
            # Fetch a page of opportunities data from SAM.gov API
            # The API supports pagination with limit and offset parameters
            response_data = fetch_opportunities_page(
                api_key=api_key,
                posted_from=posted_from,
                posted_to=posted_to,
                limit=__DEFAULT_PAGE_SIZE,
                offset=current_offset,
            )

            total_records = response_data.get("totalRecords", 0)
            opportunities = response_data.get("opportunitiesData", [])

            # Break if no more opportunities to process
            if not opportunities:
                log.info("No more opportunities to process")
                break

            log.info(f"Processing {len(opportunities)} opportunities from offset {current_offset}")

            # Process each opportunity record with individual error handling
            for i, opportunity in enumerate(opportunities):
                try:
                    process_single_opportunity(opportunity, current_offset, i)
                except Exception as e:
                    # Log the error but continue processing other records
                    notice_id = opportunity.get("noticeId", "unknown")
                    log.warning(
                        f"Failed to process opportunity {notice_id} at offset {current_offset}: {str(e)}"
                    )
                    log.warning("Continuing with next opportunity...")
                    continue

            # Update pagination state after processing each page
            current_offset += len(opportunities)
            total_records_processed += len(opportunities)

            # Save the progress by checkpointing the state after processing each page. This is important for ensuring that the sync process can resume
            # from the correct position in case of next sync or interruptions.
            # Learn more about how and where to checkpoint by reading our best practices documentation
            # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
            checkpoint_state = {
                "last_offset": current_offset,
                "total_records_processed": total_records_processed,
                "last_sync_time": datetime.now().isoformat(),
            }
            op.checkpoint(checkpoint_state)
            log.info(
                f"Checkpointed at offset {current_offset}, processed {total_records_processed} total"
            )

            # Check if we've processed all available records
            if current_offset >= total_records:
                log.info(f"Completed sync: processed all {total_records} records")
                break

        # Final checkpoint with completion state
        # Save state for incremental sync in next run
        final_state = {
            "last_offset": 0,  # Reset offset for next sync
            "last_posted_to": posted_to,  # Track end date for incremental sync
            "total_records_processed": total_records_processed,
            "last_sync_time": datetime.now().isoformat(),
            "sync_mode": "incremental",  # Switch to incremental mode after first sync
            "initial_sync_completed": True,  # Mark initial sync as complete
            "sync_completed": True,
        }

        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        op.checkpoint(final_state)

        log.info(
            f"SAM.gov sync completed successfully. Total records processed: {total_records_processed}"
        )

    except Exception as e:
        # In case of an exception, raise a runtime error with detailed information
        error_msg = f"Failed to sync SAM.gov opportunities data: {str(e)}"
        log.warning(error_msg)
        raise RuntimeError(error_msg)


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Test the connector locally
    connector.debug()
