# Trustpilot API Connector
"""This connector demonstrates how to fetch data from Trustpilot API and upsert it into destination using the Fivetran Connector SDK.
Supports querying data from Reviews, Businesses, Categories, and Consumer APIs.
"""
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

# Import required libraries for API interactions
import requests
import json
import time
import random
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

INVALID_LITERAL_ERROR = "invalid literal"


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing.
    """
    required_configs = ["api_key", "business_unit_id", "consumer_id"]
    for key in required_configs:
        if key not in configuration:
            raise ValueError(f"Missing required configuration value: {key}")

    # Validate API key format (Trustpilot API keys are typically base64 encoded)
    if not str(configuration.get("api_key", "")).strip():
        raise ValueError("API key cannot be empty")

    # Validate business unit ID
    if not str(configuration.get("business_unit_id", "")).strip():
        raise ValueError("Business unit ID cannot be empty")

    # Validate consumer ID
    if not str(configuration.get("consumer_id", "")).strip():
        raise ValueError("Consumer ID cannot be empty")

    # Validate numeric configuration parameters
    try:
        sync_frequency = int(str(configuration.get("sync_frequency_hours", "4")))
        if sync_frequency < 1 or sync_frequency > 24:
            raise ValueError("Sync frequency must be between 1 and 24 hours")
    except ValueError as e:
        if INVALID_LITERAL_ERROR in str(e):
            raise ValueError("Sync frequency must be a valid number")
        raise

    try:
        initial_sync_days = int(str(configuration.get("initial_sync_days", "90")))
        if initial_sync_days < 1 or initial_sync_days > 365:
            raise ValueError("Initial sync days must be between 1 and 365")
    except ValueError as e:
        if INVALID_LITERAL_ERROR in str(e):
            raise ValueError("Initial sync days must be a valid number")
        raise

    try:
        max_records = int(str(configuration.get("max_records_per_page", "100")))
        if max_records < 1 or max_records > 100:
            raise ValueError("Max records per page must be between 1 and 100")
    except ValueError as e:
        if INVALID_LITERAL_ERROR in str(e):
            raise ValueError("Max records per page must be a valid number")
        raise


def get_trustpilot_endpoint() -> str:
    """
    Get the Trustpilot API endpoint.
    Returns:
        The base URL for Trustpilot API
    """
    return "https://api.trustpilot.com/v1"


def execute_api_request(
    endpoint: str,
    api_key: str,
    params: Optional[Dict[str, Any]] = None,
    configuration: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Execute an API request against Trustpilot API.
    Args:
        endpoint: The API endpoint to call
        api_key: Trustpilot API key
        params: Optional query parameters
        configuration: Optional configuration dictionary for timeout and retry settings
    Returns:
        The response data from the API
    """
    base_url = get_trustpilot_endpoint()
    url = f"{base_url}{endpoint}"

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    # Get timeout from configuration or use default
    timeout = 30
    if configuration:
        try:
            timeout = int(str(configuration.get("request_timeout_seconds", "30")))
        except (ValueError, TypeError):
            timeout = 30

    # Get retry attempts from configuration
    retry_attempts = 3
    if configuration:
        try:
            retry_attempts = int(str(configuration.get("retry_attempts", "3")))
        except (ValueError, TypeError):
            retry_attempts = 3

    # Implement retry logic with rate limiting and exponential backoff
    base_delay = 1
    max_delay = 60

    for attempt in range(retry_attempts):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=timeout)

            # Handle rate limiting (HTTP 429) specifically
            if response.status_code == 429:
                # Get retry-after header if available, otherwise use exponential backoff
                retry_after = response.headers.get("Retry-After")
                if retry_after:
                    try:
                        wait_time = int(retry_after)
                    except ValueError:
                        # If Retry-After is not a valid integer, use exponential backoff
                        wait_time = min(base_delay * (2**attempt), max_delay)
                else:
                    # Use exponential backoff with jitter
                    wait_time = min(base_delay * (2**attempt), max_delay)

                # Add jitter to prevent thundering herd problem
                jitter = random.uniform(0, min(wait_time * 0.1, 5))
                total_wait_time = wait_time + jitter

                log.info(
                    f"Rate limited (HTTP 429) on attempt {attempt + 1}, waiting {total_wait_time:.2f} seconds before retry"
                )
                time.sleep(total_wait_time)
                continue

            # For other HTTP errors, raise them to be handled by the outer except block
            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            if attempt == retry_attempts - 1:  # Last attempt
                log.severe(
                    f"Failed to execute API request to {endpoint} after {retry_attempts} attempts: {str(e)}"
                )
                raise RuntimeError(f"API request failed after {retry_attempts} attempts: {str(e)}")
            else:
                # For non-rate-limit errors, use exponential backoff
                wait_time = min(base_delay * (2**attempt), max_delay)
                jitter = random.uniform(0, min(wait_time * 0.1, 2))
                total_wait_time = wait_time + jitter

                log.info(
                    f"API request attempt {attempt + 1} failed, retrying in {total_wait_time:.2f} seconds: {str(e)}"
                )
                time.sleep(total_wait_time)
                continue

    # This should never be reached, but added for type safety
    raise RuntimeError("Unexpected error in API request execution")


def get_time_range(
    last_sync_time: Optional[str] = None, configuration: Optional[Dict[str, Any]] = None
) -> Dict[str, str]:
    """
    Generate dynamic time range for API queries.
    Args:
        last_sync_time: Last sync timestamp for incremental sync
        configuration: Optional configuration dictionary for initial sync days
    Returns:
        Dictionary with start and end time parameters
    """
    if last_sync_time:
        # Incremental sync: get data since last sync
        start_time = last_sync_time
        end_time = datetime.now().isoformat()
    else:
        # Initial sync: get all available data (configurable days)
        end_time = datetime.now().isoformat()

        # Get initial sync days from configuration or use default
        initial_sync_days = 90
        if configuration:
            try:
                initial_sync_days = int(str(configuration.get("initial_sync_days", "90")))
            except (ValueError, TypeError):
                initial_sync_days = 90

        start_time = (datetime.now() - timedelta(days=initial_sync_days)).isoformat()

    return {"start": start_time, "end": end_time}


def get_reviews_data(
    api_key: str,
    business_unit_id: str,
    last_sync_time: Optional[str] = None,
    configuration: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch reviews data from Trustpilot.
    Args:
        api_key: Trustpilot API key
        business_unit_id: Business unit ID
        last_sync_time: Last sync timestamp for incremental sync
        configuration: Optional configuration dictionary
    Returns:
        List of review data
    """
    time_range = get_time_range(last_sync_time, configuration)
    endpoint = f"/business-units/{business_unit_id}/reviews"

    # Get max records per page from configuration
    max_records = 100
    if configuration:
        try:
            max_records = int(str(configuration.get("max_records_per_page", "100")))
        except (ValueError, TypeError):
            max_records = 100

    params = {
        "perPage": max_records,
        "page": 1,
        "start": time_range["start"],
        "end": time_range["end"],
    }

    all_reviews = []
    page = 1

    while True:
        params["page"] = page
        response = execute_api_request(endpoint, api_key, params, configuration)

        reviews = response.get("reviews", [])
        if not reviews:
            break

        for review in reviews:
            all_reviews.append(
                {
                    "review_id": review.get("id", ""),
                    "business_unit_id": business_unit_id,
                    "consumer_id": review.get("consumer", {}).get("id", ""),
                    "consumer_name": review.get("consumer", {}).get("displayName", ""),
                    "stars": review.get("stars", 0),
                    "title": review.get("title", ""),
                    "text": review.get("text", ""),
                    "language": review.get("language", ""),
                    "created_at": review.get("createdAt", ""),
                    "updated_at": review.get("updatedAt", ""),
                    "status": review.get("status", ""),
                    "is_verified": review.get("isVerified", False),
                    "helpful_count": review.get("helpfulCount", 0),
                    "reply_text": review.get("reply", {}).get("text", ""),
                    "reply_created_at": review.get("reply", {}).get("createdAt", ""),
                    "timestamp": datetime.now().isoformat(),
                }
            )

        # Check if there are more pages
        if len(reviews) < max_records:
            break
        page += 1

    return all_reviews


def get_business_data(
    api_key: str, business_unit_id: str, configuration: Optional[Dict[str, Any]] = None
) -> List[Dict[str, Any]]:
    """
    Fetch business unit data from Trustpilot.
    Args:
        api_key: Trustpilot API key
        business_unit_id: Business unit ID
        configuration: Optional configuration dictionary
    Returns:
        List of business data
    """
    endpoint = f"/business-units/{business_unit_id}"

    response = execute_api_request(endpoint, api_key, configuration=configuration)

    business_data = []
    if response:
        business_data.append(
            {
                "business_unit_id": business_unit_id,
                "name": response.get("name", ""),
                "display_name": response.get("displayName", ""),
                "website_url": response.get("websiteUrl", ""),
                "country_code": response.get("countryCode", ""),
                "language": response.get("language", ""),
                "number_of_reviews": response.get("numberOfReviews", 0),
                "trust_score": response.get("trustScore", 0),
                "stars": response.get("stars", 0),
                "created_at": response.get("createdAt", ""),
                "updated_at": response.get("updatedAt", ""),
                "timestamp": datetime.now().isoformat(),
            }
        )

    return business_data


def get_categories_data(
    api_key: str, configuration: Optional[Dict[str, Any]] = None
) -> List[Dict[str, Any]]:
    """
    Fetch categories data from Trustpilot.
    Args:
        api_key: Trustpilot API key
        configuration: Optional configuration dictionary
    Returns:
        List of category data
    """
    endpoint = "/categories"

    response = execute_api_request(endpoint, api_key, configuration=configuration)

    categories_data = []
    if response and "categories" in response:
        for category in response["categories"]:
            categories_data.append(
                {
                    "category_id": category.get("id", ""),
                    "name": category.get("name", ""),
                    "localized_name": category.get("localizedName", ""),
                    "parent_id": category.get("parentId", ""),
                    "level": category.get("level", 0),
                    "created_at": category.get("createdAt", ""),
                    "updated_at": category.get("updatedAt", ""),
                    "timestamp": datetime.now().isoformat(),
                }
            )

    return categories_data


def get_consumers_data(
    api_key: str,
    consumer_id: str,
    last_sync_time: Optional[str] = None,
    configuration: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch consumer reviews data from Trustpilot Consumer API.
    Args:
        api_key: Trustpilot API key
        consumer_id: Consumer ID to fetch reviews for
        last_sync_time: Last sync timestamp for incremental sync
        configuration: Optional configuration dictionary
    Returns:
        List of consumer review data
    """
    endpoint = f"/consumers/{consumer_id}/reviews"

    # Get time range for incremental sync
    time_range = get_time_range(last_sync_time, configuration)

    # Get max records per page from configuration
    max_records = 100
    if configuration:
        try:
            max_records = int(str(configuration.get("max_records_per_page", "100")))
        except (ValueError, TypeError):
            max_records = 100

    params = {
        "perPage": max_records,
        "page": 1,
        "orderBy": "createdat.desc",
        "start": time_range["start"],
        "end": time_range["end"],
    }

    all_consumer_reviews = []
    page = 1

    while True:
        params["page"] = page
        response = execute_api_request(endpoint, api_key, params, configuration)

        reviews = response.get("reviews", [])
        if not reviews:
            break

        for review in reviews:
            all_consumer_reviews.append(
                {
                    "consumer_id": consumer_id,
                    "review_id": review.get("id", ""),
                    "business_unit_id": review.get("businessUnit", {}).get("id", ""),
                    "business_unit_name": review.get("businessUnit", {}).get("displayName", ""),
                    "stars": review.get("stars", 0),
                    "title": review.get("title", ""),
                    "text": review.get("text", ""),
                    "language": review.get("language", ""),
                    "status": review.get("status", ""),
                    "is_verified": review.get("isVerified", False),
                    "number_of_likes": review.get("numberOfLikes", 0),
                    "created_at": review.get("createdAt", ""),
                    "updated_at": review.get("updatedAt", ""),
                    "experienced_at": review.get("experiencedAt", ""),
                    "review_verification_level": review.get("reviewVerificationLevel", ""),
                    "counts_towards_trust_score": review.get("countsTowardsTrustScore", False),
                    "counts_towards_location_trust_score": review.get(
                        "countsTowardsLocationTrustScore", False
                    ),
                    "company_reply_text": review.get("companyReply", {}).get("text", ""),
                    "company_reply_created_at": review.get("companyReply", {}).get(
                        "createdAt", ""
                    ),
                    "company_reply_updated_at": review.get("companyReply", {}).get(
                        "updatedAt", ""
                    ),
                    "location_id": review.get("location", {}).get("id", ""),
                    "location_name": review.get("location", {}).get("name", ""),
                    "timestamp": datetime.now().isoformat(),
                }
            )

        # Check if there are more pages
        if len(reviews) < max_records:
            break
        page += 1

    return all_consumer_reviews


def get_invitations_data(
    api_key: str,
    business_unit_id: str,
    last_sync_time: Optional[str] = None,
    configuration: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch invitation links data from Trustpilot.
    Args:
        api_key: Trustpilot API key
        business_unit_id: Business unit ID
        last_sync_time: Last sync timestamp for incremental sync
        configuration: Optional configuration dictionary
    Returns:
        List of invitation link data
    """
    time_range = get_time_range(last_sync_time, configuration)
    endpoint = f"/business-units/{business_unit_id}/invitation-links"

    # Get max records per page from configuration
    max_records = 100
    if configuration:
        try:
            max_records = int(str(configuration.get("max_records_per_page", "100")))
        except (ValueError, TypeError):
            max_records = 100

    params = {
        "perPage": max_records,
        "page": 1,
        "start": time_range["start"],
        "end": time_range["end"],
    }

    all_invitations = []
    page = 1

    while True:
        params["page"] = page
        response = execute_api_request(endpoint, api_key, params, configuration)

        invitation_links = response.get("invitationLinks", [])
        if not invitation_links:
            break

        for invitation_link in invitation_links:
            all_invitations.append(
                {
                    "invitation_id": invitation_link.get("id", ""),
                    "business_unit_id": business_unit_id,
                    "consumer_id": invitation_link.get("consumerId", ""),
                    "consumer_email": invitation_link.get("consumerEmail", ""),
                    "status": invitation_link.get("status", ""),
                    "type": invitation_link.get("type", ""),
                    "created_at": invitation_link.get("createdAt", ""),
                    "updated_at": invitation_link.get("updatedAt", ""),
                    "sent_at": invitation_link.get("sentAt", ""),
                    "responded_at": invitation_link.get("respondedAt", ""),
                    "timestamp": datetime.now().isoformat(),
                }
            )

        # Check if there are more pages
        if len(invitation_links) < max_records:
            break
        page += 1

    return all_invitations


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    return [
        {
            "table": "review",
            "primary_key": ["review_id", "business_unit_id"],
            "columns": {
                "review_id": "STRING",
                "business_unit_id": "STRING",
                "consumer_id": "STRING",
                "consumer_name": "STRING",
                "stars": "INT",
                "title": "STRING",
                "text": "STRING",
                "language": "STRING",
                "created_at": "STRING",
                "updated_at": "STRING",
                "status": "STRING",
                "is_verified": "BOOLEAN",
                "helpful_count": "INT",
                "reply_text": "STRING",
                "reply_created_at": "STRING",
                "timestamp": "STRING",
            },
        },
        {
            "table": "business_unit",
            "primary_key": ["business_unit_id"],
            "columns": {
                "business_unit_id": "STRING",
                "name": "STRING",
                "display_name": "STRING",
                "website_url": "STRING",
                "country_code": "STRING",
                "language": "STRING",
                "number_of_reviews": "INT",
                "trust_score": "FLOAT",
                "stars": "FLOAT",
                "created_at": "STRING",
                "updated_at": "STRING",
                "timestamp": "STRING",
            },
        },
        {
            "table": "category",
            "primary_key": ["category_id"],
            "columns": {
                "category_id": "STRING",
                "name": "STRING",
                "localized_name": "STRING",
                "parent_id": "STRING",
                "level": "INT",
                "created_at": "STRING",
                "updated_at": "STRING",
                "timestamp": "STRING",
            },
        },
        {
            "table": "consumer_review",
            "primary_key": ["consumer_id", "review_id"],
            "columns": {
                "consumer_id": "STRING",
                "review_id": "STRING",
                "business_unit_id": "STRING",
                "business_unit_name": "STRING",
                "stars": "INT",
                "title": "STRING",
                "text": "STRING",
                "language": "STRING",
                "status": "STRING",
                "is_verified": "BOOLEAN",
                "number_of_likes": "INT",
                "created_at": "STRING",
                "updated_at": "STRING",
                "experienced_at": "STRING",
                "review_verification_level": "STRING",
                "counts_towards_trust_score": "BOOLEAN",
                "counts_towards_location_trust_score": "BOOLEAN",
                "company_reply_text": "STRING",
                "company_reply_created_at": "STRING",
                "company_reply_updated_at": "STRING",
                "location_id": "STRING",
                "location_name": "STRING",
                "timestamp": "STRING",
            },
        },
        {
            "table": "invitation_link",
            "primary_key": ["invitation_id", "business_unit_id"],
            "columns": {
                "invitation_id": "STRING",
                "business_unit_id": "STRING",
                "consumer_id": "STRING",
                "consumer_email": "STRING",
                "status": "STRING",
                "type": "STRING",
                "created_at": "STRING",
                "updated_at": "STRING",
                "sent_at": "STRING",
                "responded_at": "STRING",
                "timestamp": "STRING",
            },
        },
    ]


def update(configuration: dict, state: dict):
    """
    Define the update function, which is called by Fivetran during each sync.
    Args:
        configuration: A dictionary containing connection details
        state: A dictionary containing state information from previous runs
    """
    log.info("Starting Trustpilot API connector sync")

    # Validate the configuration
    validate_configuration(configuration=configuration)

    # Extract configuration parameters
    api_key = str(configuration.get("api_key", ""))
    business_unit_id = str(configuration.get("business_unit_id", ""))
    consumer_id = str(configuration.get("consumer_id", ""))

    # Get feature flags from configuration
    enable_consumer_reviews = (
        str(configuration.get("enable_consumer_reviews", "true")).lower() == "true"
    )
    enable_invitation_links = (
        str(configuration.get("enable_invitation_links", "true")).lower() == "true"
    )
    enable_categories = str(configuration.get("enable_categories", "true")).lower() == "true"
    enable_debug_logging = (
        str(configuration.get("enable_debug_logging", "false")).lower() == "true"
    )

    # Get the state variable for the sync
    last_sync_time = state.get("last_sync_time")

    # Log sync type
    if last_sync_time:
        log.info(f"Incremental sync: fetching data since {last_sync_time}")
    else:
        initial_days = str(configuration.get("initial_sync_days", "90"))
        log.info(f"Initial sync: fetching all available data (last {initial_days} days)")

    if enable_debug_logging:
        log.info(
            f"Configuration: consumer_reviews={enable_consumer_reviews}, invitation_links={enable_invitation_links}, categories={enable_categories}"
        )

    try:
        # Fetch business unit data
        log.info("Fetching business unit data...")
        business_data = get_business_data(api_key, business_unit_id, configuration)
        for record in business_data:
            op.upsert(table="business_unit", data=record)

        # Fetch reviews data
        log.info("Fetching reviews data...")
        reviews_data = get_reviews_data(api_key, business_unit_id, last_sync_time, configuration)
        for record in reviews_data:
            op.upsert(table="review", data=record)

        # Fetch categories data (if enabled)
        if enable_categories:
            log.info("Fetching categories data...")
            categories_data = get_categories_data(api_key, configuration)
            for record in categories_data:
                op.upsert(table="category", data=record)
        else:
            log.info("Categories data fetching disabled")

        # Fetch consumer reviews data (if enabled)
        if enable_consumer_reviews:
            log.info("Fetching consumer reviews data...")
            consumers_data = get_consumers_data(
                api_key, consumer_id, last_sync_time, configuration
            )
            for record in consumers_data:
                op.upsert(table="consumer_review", data=record)
        else:
            log.info("Consumer reviews data fetching disabled")

        # Fetch invitation links data (if enabled)
        if enable_invitation_links:
            log.info("Fetching invitation links data...")
            invitations_data = get_invitations_data(
                api_key, business_unit_id, last_sync_time, configuration
            )
            for record in invitations_data:
                op.upsert(table="invitation_link", data=record)
        else:
            log.info("Invitation links data fetching disabled")

        # Update state with the current sync time
        new_state = {"last_sync_time": datetime.now().isoformat()}

        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        op.checkpoint(new_state)

        log.info("Trustpilot API connector sync completed successfully")

    except Exception as e:
        log.severe(f"Failed to sync Trustpilot data: {str(e)}")
        raise RuntimeError(f"Failed to sync data: {str(e)}")


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module
if __name__ == "__main__":
    # Open the configuration.json file and load its contents
    with open("configuration.json", "r") as f:
        configuration = json.load(f)

    # Test the connector locally
    connector.debug(configuration=configuration)
