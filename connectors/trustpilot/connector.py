"""
Trustpilot API Connector for Fivetran Connector SDK.

This connector demonstrates how to fetch data from Trustpilot API and upsert it
into destination using the Fivetran Connector SDK.
Supports querying data from Reviews, Businesses, Categories, and Consumer APIs.
See the Technical Reference documentation
(https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
and the Best Practices documentation
(https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

# For reading configuration from a JSON file
import json

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like upsert(), update(), delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# Import required libraries for API interactions
# random: For adding jitter to exponential backoff retry delays
import random

# requests: For making HTTP requests to the Trustpilot API
import requests

# time: For implementing delays between retries and rate limit handling
import time

# datetime, timedelta, timezone: For timestamp management and calculating time ranges for API queries
from datetime import datetime, timedelta, timezone

# typing: For type hints to improve code clarity and enable static type checking
from typing import Dict, Any, Optional

# Constants
__INVALID_LITERAL_ERROR = "invalid literal"
__TRUSTPILOT_API_ENDPOINT = "https://api.trustpilot.com/v1"
__MAX_RECORDS_PER_PAGE = 100
__REQUEST_TIMEOUT_SECONDS = 30
__RETRY_ATTEMPTS = 3
__CHECKPOINT_INTERVAL = 100  # for state checkpointing


def __get_config_int(
    configuration: Optional[Dict[str, Any]],
    key: str,
    default: int,
    min_val: int = None,
    max_val: int = None,
) -> int:
    """
    Extract and validate integer configuration value.
    Args:
        configuration: Configuration dictionary
        key: Configuration key
        default: Default value
        min_val: Minimum allowed value
        max_val: Maximum allowed value
    Returns:
        Validated integer value
    """
    if not configuration:
        return default

    try:
        value = int(str(configuration.get(key, str(default))))
        if min_val is not None and value < min_val:
            return default
        if max_val is not None and value > max_val:
            return default
        return value
    except (ValueError, TypeError):
        return default


def __validate_required_fields(configuration: dict) -> None:
    """
    Validate required configuration fields.
    Args:
        configuration: Configuration dictionary
    Raises:
        ValueError: If any required field is missing or empty
    """
    required_fields = {
        "api_key": "API key cannot be empty",
        "business_unit_id": "Business unit ID cannot be empty",
    }

    for field, error_msg in required_fields.items():
        if field not in configuration or not str(configuration.get(field, "")).strip():
            raise ValueError(error_msg)


def __validate_consumer_fields(configuration: dict) -> None:
    """
    Validate consumer-specific configuration fields when consumer reviews are enabled.
    Args:
        configuration: Configuration dictionary
    Raises:
        ValueError: If consumer fields are missing when consumer reviews are enabled
    """
    enable_consumer_reviews = (
        str(configuration.get("enable_consumer_reviews", "true")).lower() == "true"
    )

    if enable_consumer_reviews:
        consumer_id = configuration.get("consumer_id", "")
        if not str(consumer_id).strip():
            raise ValueError("Consumer ID is required when consumer reviews are enabled")


def __validate_numeric_ranges(configuration: dict) -> None:
    """
    Validate numeric configuration parameters.
    Args:
        configuration: Configuration dictionary
    Raises:
        ValueError: If numeric values are out of range
    """
    numeric_validations = [
        (
            "initial_sync_days",
            90,
            1,
            365,
            "Initial sync days must be between 1 and 365",
        )
    ]

    for field, default, min_val, max_val, error_msg in numeric_validations:
        try:
            value = int(str(configuration.get(field, str(default))))
            if value < min_val or value > max_val:
                raise ValueError(error_msg)
        except ValueError as e:
            if __INVALID_LITERAL_ERROR in str(e):
                raise ValueError(f"{field} must be a valid number")
            raise


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing.
    """
    __validate_required_fields(configuration)
    __validate_consumer_fields(configuration)
    __validate_numeric_ranges(configuration)


def __calculate_wait_time(
    attempt: int,
    response_headers: Dict[str, Any],
    base_delay: int = 1,
    max_delay: int = 60,
) -> float:
    """
    Calculate wait time for rate limiting and retries.
    Args:
        attempt: Current attempt number
        response_headers: Response headers dictionary
        base_delay: Base delay in seconds
        max_delay: Maximum delay in seconds
    Returns:
        Wait time in seconds with jitter
    """
    retry_after = response_headers.get("Retry-After")
    if retry_after:
        try:
            return int(retry_after)
        except ValueError:
            pass

    wait_time = min(base_delay * (2**attempt), max_delay)
    jitter = random.uniform(0, min(wait_time * 0.1, 5))
    return wait_time + jitter


def __handle_rate_limit(attempt: int, response: requests.Response) -> None:
    """
    Handle HTTP 429 rate limiting.
    Args:
        attempt: Current attempt number
        response: HTTP response object
    """
    wait_time = __calculate_wait_time(attempt, response.headers)
    log.info(f"Rate limited (HTTP 429) on attempt {attempt + 1}, waiting {wait_time:.2f} seconds")
    time.sleep(wait_time)


def __handle_request_error(
    attempt: int, retry_attempts: int, error: Exception, endpoint: str
) -> None:
    """
    Handle request errors with retry logic.
    Args:
        attempt: Current attempt number
        retry_attempts: Total retry attempts
        error: Exception that occurred
        endpoint: API endpoint being called
    Raises:
        RuntimeError: If this is the final attempt
    """
    if attempt == retry_attempts - 1:
        log.severe(
            f"Failed to execute API request to {endpoint} after {retry_attempts} "
            f"attempts: {str(error)}"
        )
        raise RuntimeError(f"API request failed after {retry_attempts} attempts: {str(error)}")

    wait_time = __calculate_wait_time(attempt, {})
    log.info(f"Attempt {attempt + 1} failed, retrying in {wait_time:.2f} seconds: {str(error)}")
    time.sleep(wait_time)


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
    url = f"{__TRUSTPILOT_API_ENDPOINT}{endpoint}"
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

    for attempt in range(__RETRY_ATTEMPTS):
        try:
            response = requests.get(
                url, headers=headers, params=params, timeout=__REQUEST_TIMEOUT_SECONDS
            )

            if response.status_code == 429:
                __handle_rate_limit(attempt, response)
                continue

            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            __handle_request_error(attempt, __RETRY_ATTEMPTS, e, endpoint)
            continue

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
        A dictionary containing the time range for the API query with the following keys:
            start: ISO 8601 UTC datetime string representing the start of the time range.
            end: ISO 8601 UTC datetime string representing the end of the time range.
    """
    end_time = datetime.now(timezone.utc).isoformat()

    if last_sync_time:
        start_time = last_sync_time
    else:
        initial_sync_days = __get_config_int(configuration, "initial_sync_days", 90)
        start_time = (datetime.now(timezone.utc) - timedelta(days=initial_sync_days)).isoformat()

    return {"start": start_time, "end": end_time}


def __map_review_data(review: Dict[str, Any], business_unit_id: str) -> Dict[str, Any]:
    """
    Map API response fields to database schema for reviews.
    Args:
        review: Review data from API
        business_unit_id: Business unit ID
    Returns:
        Mapped review data
    """
    return {
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
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def get_reviews_data(
    api_key: str,
    business_unit_id: str,
    last_sync_time: Optional[str] = None,
    configuration: Optional[Dict[str, Any]] = None,
    state: Optional[Dict[str, Any]] = None,
):
    """
    Fetch reviews data from Trustpilot using streaming approach.
    Args:
        api_key: Trustpilot API key
        business_unit_id: Business unit ID
        last_sync_time: Last sync timestamp for incremental sync
        configuration: Optional configuration dictionary
        state: Optional state dictionary to track pagination
    Yields:
        Individual review records
    """
    time_range = get_time_range(last_sync_time, configuration)
    endpoint = f"/business-units/{business_unit_id}/reviews"

    params = {
        "perPage": __MAX_RECORDS_PER_PAGE,
        "page": 1,
        "start": time_range["start"],
        "end": time_range["end"],
    }

    # Resume from saved page if available
    if state and "reviews_page" in state:
        params["page"] = state["reviews_page"]

    page = params["page"]
    while True:
        params["page"] = page
        response = execute_api_request(endpoint, api_key, params, configuration)

        reviews = response.get("reviews", [])
        if not reviews:
            break

        for review in reviews:
            yield __map_review_data(review, business_unit_id)

        if len(reviews) < __MAX_RECORDS_PER_PAGE:
            break
        page += 1
        # Update state with current page for pagination resumption
        if state is not None:
            state["reviews_page"] = page


def __map_business_data(response: Dict[str, Any], business_unit_id: str) -> Dict[str, Any]:
    """
    Map API response fields to database schema for business unit.
    Args:
        response: Business unit data from API
        business_unit_id: Business unit ID
    Returns:
        Mapped business unit data
    """
    return {
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
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def get_business_data(
    api_key: str, business_unit_id: str, configuration: Optional[Dict[str, Any]] = None
):
    """
    Fetch business unit data from Trustpilot.
    Args:
        api_key: Trustpilot API key
        business_unit_id: Business unit ID
        configuration: Optional configuration dictionary
    Yields:
        Business unit record
    """
    endpoint = f"/business-units/{business_unit_id}"
    response = execute_api_request(endpoint, api_key, configuration=configuration)

    if response:
        yield __map_business_data(response, business_unit_id)


def __map_category_data(category: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map API response fields to database schema for categories.
    Args:
        category: Category data from API
    Returns:
        Mapped category data
    """
    return {
        "category_id": category.get("id", ""),
        "name": category.get("name", ""),
        "localized_name": category.get("localizedName", ""),
        "parent_id": category.get("parentId", ""),
        "level": category.get("level", 0),
        "created_at": category.get("createdAt", ""),
        "updated_at": category.get("updatedAt", ""),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def get_categories_data(
    api_key: str,
    configuration: Optional[Dict[str, Any]] = None,
    state: Optional[Dict[str, Any]] = None,
):
    """
    Fetch categories data from Trustpilot using streaming approach.
    Args:
        api_key: Trustpilot API key
        configuration: Optional configuration dictionary
        state: Optional state dictionary to track pagination
    Yields:
        Individual category records
    """
    endpoint = "/categories"

    params = {
        "perPage": __MAX_RECORDS_PER_PAGE,
        "page": 1,
    }

    # Resume from saved page if available
    if state and "categories_page" in state:
        params["page"] = state["categories_page"]

    page = params["page"]
    while True:
        params["page"] = page
        response = execute_api_request(endpoint, api_key, params, configuration)

        categories = response.get("categories", [])
        if not categories:
            break

        for category in categories:
            yield __map_category_data(category)

        if len(categories) < __MAX_RECORDS_PER_PAGE:
            break
        page += 1
        # Update state with current page for pagination resumption
        if state is not None:
            state["categories_page"] = page


def __map_consumer_review_data(review: Dict[str, Any], consumer_id: str) -> Dict[str, Any]:
    """
    Map API response fields to database schema for consumer reviews.
    Args:
        review: Consumer review data from API
        consumer_id: Consumer ID
    Returns:
        Mapped consumer review data
    """
    return {
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
        "company_reply_created_at": review.get("companyReply", {}).get("createdAt", ""),
        "company_reply_updated_at": review.get("companyReply", {}).get("updatedAt", ""),
        "location_id": review.get("location", {}).get("id", ""),
        "location_name": review.get("location", {}).get("name", ""),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def get_consumers_data(
    api_key: str,
    consumer_id: str,
    last_sync_time: Optional[str] = None,
    configuration: Optional[Dict[str, Any]] = None,
    state: Optional[Dict[str, Any]] = None,
):
    """
    Fetch consumer reviews data from Trustpilot Consumer API.
    Args:
        api_key: Trustpilot API key
        consumer_id: Consumer ID to fetch reviews for
        last_sync_time: Last sync timestamp for incremental sync
        configuration: Optional configuration dictionary
        state: Optional state dictionary to track pagination
    Yields:
        Individual consumer review records
    """
    endpoint = f"/consumers/{consumer_id}/reviews"
    time_range = get_time_range(last_sync_time, configuration)

    params = {
        "perPage": __MAX_RECORDS_PER_PAGE,
        "page": 1,
        "orderBy": "createdat.desc",
        "start": time_range["start"],
        "end": time_range["end"],
    }

    # Resume from saved page if available
    if state and "consumer_reviews_page" in state:
        params["page"] = state["consumer_reviews_page"]

    page = params["page"]
    while True:
        params["page"] = page
        response = execute_api_request(endpoint, api_key, params, configuration)

        reviews = response.get("reviews", [])
        if not reviews:
            break

        for review in reviews:
            yield __map_consumer_review_data(review, consumer_id)

        if len(reviews) < __MAX_RECORDS_PER_PAGE:
            break
        page += 1
        # Update state with current page for pagination resumption
        if state is not None:
            state["consumer_reviews_page"] = page


def __map_invitation_data(
    invitation_link: Dict[str, Any], business_unit_id: str
) -> Dict[str, Any]:
    """
    Map API response fields to database schema for invitation links.
    Args:
        invitation_link: Invitation link data from API
        business_unit_id: Business unit ID
    Returns:
        Mapped invitation link data
    """
    return {
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
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def get_invitations_data(
    api_key: str,
    business_unit_id: str,
    last_sync_time: Optional[str] = None,
    configuration: Optional[Dict[str, Any]] = None,
    state: Optional[Dict[str, Any]] = None,
):
    """
    Fetch invitation links data from Trustpilot.
    Args:
        api_key: Trustpilot API key
        business_unit_id: Business unit ID
        last_sync_time: Last sync timestamp for incremental sync
        configuration: Optional configuration dictionary
        state: Optional state dictionary to track pagination
    Yields:
        Individual invitation link records
    """
    time_range = get_time_range(last_sync_time, configuration)
    endpoint = f"/business-units/{business_unit_id}/invitation-links"

    params = {
        "perPage": __MAX_RECORDS_PER_PAGE,
        "page": 1,
        "start": time_range["start"],
        "end": time_range["end"],
    }

    # Resume from saved page if available
    if state and "invitations_page" in state:
        params["page"] = state["invitations_page"]

    page = params["page"]
    while True:
        params["page"] = page
        response = execute_api_request(endpoint, api_key, params, configuration)

        invitation_links = response.get("invitationLinks", [])
        if not invitation_links:
            break

        for invitation_link in invitation_links:
            yield __map_invitation_data(invitation_link, business_unit_id)

        if len(invitation_links) < __MAX_RECORDS_PER_PAGE:
            break
        page += 1
        # Update state with current page for pagination resumption
        if state is not None:
            state["invitations_page"] = page


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    return [
        {"table": "review", "primary_key": ["review_id", "business_unit_id"]},
        {"table": "business_unit", "primary_key": ["business_unit_id"]},
        {"table": "category", "primary_key": ["category_id"]},
        {"table": "consumer_review", "primary_key": ["consumer_id", "review_id"]},
        {
            "table": "invitation_link",
            "primary_key": ["invitation_id", "business_unit_id"],
        },
    ]


def _extract_configuration_params(configuration: dict) -> Dict[str, str]:
    """Extract and validate configuration parameters."""
    return {
        "api_key": str(configuration.get("api_key", "")),
        "business_unit_id": str(configuration.get("business_unit_id", "")),
        "consumer_id": (
            str(configuration.get("consumer_id", "")) if configuration.get("consumer_id") else ""
        ),
    }


def _extract_feature_flags(configuration: dict) -> Dict[str, bool]:
    """Extract feature flags from configuration."""
    return {
        "enable_consumer_reviews": (
            str(configuration.get("enable_consumer_reviews", "true")).lower() == "true"
        ),
        "enable_invitation_links": (
            str(configuration.get("enable_invitation_links", "true")).lower() == "true"
        ),
        "enable_categories": (
            str(configuration.get("enable_categories", "true")).lower() == "true"
        ),
    }


def _log_sync_info(
    last_sync_time: Optional[str], configuration: dict, feature_flags: Dict[str, bool]
):
    """Log sync information and configuration details."""
    if last_sync_time:
        log.info(f"Incremental sync: fetching data since {last_sync_time}")
    else:
        initial_days = str(configuration.get("initial_sync_days", "90"))
        log.info(f"Initial sync: fetching all available data (last {initial_days} days)")


def _sync_business_data(api_key: str, business_unit_id: str, configuration: dict, state: dict):
    """Sync business unit data."""
    log.info("Fetching business unit data...")
    for record in get_business_data(api_key, business_unit_id, configuration):
        # The 'upsert' operation is used to insert or update data in the destination table.
        # The first argument is the name of the destination table.
        # The second argument is a dictionary containing the record to be upserted.
        op.upsert(table="business_unit", data=record)
        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        op.checkpoint(state)


def _sync_reviews_data(
    api_key: str,
    business_unit_id: str,
    last_sync_time: Optional[str],
    configuration: dict,
    state: dict,
):
    """Sync reviews data."""
    log.info("Fetching reviews data...")
    record_count = 0
    for record in get_reviews_data(
        api_key, business_unit_id, last_sync_time, configuration, state
    ):
        # The 'upsert' operation is used to insert or update data in the destination table.
        # The first argument is the name of the destination table.
        # The second argument is a dictionary containing the record to be upserted.
        op.upsert(table="review", data=record)
        record_count += 1
        # Checkpoint after processing a batch of records to optimize checkpoint frequency
        if record_count % __CHECKPOINT_INTERVAL == 0:
            # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
            # from the correct position in case of next sync or interruptions.
            # Learn more about how and where to checkpoint by reading our best practices documentation
            # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
            op.checkpoint(state)
    # Checkpoint after all records are processed to ensure final batch is persisted
    if record_count > 0:
        op.checkpoint(state)


def _sync_categories_data(api_key: str, configuration: dict, enable_categories: bool, state: dict):
    """Sync categories data if enabled."""
    if enable_categories:
        log.info("Fetching categories data...")
        record_count = 0
        for record in get_categories_data(api_key, configuration, state):
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The first argument is the name of the destination table.
            # The second argument is a dictionary containing the record to be upserted.
            op.upsert(table="category", data=record)
            record_count += 1
            # Checkpoint after processing a batch of records to optimize checkpoint frequency
            if record_count % __CHECKPOINT_INTERVAL == 0:
                # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                # from the correct position in case of next sync or interruptions.
                # Learn more about how and where to checkpoint by reading our best practices documentation
                # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
                op.checkpoint(state)
        # Checkpoint after all records are processed to ensure final batch is persisted
        if record_count > 0:
            op.checkpoint(state)
    else:
        log.info("Categories data fetching disabled")


def _sync_consumer_reviews_data(
    api_key: str,
    consumer_id: str,
    last_sync_time: Optional[str],
    configuration: dict,
    enable_consumer_reviews: bool,
    state: dict,
):
    """Sync consumer reviews data if enabled."""
    if enable_consumer_reviews and consumer_id:
        log.info("Fetching consumer reviews data...")
        __CHECKPOINT_INTERVAL = 100
        record_count = 0
        for record in get_consumers_data(
            api_key, consumer_id, last_sync_time, configuration, state
        ):
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The first argument is the name of the destination table.
            # The second argument is a dictionary containing the record to be upserted.
            op.upsert(table="consumer_review", data=record)
            record_count += 1
            # Checkpoint after processing a batch of records to optimize checkpoint frequency
            if record_count % __CHECKPOINT_INTERVAL == 0:
                # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                # from the correct position in case of next sync or interruptions.
                # Learn more about how and where to checkpoint by reading our best practices documentation
                # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
                op.checkpoint(state)
        # Checkpoint after all records are processed to ensure final batch is persisted
        if record_count > 0:
            op.checkpoint(state)
    elif enable_consumer_reviews and not consumer_id:
        log.info("Consumer reviews data fetching disabled - no consumer_id provided")
    else:
        log.info("Consumer reviews data fetching disabled")


def _sync_invitations_data(
    api_key: str,
    business_unit_id: str,
    last_sync_time: Optional[str],
    configuration: dict,
    enable_invitation_links: bool,
    state: dict,
):
    """Sync invitation links data if enabled."""
    if enable_invitation_links:
        log.info("Fetching invitation links data...")
        record_count = 0
        for record in get_invitations_data(
            api_key, business_unit_id, last_sync_time, configuration, state
        ):
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The first argument is the name of the destination table.
            # The second argument is a dictionary containing the record to be upserted.
            op.upsert(table="invitation_link", data=record)
            record_count += 1
            # Checkpoint after processing a batch of records to optimize checkpoint frequency
            if record_count % __CHECKPOINT_INTERVAL == 0:
                # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                # from the correct position in case of next sync or interruptions.
                # Learn more about how and where to checkpoint by reading our best practices documentation
                # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
                op.checkpoint(state)
        # Checkpoint after all records are processed to ensure final batch is persisted
        if record_count > 0:
            op.checkpoint(state)
    else:
        log.info("Invitation links data fetching disabled")


def update(configuration: dict, state: dict):
    """
    Define the update function which lets you configure how your connector fetches data.
    See the technical reference documentation for more details on the update function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
        state: a dictionary that holds the state of the connector.
    """
    log.warning("Example: Connectors : Trustpilot")

    # Validate the configuration
    validate_configuration(configuration=configuration)

    # Extract configuration parameters and feature flags
    config_params = _extract_configuration_params(configuration)
    feature_flags = _extract_feature_flags(configuration)
    last_sync_time = state.get("last_sync_time")

    # Log sync information
    _log_sync_info(last_sync_time, configuration, feature_flags)

    # Sync all data sources
    _sync_business_data(
        config_params["api_key"], config_params["business_unit_id"], configuration, state
    )

    _sync_reviews_data(
        config_params["api_key"],
        config_params["business_unit_id"],
        last_sync_time,
        configuration,
        state,
    )

    _sync_categories_data(
        config_params["api_key"], configuration, feature_flags["enable_categories"], state
    )

    _sync_consumer_reviews_data(
        config_params["api_key"],
        config_params["consumer_id"],
        last_sync_time,
        configuration,
        feature_flags["enable_consumer_reviews"],
        state,
    )

    _sync_invitations_data(
        config_params["api_key"],
        config_params["business_unit_id"],
        last_sync_time,
        configuration,
        feature_flags["enable_invitation_links"],
        state,
    )

    # Update state with the current sync time for next incremental sync
    state["last_sync_time"] = datetime.now(timezone.utc).isoformat()
    # Clear pagination page markers to reset to page 1 for next sync
    state.pop("reviews_page", None)
    state.pop("consumer_reviews_page", None)
    state.pop("invitations_page", None)
    state.pop("categories_page", None)
    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
    op.checkpoint(state)

    log.info("Trustpilot API connector sync completed successfully")


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly
# from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not
# called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents
    with open("configuration.json", "r") as f:
        configuration = json.load(f)

    # Test the connector locally
    connector.debug(configuration=configuration)
