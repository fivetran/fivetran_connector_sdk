"""Refiner Survey Analytics Connector for Fivetran - syncs NPS surveys, responses, and user data.
This connector extracts survey responses keyed by user ID from the Refiner API and loads them into
your destination for product analytics and user-level joins. Supports incremental syncs based on
updated_at timestamps with automatic pagination and nested data flattening.
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

# For making HTTP API requests (provided by SDK runtime)
import requests

# For handling time operations and timestamps
from datetime import datetime, timezone

# For URL parameter encoding
from urllib.parse import urlencode

# For handling retries with exponential backoff
import time

# Refiner API base URL
__API_BASE_URL = "https://api.refiner.io/v1"

# Maximum number of retries for API requests
__MAX_RETRIES = 3

# Initial retry delay in seconds
__RETRY_DELAY_SECONDS = 2

# Maximum retry delay in seconds
__MAX_RETRY_DELAY_SECONDS = 60

# Page size for API pagination (max 1000 per Refiner API docs, but use smaller size for reliability)
__PAGE_SIZE = 100

# Checkpoint interval for large datasets
__CHECKPOINT_INTERVAL = 1000

# Default start date for initial sync (epoch)
__DEFAULT_START_DATE = "1970-01-01T00:00:00Z"

# Use cursor-based pagination for large datasets (>10k records per Refiner docs)
__USE_CURSOR_PAGINATION = True


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing.
    """
    required_configs = ["api_key"]
    for key in required_configs:
        if key not in configuration or not configuration[key]:
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
        {"table": "surveys", "primary_key": ["uuid"]},
        {"table": "questions", "primary_key": ["survey_uuid", "question_id"]},
        {"table": "responses", "primary_key": ["uuid"]},
        {"table": "answers", "primary_key": ["response_uuid", "question_id"]},
        {"table": "respondents", "primary_key": ["user_id"]},
    ]


def make_api_request(url: str, headers: dict, params: dict = None) -> dict:
    """
    Make an API request with retry logic and exponential backoff.
    Handles 429 rate limiting with proper backoff.
    Args:
        url: The API endpoint URL.
        headers: Request headers including authentication.
        params: Optional query parameters.
    Returns:
        JSON response as a dictionary.
    """
    for attempt in range(__MAX_RETRIES):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            if response.status_code in [401, 403, 404]:
                log.severe(f"Permanent API error {response.status_code}: {e}")
                raise RuntimeError(f"API request failed with status {response.status_code}: {e}")
            if response.status_code == 429:
                if attempt == __MAX_RETRIES - 1:
                    log.severe(f"Rate limit exceeded after {__MAX_RETRIES} attempts")
                    raise RuntimeError("API rate limit exceeded")
                sleep_time = min(__MAX_RETRY_DELAY_SECONDS, __RETRY_DELAY_SECONDS * (2**attempt))
                log.warning(
                    f"Rate limited. Retry {attempt + 1}/{__MAX_RETRIES} after {sleep_time}s"
                )
                time.sleep(sleep_time)
                continue
            if attempt == __MAX_RETRIES - 1:
                log.severe(f"API request failed after {__MAX_RETRIES} attempts: {e}")
                raise RuntimeError(f"API request failed: {e}")
            sleep_time = min(__MAX_RETRY_DELAY_SECONDS, __RETRY_DELAY_SECONDS * (2**attempt))
            log.warning(f"Retry {attempt + 1}/{__MAX_RETRIES} after {sleep_time}s due to: {e}")
            time.sleep(sleep_time)
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            if attempt == __MAX_RETRIES - 1:
                log.severe(f"Network error after {__MAX_RETRIES} attempts: {e}")
                raise RuntimeError(f"Network error: {e}")
            sleep_time = min(__MAX_RETRY_DELAY_SECONDS, __RETRY_DELAY_SECONDS * (2**attempt))
            log.warning(f"Retry {attempt + 1}/{__MAX_RETRIES} after {sleep_time}s due to: {e}")
            time.sleep(sleep_time)


def parse_iso_datetime(date_string: str) -> datetime:
    """
    Parse ISO 8601 datetime string to datetime object.
    Args:
        date_string: ISO 8601 formatted datetime string.
    Returns:
        datetime object in UTC timezone.
    """
    try:
        if date_string.endswith("Z"):
            date_string = date_string[:-1] + "+00:00"
        return datetime.fromisoformat(date_string).astimezone(timezone.utc)
    except Exception as e:
        log.warning(f"Failed to parse datetime '{date_string}': {e}")
        return None


def flatten_dict(data: dict, parent_key: str = "", separator: str = "_") -> dict:
    """
    Flatten nested dictionary into single-level dictionary with underscore-separated keys.
    Args:
        data: Dictionary to flatten.
        parent_key: Parent key for nested recursion.
        separator: Separator for joining keys.
    Returns:
        Flattened dictionary.
    """
    items = []
    for key, value in data.items():
        new_key = f"{parent_key}{separator}{key}" if parent_key else key
        if isinstance(value, dict):
            items.extend(flatten_dict(value, new_key, separator).items())
        elif isinstance(value, list):
            items.append((new_key, json.dumps(value)))
        else:
            items.append((new_key, value))
    return dict(items)


def fetch_surveys(api_key: str, state: dict) -> int:
    """
    Fetch all surveys from Refiner API with cursor-based or page-based pagination.
    Uses cursor pagination for large datasets as recommended by Refiner API docs.
    Args:
        api_key: Refiner API key for authentication.
        state: State dictionary for tracking sync progress.
    Returns:
        Number of surveys synced.
    """
    headers = {"Authorization": f"Bearer {api_key}"}
    page = 1
    page_cursor = None
    total_surveys = 0

    log.info("Starting survey sync with cursor-based pagination")

    while True:
        params = {"page_length": __PAGE_SIZE, "config": "true", "meta": "true"}

        if __USE_CURSOR_PAGINATION and page_cursor:
            params["page_cursor"] = page_cursor
            log.info(f"Fetching surveys with cursor pagination")
        else:
            params["page"] = page
            log.info(f"Fetching surveys page {page}")

        url = f"{__API_BASE_URL}/forms"
        response_data = make_api_request(url, headers, params)

        surveys = response_data.get("items", [])
        if not surveys:
            log.info("No more surveys to process")
            break

        for survey in surveys:
            survey_uuid = survey.get("uuid")
            if not survey_uuid:
                log.warning("Survey missing uuid, skipping")
                continue

            flattened_survey = flatten_dict(survey)

            # The 'upsert' operation is used to insert or update data in the destination table.
            # The first argument is the name of the destination table.
            # The second argument is a dictionary containing the record to be upserted.
            op.upsert(table="surveys", data=flattened_survey)
            total_surveys += 1

            questions_synced = fetch_questions(survey_uuid, survey, api_key)
            log.info(f"Synced survey {survey_uuid} with {questions_synced} questions")

        pagination = response_data.get("pagination", {})
        next_page_cursor = pagination.get("next_page_cursor")

        if __USE_CURSOR_PAGINATION and next_page_cursor:
            page_cursor = next_page_cursor
        elif not __USE_CURSOR_PAGINATION:
            current_page = pagination.get("current_page", page)
            last_page = pagination.get("last_page", page)

            if current_page >= last_page:
                log.info(f"Reached last page of surveys: {last_page}")
                break
            page += 1
        else:
            log.info("No next page cursor, pagination complete")
            break

    log.info(f"Completed survey sync: {total_surveys} surveys")
    return total_surveys


def fetch_questions(survey_uuid: str, survey_data: dict, api_key: str) -> int:
    """
    Extract questions from survey configuration and create child table records.
    Args:
        survey_uuid: Survey UUID (parent key).
        survey_data: Full survey data including config.
        api_key: API key for authentication.
    Returns:
        Number of questions extracted.
    """
    config = survey_data.get("config", {})
    form_elements = config.get("form_elements", [])
    questions_count = 0

    for idx, element in enumerate(form_elements):
        if element.get("type") in ["question", "nps_question", "rating_question"]:
            question_record = {
                "survey_uuid": survey_uuid,
                "question_id": element.get("id", f"q_{idx}"),
                "question_text": element.get("text", ""),
                "question_type": element.get("type", ""),
                "required": element.get("required", False),
                "options": json.dumps(element.get("options", [])),
            }

            # The 'upsert' operation is used to insert or update data in the destination table.
            # The first argument is the name of the destination table.
            # The second argument is a dictionary containing the record to be upserted.
            op.upsert(table="questions", data=question_record)
            questions_count += 1

    return questions_count


def fetch_responses(api_key: str, state: dict, last_sync_time: str) -> int:
    """
    Fetch survey responses incrementally based on last_data_reception_at timestamp.
    Uses cursor-based pagination for large datasets and date_range_start for incremental sync.
    Args:
        api_key: Refiner API key for authentication.
        state: State dictionary for tracking sync progress.
        last_sync_time: Last sync timestamp for incremental sync (ISO 8601 format).
    Returns:
        Number of responses synced.
    """
    headers = {"Authorization": f"Bearer {api_key}"}
    page = 1
    page_cursor = None
    total_responses = 0
    record_count = 0
    latest_timestamp = last_sync_time

    log.info(f"Starting incremental responses sync from {last_sync_time}")

    while True:
        params = {"page_length": __PAGE_SIZE, "date_range_start": last_sync_time}

        if __USE_CURSOR_PAGINATION and page_cursor:
            params["page_cursor"] = page_cursor
            log.info(f"Fetching responses with cursor pagination")
        else:
            params["page"] = page
            log.info(f"Fetching responses page {page}")

        url = f"{__API_BASE_URL}/responses"
        response_data = make_api_request(url, headers, params)

        responses = response_data.get("items", [])
        if not responses:
            log.info("No more responses to process")
            break

        for response in responses:
            response_uuid = response.get("uuid")
            if not response_uuid:
                log.warning("Response missing uuid, skipping")
                continue

            user_id = response.get("contact_uuid") or response.get("user_id")
            form_data = response.get("form", {})
            contact_data = response.get("contact", {})

            response_record = {
                "uuid": response_uuid,
                "survey_uuid": (
                    form_data.get("uuid")
                    if isinstance(form_data, dict)
                    else response.get("form_uuid")
                ),
                "user_id": (
                    user_id or contact_data.get("remote_id")
                    if isinstance(contact_data, dict)
                    else user_id
                ),
                "completed_at": response.get("completed_at"),
                "first_shown_at": response.get("first_shown_at"),
                "last_shown_at": response.get("last_shown_at"),
                "last_data_reception_at": response.get("last_data_reception_at"),
                "created_at": response.get("created_at"),
                "updated_at": response.get("updated_at"),
                "score": response.get("score"),
            }

            # The 'upsert' operation is used to insert or update data in the destination table.
            # The first argument is the name of the destination table.
            # The second argument is a dictionary containing the record to be upserted.
            op.upsert(table="responses", data=response_record)
            total_responses += 1
            record_count += 1

            answers_synced = fetch_answers(response_uuid, response)

            if response_record["user_id"]:
                fetch_respondent(response_record["user_id"], response, api_key)

            current_response_timestamp = response.get("last_data_reception_at") or response.get(
                "updated_at"
            )
            if current_response_timestamp and current_response_timestamp > latest_timestamp:
                latest_timestamp = current_response_timestamp

            if record_count >= __CHECKPOINT_INTERVAL:
                state["last_response_sync"] = latest_timestamp
                # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                # from the correct position in case of next sync or interruptions.
                # Learn more about how and where to checkpoint by reading our best practices documentation
                # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
                op.checkpoint(state)
                record_count = 0
                log.info(
                    f"Checkpointed at {total_responses} responses, latest timestamp: {latest_timestamp}"
                )

        pagination = response_data.get("pagination", {})
        next_page_cursor = pagination.get("next_page_cursor")

        if __USE_CURSOR_PAGINATION and next_page_cursor:
            page_cursor = next_page_cursor
        elif not __USE_CURSOR_PAGINATION:
            current_page = pagination.get("current_page", page)
            last_page = pagination.get("last_page", page)

            if current_page >= last_page:
                log.info(f"Reached last page of responses: {last_page}")
                break
            page += 1
        else:
            log.info("No next page cursor, pagination complete")
            break

    log.info(
        f"Completed responses sync: {total_responses} responses, latest timestamp: {latest_timestamp}"
    )

    # Update state with the latest timestamp from this batch
    if total_responses > 0:
        state["last_response_sync"] = latest_timestamp

    return total_responses


def fetch_answers(response_uuid: str, response_data: dict) -> int:
    """
    Extract answers from response data and create child table records.
    Args:
        response_uuid: Response UUID (parent key).
        response_data: Full response data including answers.
    Returns:
        Number of answers extracted.
    """
    answers = response_data.get("data", {})
    answers_count = 0

    for question_id, answer_value in answers.items():
        answer_record = {
            "response_uuid": response_uuid,
            "question_id": question_id,
            "answer_value": (
                json.dumps(answer_value)
                if isinstance(answer_value, (dict, list))
                else str(answer_value)
            ),
        }

        # The 'upsert' operation is used to insert or update data in the destination table.
        # The first argument is the name of the destination table.
        # The second argument is a dictionary containing the record to be upserted.
        op.upsert(table="answers", data=answer_record)
        answers_count += 1

    return answers_count


def fetch_respondent(user_id: str, response_data: dict, api_key: str):
    """
    Fetch or extract respondent (contact) information from response data.
    Args:
        user_id: User ID for the respondent.
        response_data: Response data that may contain contact info.
        api_key: API key for authentication.
    """
    respondent_record = {
        "user_id": user_id,
        "first_seen_at": response_data.get("created_at"),
        "last_seen_at": response_data.get("updated_at"),
    }

    contact_data = response_data.get("contact", {})
    if contact_data and isinstance(contact_data, dict):
        respondent_record.update(
            {
                "contact_uuid": contact_data.get("uuid"),
                "email": contact_data.get("email"),
                "display_name": contact_data.get("display_name"),
                "first_seen_at": contact_data.get("first_seen_at")
                or respondent_record["first_seen_at"],
                "last_seen_at": contact_data.get("last_seen_at")
                or respondent_record["last_seen_at"],
                "attributes": json.dumps(contact_data.get("attributes", {})),
            }
        )

    # The 'upsert' operation is used to insert or update data in the destination table.
    # The first argument is the name of the destination table.
    # The second argument is a dictionary containing the record to be upserted.
    op.upsert(table="respondents", data=respondent_record)


def fetch_contacts(api_key: str, state: dict) -> int:
    """
    Fetch all contacts from Refiner API with cursor-based pagination.
    Contacts endpoint does not support date filtering, so we sync all contacts each time.
    Args:
        api_key: Refiner API key for authentication.
        state: State dictionary for tracking sync progress.
    Returns:
        Number of contacts synced.
    """
    headers = {"Authorization": f"Bearer {api_key}"}
    page = 1
    page_cursor = None
    total_contacts = 0

    log.info("Starting contacts sync with cursor-based pagination")

    while True:
        params = {"page_length": __PAGE_SIZE}

        if __USE_CURSOR_PAGINATION and page_cursor:
            params["page_cursor"] = page_cursor
            log.info(f"Fetching contacts with cursor pagination")
        else:
            params["page"] = page
            log.info(f"Fetching contacts page {page}")

        url = f"{__API_BASE_URL}/contacts"
        response_data = make_api_request(url, headers, params)

        contacts = response_data.get("items", [])
        if not contacts:
            log.info("No more contacts to process")
            break

        for contact in contacts:
            contact_uuid = contact.get("uuid")
            remote_id = contact.get("remote_id")

            if not contact_uuid:
                log.warning("Contact missing uuid, skipping")
                continue

            contact_record = {
                "user_id": remote_id or contact_uuid,
                "contact_uuid": contact_uuid,
                "remote_id": remote_id,
                "email": contact.get("email"),
                "display_name": contact.get("display_name"),
                "first_seen_at": contact.get("first_seen_at"),
                "last_seen_at": contact.get("last_seen_at"),
                "last_form_submission_at": contact.get("last_form_submission_at"),
                "last_tracking_event_at": contact.get("last_tracking_event_at"),
                "attributes": json.dumps(contact.get("attributes", {})),
                "segments": json.dumps(contact.get("segments", [])),
            }

            # The 'upsert' operation is used to insert or update data in the destination table.
            # The first argument is the name of the destination table.
            # The second argument is a dictionary containing the record to be upserted.
            op.upsert(table="respondents", data=contact_record)
            total_contacts += 1

        pagination = response_data.get("pagination", {})
        next_page_cursor = pagination.get("next_page_cursor")

        if __USE_CURSOR_PAGINATION and next_page_cursor:
            page_cursor = next_page_cursor
        elif not __USE_CURSOR_PAGINATION:
            current_page = pagination.get("current_page", page)
            last_page = pagination.get("last_page", page)

            if current_page >= last_page:
                log.info(f"Reached last page of contacts: {last_page}")
                break
            page += 1
        else:
            log.info("No next page cursor, pagination complete")
            break

    log.info(f"Completed contacts sync: {total_contacts} contacts")
    return total_contacts


def update(configuration: dict, state: dict):
    """
    Define the update function which lets you configure how your connector fetches data.
    See the technical reference documentation for more details on the update function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
        state: a dictionary that holds the state of the connector.
    """
    log.warning("Example: REFINER_SURVEY_ANALYTICS_CONNECTOR")

    validate_configuration(configuration)

    api_key = configuration.get("api_key")
    start_date = configuration.get("start_date", __DEFAULT_START_DATE)

    last_survey_sync = state.get("last_survey_sync", start_date)
    last_response_sync = state.get("last_response_sync", start_date)

    current_sync_time = datetime.now(timezone.utc).isoformat()

    log.info(f"Starting sync from last_response_sync: {last_response_sync}")

    try:
        surveys_synced = fetch_surveys(api_key, state)
        log.info(f"Synced {surveys_synced} surveys")

        contacts_synced = fetch_contacts(api_key, state)
        log.info(f"Synced {contacts_synced} contacts")

        responses_synced = fetch_responses(api_key, state, last_response_sync)
        log.info(f"Synced {responses_synced} responses")

        # Update sync timestamps - note that last_response_sync is already updated in fetch_responses
        state["last_survey_sync"] = current_sync_time
        state["last_contact_sync"] = current_sync_time

        # Only update last_response_sync if no responses were found (use current time as marker)
        if responses_synced == 0:
            state["last_response_sync"] = current_sync_time

        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        op.checkpoint(state)

        log.info(f"Sync completed successfully at {current_sync_time}")

    except Exception as e:
        log.severe(f"Sync failed: {e}")
        raise RuntimeError(f"Sync failed: {str(e)}")


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Test the connector locally
    connector.debug()
