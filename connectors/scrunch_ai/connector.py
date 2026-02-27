"""
This example shows how to pull AI/ML response and aggregate data from the
Scrunch API and load it into a destination using the Fivetran Connector SDK.

Scrunch is an AI/ML platform that provides responses, citations, and
performance metrics across prompts, personas, and platforms.
This connector demonstrates:
- Paginated retrieval of individual responses
- Aggregated queries for overall performance, competitor performance,
  and daily citations
- Flattening/serialization of nested list fields for destination compatibility
- State check-pointing for incremental syncs

Refer to Scrunch's API for more information (https://api.scrunchai.com)
See the Technical Reference documentation
(https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
and the Best Practices documentation
(https://fivetran.com/docs/connectors/connector-sdk/best-practices)
for details.
"""

# For reading configuration from a JSON file
import json

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

import time  # Rate limiting, retries, or backoff (if needed)
from datetime import datetime  # Timestamps and date utilities

import requests  # HTTP requests to Scrunch API
from dateutil.relativedelta import relativedelta  # Date arithmetic utilities

__API_BASE = "https://api.scrunchai.com/v1"
__LIST_JOINER = " | "  # Delimiter used to collapse list fields into strings
__MAX_RETRIES = 5
__INITIAL_BACKOFF = 2  # seconds
__CHECKPOINT_INTERVAL = 1000


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required
    parameters.
    This function is called at the start of the update method to ensure that
    the connector has all necessary configuration values.
    Args:
        configuration: a dictionary that holds the configuration settings
                       for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing.
    """

    # Define required configuration keys
    required_configs = ["api_token", "brand_id"]

    # Check for missing keys or empty values
    for key in required_configs:
        value = configuration.get(key)
        if not value:
            raise ValueError(f"Missing required configuration value: {key}")


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers
    See the technical reference documentation for more details
    on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema

    Args:
      configuration: a dictionary that holds the configuration settings for the connector.
    """
    return [
        {
            "table": "response",  # Destination table for responses
            "primary_key": ["id"],
        },
        {
            "table": "overall_scrunch_performance",  # Performance table
            # No primary key specified; Fivetran will create _fivetran_id
        },
        {
            "table": "competitor_performance",  # Aggregated competitor metrics
            # No primary key specified; Fivetran will create _fivetran_id
        },
    ]


def get_responses(start_date, end_date, offset, limit, token, brand_id):
    """
    Retrieve paginated response records from Scrunch.

    Args:
        start_date (str): Start date in 'YYYY-MM-DD' format.
        end_date   (str): End date in 'YYYY-MM-DD' format.
        offset     (int): Offset for pagination.
        token     (str): Bearer token for Scrunch API.

    Returns:
        dict: Response JSON containing "total" and "items" keys.
    """
    resp = requests.get(
        f"{__API_BASE}/{brand_id}/responses",
        headers={"Authorization": f"Bearer {token}"},
        params={"start_date": start_date, "end_date": end_date, "offset": offset, "limit": limit},
    )

    resp.raise_for_status()
    data = resp.json()
    return data


def flatten_response(rec: dict) -> dict:
    """
    Transform a raw API response record into a flattened dictionary.

     - Preserves key scalar fields (id, created_at, prompt_id, etc.)
       using direct access or safe .get().
     - Converts list fields (tags, key_topics, competitors_present) into a
       single string joined by LIST_JOINER, or None if empty.
     - Serializes `citations` (which may be a list of dicts) into a JSON
       string or None if missing.
     - Ensures downstream consumers (e.g., database upserts)
       can rely on a flat schema of simple scalars
       and strings instead of nested lists/objects.
       Necessary because Fivetran does not take in lists as datatypes
    Returns:
      Dictionary of records
    """
    return {
        "id": rec["id"],
        "created_at": rec["created_at"],
        "prompt_id": rec.get("prompt_id"),
        "prompt": rec.get("prompt"),
        "persona_id": rec.get("persona_id"),
        "persona_name": rec.get("persona_name"),
        "country": rec.get("country"),
        "stage": rec.get("stage"),
        "branded": rec.get("branded"),
        "platform": rec.get("platform"),
        "brand_present": rec.get("brand_present"),
        "brand_sentiment": rec.get("brand_sentiment"),
        "brand_position": rec.get("brand_position"),
        "response_text": rec.get("response_text"),
        # lists → scalars
        "tags": (__LIST_JOINER.join(rec.get("tags", [])) if rec.get("tags") else None),
        "key_topics": (
            __LIST_JOINER.join(rec.get("key_topics", [])) if rec.get("key_topics") else None
        ),
        "competitors_present": (
            __LIST_JOINER.join(rec.get("competitors_present", []))
            if rec.get("competitors_present")
            else None
        ),
        "citations_json": (
            json.dumps(rec.get("citations", []), ensure_ascii=False)
            if rec.get("citations") is not None
            else None
        ),
    }


def _fetch_page_with_retries(start_date, end_date, offset, limit, token, brand_id):
    """
    Fetches a single page of responses from the API with exponential backoff retries.
    Args:
        start_date (str): Start date in 'YYYY-MM-DD' format (passed through).
        end_date   (str): End date in 'YYYY-MM-DD' format (used in checkpoint).
        offset     (int): Offset to mark location in API call
        token      (str): Bearer token for Scrunch API.
        brand_id   (str): Brand identifier for the Scrunch API endpoint.
    """
    for attempt in range(__MAX_RETRIES):
        try:
            log.info(
                f"Fetching responses (offset={offset}, attempt "
                f"{attempt + 1}/{__MAX_RETRIES})..."
            )
            response = get_responses(start_date, end_date, offset, limit, token, brand_id)
            return response  # Success, return response

        except requests.exceptions.RequestException as e:
            log.warning(f"Error fetching responses on attempt {attempt + 1}: {e}")
            if attempt < __MAX_RETRIES - 1:
                wait_time = __INITIAL_BACKOFF * (2**attempt)
                log.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                # Max retries reached, raise the last exception
                log.severe("Max retries reached while fetching responses. Failing")
                raise
    return None


def get_all_responses(start_date, end_date, token, brand_id):
    """
    Iterate through all paginated responses and upsert into 'response' table.
    Uses get_responses() for pagination and flatten_response() to
    normalize each item before writing to the destination.
    Includes exponential backoff retries
    for transient network or HTTP errors. Sets a checkpoint to enable
    incremental syncs based on end_date.

    Args:
        start_date (str): Start date in 'YYYY-MM-DD' format (passed through).
        end_date   (str): End date in 'YYYY-MM-DD' format (used in checkpoint).
        token      (str): Bearer token for Scrunch API.
        brand_id   (str): Brand identifier for the Scrunch API endpoint.
    """
    offset = 0
    limit = 10000
    total = None  # unknown initially
    processed = 0

    while True:
        # 1. Fetch the page using the helper function
        try:
            response = _fetch_page_with_retries(
                start_date, end_date, offset, limit, token, brand_id
            )
        except Exception:
            # The helper function handles retries and raises if all fail
            raise  # Re-raise the exception from the helper

        # 2. Process the response
        if total is None:
            # First successful call, set the actual total
            total = response.get("total")

        items = response.get("items", [])
        log.info(f"Fetched {len(items)} responses (offset={offset}/{total})")

        # 3. Upsert items
        for item in items:
            flat = flatten_response(item)
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The first argument is the name of the destination table.
            # The second argument is a dictionary containing the record to be upserted.
            op.upsert(table="response", data=flat)

        # Track total processed
        processed += len(items)

        # Checkpoint
        if processed > 0 and processed % __CHECKPOINT_INTERVAL == 0:
            # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
            # from the correct position in case of next sync or interruptions.
            # Learn more about how and where to checkpoint by reading our best practices documentation
            # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
            op.checkpoint(state={"response_start_date": end_date})
            log.info(f"Checkpointed after {processed} records.")

        # 4. Prepare for next page
        offset += limit

        # ---- loop exit condition ----
        if total is not None and offset >= total:
            break

    log.info("All responses fetched successfully.")


def get_all_performances(start_date, end_date, token, brand_id, kind: str = "scrunch"):
    """
    Fetch performance aggregates from Scrunch and upsert into the appropriate table.
    Uses paginated API calls and checkpoints progress for large datasets.

    Args:
        start_date (str): Start date (YYYY-MM-DD).
        end_date   (str): End date   (YYYY-MM-DD).
        token      (str): Bearer token for Scrunch API.
        brand_id   (str): Brand ID specified in configuration.
        kind       (str): "scrunch" for overall performance,
                          "competitor" for competitor performance.
    """
    if kind not in ("scrunch", "competitor"):
        raise ValueError("kind must be either 'scrunch' or 'competitor'")

    if kind == "scrunch":
        table_name = "overall_scrunch_performance"
    else:
        table_name = "competitor_performance"

    offset = 0
    limit = 50000
    processed = 0

    while True:
        # 1. Fetch the page using the helper function
        try:
            items = _fetch_performances_with_retries(
                start_date, end_date, offset, token, brand_id, kind
            )
        except Exception:
            # The helper function handles retries and raises if all fail
            raise  # Re-raise the exception from the helper

        if items is None or len(items) == 0:
            log.info(f"No more {kind} records found. Finishing.")
            break

        # 2. Process the response
        # Since 'items' is the list itself, we check its length
        current_batch_size = len(items)
        log.info(f"Fetched {current_batch_size} {kind} rows (offset={offset})")

        # 3. Upsert items
        for rec in items:
            # The 'upsert' operation is used to insert or update data in the
            # destination table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data
            # - The second argument is a dictionary containing
            #   the data to be upserted
            op.upsert(table=table_name, data=rec)

        # Track total processed
        processed += len(items)

        # Checkpoint
        if processed > 0 and processed % __CHECKPOINT_INTERVAL == 0:
            # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
            # from the correct position in case of next sync or interruptions.
            # Learn more about how and where to checkpoint by reading our best practices documentation
            # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
            op.checkpoint(state={"response_start_date": end_date})
            log.info(f"Checkpointed after {processed} {kind} records.")

        # 4. Prepare for next page
        offset += limit

    log.info(f"All {kind} performances fetched successfully.")


def get_performances(start_date, end_date, offset, token, brand_id, kind: str):
    """
    Retrieve performance aggregates (overall or competitor) from Scrunch
    with a single HTTP request (no retries).

    Args:
        start_date (str): Start date (YYYY-MM-DD).
        end_date   (str): End date   (YYYY-MM-DD).
        token      (str): Bearer token for Scrunch API.
        brand_id   (str): Brand ID specified in configuration.
        kind       (str): "scrunch" for overall performance,
                          "competitor" for competitor performance.

    Returns:
        list[dict]: List of performance records.
    """
    if kind not in ("scrunch", "competitor"):
        raise ValueError("kind must be either 'scrunch' or 'competitor'")

    # Construct base URL dynamically using brand_id
    base_url = f"{__API_BASE}/{brand_id}/query"
    headers = {"Authorization": f"Bearer {token}"}

    if kind == "scrunch":
        # Overall Scrunch performance configuration
        fields = [
            "date_week",
            "date_month",
            "date_year",
            "date",
            "prompt_id",
            "prompt",
            "persona_id",
            "persona_name",
            "ai_platform",
            "branded",
            "responses",
            "brand_presence_percentage",
            "brand_position_score",
            "brand_sentiment_score",
            "competitor_presence_percentage",
        ]
        params = {
            "start_date": start_date,
            "end_date": end_date,
            "offset": offset,
            "fields": ",".join(fields),
        }
    else:
        # Competitor performance configuration
        dims = [
            "date",
            "date_month",
            "date_week",
            "date_year",
            "prompt_id",
            "prompt",
            "ai_platform",
            "competitor_id",
            "competitor_name",
        ]
        metrics = [
            "responses",
            "brand_presence_percentage",
            "competitor_presence_percentage",
        ]
        fields = dims + metrics
        params = {
            "start_date": start_date,
            "end_date": end_date,
            "offset": offset,
            "fields": ",".join(fields),
            "group_by": ",".join(dims),
        }

    resp = requests.get(base_url, headers=headers, params=params)
    # This will raise an HTTPError for bad responses (4xx or 5xx)
    resp.raise_for_status()
    return resp.json()  # list[dict]


def _fetch_performances_with_retries(start_date, end_date, offset, token, brand_id, kind: str):
    """
    Wrapper around get_performances that adds exponential backoff retries.

    Args:
        start_date (str): Start date (YYYY-MM-DD).
        end_date   (str): End date   (YYYY-MM-DD).
        token      (str): Bearer token for Scrunch API.
        brand_id   (str): Brand ID specified in configuration.
        kind       (str): "scrunch" for overall performance,
                          "competitor" for competitor performance.

    Returns:
        list[dict]: List of performance records.
    """
    if kind not in ("scrunch", "competitor"):
        raise ValueError("kind must be either 'scrunch' or 'competitor'")

    if kind == "scrunch":
        log_prefix = "overall Scrunch performance"
        error_suffix = "Failed to fetch data after all retries."
    else:
        log_prefix = "competitor performance"
        error_suffix = "Failed to fetch competitor performance after all retries."

    data = None
    for attempt in range(__MAX_RETRIES):
        try:
            log.info(f"Fetching {log_prefix} (attempt {attempt + 1}/{__MAX_RETRIES})...")
            data = get_performances(start_date, end_date, offset, token, brand_id, kind)
            return data  # success
        except requests.exceptions.RequestException as e:
            log.warning(f"Error on attempt {attempt + 1}: {e}")
            if attempt < __MAX_RETRIES - 1:
                wait_time = __INITIAL_BACKOFF * (2**attempt)
                log.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                log.severe(f"Max retries reached for {log_prefix}.")
                raise

    # Extremely defensive; normally we either returned data or raised above
    if data is None:
        raise ConnectionError(error_suffix)
    return data


def update(configuration: dict, state: dict):
    """
    Define the update function, which is a required function, and is called by
    Fivetran during each sync.
    See the technical reference documentation for more details on the update
    function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update

    Args:
        configuration: A dictionary containing connection details.
        state: A dictionary containing state information from previous runs.
               The state dictionary is empty for the first sync or for any
               full re-sync.
    """
    # Validate then grab token from configuration
    validate_configuration(configuration)
    log.warning("Example: API Connector : Scrunch AI")

    token = configuration["api_token"]
    brand_id = configuration["brand_id"]

    if "response_start_date" not in state:
        # Historical: 7-day initial historical sync
        response_start_date = (datetime.now() - relativedelta(days=7)).strftime("%Y-%m-%d")
    else:
        # Incremental: 2-day lookback from the previous end date
        saved_state = datetime.strptime(state.get("response_start_date"), "%Y-%m-%d")
        response_start_date = (saved_state - relativedelta(days=2)).strftime("%Y-%m-%d")

    end_date = datetime.now().strftime("%Y-%m-%d")

    # Pull and upsert responses
    get_all_responses(response_start_date, end_date, token, brand_id)
    get_all_performances(response_start_date, end_date, token, brand_id, "competitor")
    get_all_performances(response_start_date, end_date, token, brand_id, "scrunch")

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
    op.checkpoint(state={"response_start_date": end_date})


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your
# script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code.
# Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents
    with open("configuration.json", "r") as f:
        configuration = json.load(f)

    # Test the connector locally
    connector.debug()
