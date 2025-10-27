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
import time  # Rate limiting, retries, or backoff (if needed)
from datetime import datetime  # Timestamps and date utilities

import requests  # HTTP requests to Scrunch API
from dateutil.relativedelta import relativedelta  # Date arithmetic utilities

# For supporting Data operations like Upsert(), Update(), Delete()
# and Checkpoint()
# For enabling Logs in your connector code
# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

__API_BASE = "https://api.scrunchai.com/v1"
__LIST_JOINER = " | "  # Delimiter used to collapse list fields into strings


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
    return [
        {
            "table": "responses",  # Destination table for responses
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
        {
            "table": "daily_citations",  # Aggregated daily citations
            # No primary key specified; Fivetran will create _fivetran_id
        },
    ]


def get_responses(start_date, end_date, offset, token, brand_id):
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
        params={
            "start_date": start_date,
            "end_date": end_date,
            "offset": offset,
        },
    )

    resp.raise_for_status()
    data = resp.json()
    return data


def flatten_response(rec: dict) -> dict:
    # """
    # Transform a raw API response record into a flattened dictionary.

    # - Preserves key scalar fields (id, created_at, prompt_id, etc.)
    #   using direct access or safe .get().
    # - Converts list fields (tags, key_topics, competitors_present) into a
    #   single string joined by LIST_JOINER, or None if empty.
    # - Serializes `citations` (which may be a list of dicts) into a JSON
    #   string or None if missing.
    # - Ensures downstream consumers (e.g., database upserts)
    #   can rely on a flat schema of simple scalars
    #   and strings instead of nested lists/objects.
    #   Necessary because Fivetran does not take in lists as datatypes
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
        "tags": (
            __LIST_JOINER.join(rec.get("tags", []))
            if rec.get("tags")
            else None
        ),
        "key_topics": (
            __LIST_JOINER.join(rec.get("key_topics", []))
            if rec.get("key_topics")
            else None
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


def get_all_responses(start_date, end_date, token, brand_id):
    """
    Iterate through all paginated responses and upsert into 'responses' table.
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
    limit = 100
    MAX_RETRIES = 5
    INITIAL_BACKOFF = 2  # seconds

    while True:
        response = None

        for attempt in range(MAX_RETRIES):
            try:
                log.info(
                    f"Fetching responses (offset={offset}, attempt "
                    f"{attempt + 1}/{MAX_RETRIES})..."
                )
                response = get_responses(
                    start_date, end_date, offset, token, brand_id
                )
                break  # Success, exit retry loop

            except requests.exceptions.RequestException as e:
                log.warning(
                    f"Error fetching responses on attempt {attempt + 1}: {e}"
                )
                if attempt < MAX_RETRIES - 1:
                    wait_time = INITIAL_BACKOFF * (2**attempt)
                    log.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    log.error(
                        "Max retries reached while fetching responses. Failing"
                    )
                    raise  # re-raise the last exception

        if response is None:
            raise ConnectionError(
                "Failed to fetch responses after all retries."
            )

        total = response.get("total")
        items = response.get("items", [])

        log.info(f"Fetched {len(items)} responses (offset={offset}/{total})")

        for item in items:
            flat = flatten_response(item)
            # The 'upsert' operation is used to insert or update data in the
            # destination table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data
            #   into.
            # - The second argument is a dictionary containing the data to be
            #   upserted
            op.upsert(table="responses", data=flat)

        offset += limit
        if offset >= total:
            log.info("All responses fetched successfully.")
            break


def get_scrunch_performance(start_date, end_date, token, brand_id):
    """
    Pull overall Scrunch performance aggregates and upsert into
    'overall_scrunch_performance'.

    Fields include dates, prompt/persona info, platform, branded flag, and
    aggregate metrics.

    Args:
        start_date (str): Start date (YYYY-MM-DD).
        end_date   (str): End date   (YYYY-MM-DD).
        token      (str): Bearer token for Scrunch API.
    """
    MAX_RETRIES = 5
    INITIAL_BACKOFF = 2  # seconds

    # Construct base URL dynamically using brand_id
    base_url = f"{__API_BASE}/{brand_id}/query"

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
    fields_param = ",".join(fields)

    query_params = (
        f"?start_date={start_date}"
        f"&end_date={end_date}"
        f"&fields={fields_param}"
    )

    headers = {"Authorization": f"Bearer {token}"}

    url = f"{base_url}{query_params}"

    response = None
    for attempt in range(MAX_RETRIES):
        try:
            log.info(f"Fetching data (Attempt {attempt + 1}/{MAX_RETRIES})...")
            response = requests.get(url, headers=headers)
            # This will raise an HTTPError for bad responses (4xx or 5xx)
            response.raise_for_status()
            break  # Break the loop if the request was successful
        except requests.exceptions.RequestException as e:
            # Catch network errors and HTTP errors
            log.warning(f"Error fetching data on attempt {attempt + 1}: {e}")
            if attempt < MAX_RETRIES - 1:
                # Calculate exponential backoff time
                wait_time = INITIAL_BACKOFF * (2**attempt)
                log.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                # Re-raise the exception on the last attempt
                log.error("Max retries reached. Failing.")
                raise  # Re-raise the last exception

    # Check if a successful response was obtained
    # (should always be true if break was hit)
    if response is None:
        # Should not be reached if the final 'raise' is working, but safe to
        # include
        raise ConnectionError("Failed to fetch data after all retries.")

    data = response.json()  # list[dict]

    for rec in data:
        # The 'upsert' operation is used to insert or update data in the
        # destination table.
        # The op.upsert method is called with two arguments:
        # - The first argument is the name of the table to upsert the data
        # - The second argument is a dictionary containing
        # - the data to be upserted
        op.upsert(table="overall_scrunch_performance", data=rec)


def get_competitor_performance(start_date, end_date, token, brand_id):
    """
    Pull competitor performance aggregates and
    upsert into 'competitor_performance'.
    """
    DIMS = [
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
    METRICS = [
        "responses",
        "brand_presence_percentage",
        "competitor_presence_percentage",
    ]
    FIELDS = DIMS + METRICS

    base_url = f"{__API_BASE}/{brand_id}/query"
    params = {
        "start_date": start_date,
        "end_date": end_date,
        "fields": ",".join(FIELDS),
        "group_by": ",".join(DIMS),
    }
    headers = {"Authorization": f"Bearer {token}"}

    MAX_RETRIES = 5
    INITIAL_BACKOFF = 2  # seconds

    response = None
    for attempt in range(MAX_RETRIES):
        try:
            log.info(
                "Fetching competitor performance (attempt "
                f"{attempt + 1}/{MAX_RETRIES})..."
            )
            response = requests.get(base_url, headers=headers, params=params)
            response.raise_for_status()
            break  # success
        except requests.exceptions.RequestException as e:
            log.warning(f"Error on attempt {attempt + 1}: {e}")
            if attempt < MAX_RETRIES - 1:
                wait_time = INITIAL_BACKOFF * (2**attempt)
                log.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                log.error("Max retries reached for competitor performance.")
                raise

    if response is None:
        raise ConnectionError(
            "Failed to fetch competitor performance after all retries."
        )

    data = response.json()  # list[dict]

    log.info(f"Upserting {len(data)} competitor performance rows...")
    for rec in data:
        # The 'upsert' operation is used to insert or update data in the
        # destination table.
        # The op.upsert method is called with two arguments:
        # - The first argument is the name of the table to upsert the data
        # - The second argument is a dictionary containing
        # - the data to be upserted
        op.upsert(table="competitor_performance", data=rec)


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
    token = configuration["api_token"]
    brand_id = configuration["brand_id"]

    if "response_start_date" not in state:
        # Historical: 7-day initial historical sync
        response_start_date = (
            datetime.now() - relativedelta(days=7)
        ).strftime("%Y-%m-%d")
    else:
        # Incremental: 2-day lookback from the previous end date
        response_start_date = (
            datetime.strptime(state.get("response_start_date"), "%Y-%m-%d")
            - relativedelta(days=2)
        ).strftime("%Y-%m-%d")

    end_date = datetime.now().strftime("%Y-%m-%d")

    # Pull and upsert responses
    get_all_responses(response_start_date, end_date, token, brand_id)
    get_scrunch_performance(response_start_date, end_date, token, brand_id)
    get_competitor_performance(response_start_date, end_date, token, brand_id)

    # Save the progress by checkpointing the state
    op.checkpoint(state={"response_start_date": end_date})


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

# Standard Python entry point for local testing
# (not used by Fivetran in production)
# Please test using the Fivetran debug command prior
# to finalizing and deploying.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents
    with open("configuration.json", "r") as f:
        configuration = json.load(f)
