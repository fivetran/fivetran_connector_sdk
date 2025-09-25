"""
This example shows how to pull AI/ML response and aggregate data from the Scrunch API
and load it into a destination using the Fivetran Connector SDK.

Scrunch is an AI/ML platform that provides responses, citations, and performance metrics
across prompts, personas, and platforms. This connector demonstrates:
- Paginated retrieval of individual responses
- Aggregated queries for overall performance, competitor performance, and daily citations
- Flattening/serialization of nested list fields for destination compatibility
- State check-pointing for incremental syncs

Refer to Scrunch's API for more information (https://api.scrunchai.com)
See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details.
"""

# --- Standard Library Imports -------------------------------------------------
import json  # Handle JSON serialization/deserialization
import time  # Rate limiting, retries, or backoff (if needed)
from datetime import datetime, timezone  # Timestamps and date utilities
from typing import Dict, List, Any, Optional  # Type hints for clarity
import os  # Environment variable access (when used with dotenv)

# --- Third-Party Imports ------------------------------------------------------
import requests  # HTTP requests to Scrunch API
from dotenv import load_dotenv  # Load environment variables from .env
from dateutil.relativedelta import relativedelta  # Date arithmetic utilities

# --- Fivetran Connector SDK Imports ------------------------------------------
from fivetran_connector_sdk import Connector  # Core connector object (update/schema)
from fivetran_connector_sdk import Logging as log  # Structured logging
from fivetran_connector_sdk import Operations as op  # Upsert/Update/Delete/checkpoint

# --- Module Constants ---------------------------------------------------------
API_BASE = "https://api.scrunchai.com/v1"
LIST_JOINER = " | "  # Delimiter used to collapse list fields into strings


def schema(configuration: dict):
    """
    Define the schema the connector delivers to the destination.

    See the technical reference for details on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema

    Args:
        configuration (dict): Connector configuration dictionary.

    Returns:
        list[dict]: Table definitions with table names and optional primary keys.
    """
    return [
        {
            "table": "responses",  # Destination table for raw/flattened responses
            "primary_key": ["id"],
        },
        {
            "table": "overall_scrunch_performance",  # Aggregated performance table
            # No primary key specified; Fivetran will create _fivetran_id
        },
        {
            "table": "competitor_performance",  # Aggregated competitor metrics
            # No primary key specified; Fivetran will create _fivetran_id
        },
        {
            "table": "daily_citations",  # Aggregated daily citations with source info
            # No primary key specified; Fivetran will create _fivetran_id
        },
    ]


# --- Responses API Helpers ----------------------------------------------------
def get_responses(start_date, end_date, offset, token):
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
        f"{API_BASE}/2812/responses",
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
    """
    Flatten a single raw response record into a destination-friendly mapping.

    - Preserves scalar fields (id, created_at, prompt_id, etc.).
    - Collapses list fields (tags, key_topics, competitors_present) into delimited strings.
    - Serializes 'citations' (if present) into a JSON string.
    - Ensures destination receives only scalars/strings (Fivetran does not load list types).

    Args:
        rec (dict): Raw response record.

    Returns:
        dict: Flattened record suitable for upsert into the 'responses' table.
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
        # Lists → scalars
        "tags": LIST_JOINER.join(rec.get("tags", [])) if rec.get("tags") else None,
        "key_topics": LIST_JOINER.join(rec.get("key_topics", [])) if rec.get("key_topics") else None,
        "competitors_present": LIST_JOINER.join(rec.get("competitors_present", [])) if rec.get("competitors_present") else None,
        "citations_json": json.dumps(rec.get("citations", []), ensure_ascii=False) if rec.get("citations") is not None else None,
    }


def get_all_responses(start_date, end_date, token):
    """
    Iterate through all paginated responses and upsert into 'responses' table.

    Uses get_responses() for pagination and flatten_response() to normalize each
    item before writing to the destination. Sets a checkpoint to enable incremental
    syncs based on end_date.

    Args:
        start_date (str): Start date in 'YYYY-MM-DD' format (passed through).
        end_date   (str): End date in 'YYYY-MM-DD' format (used in checkpoint).
        token     (str): Bearer token for Scrunch API.
    """
    offset = 0
    limit = 100

    while True:
        response = get_responses(start_date, end_date, offset, token)
        total = response.get("total")
        items = response.get("items", [])

        for item in items:
            flat = flatten_response(item)
            op.upsert(table="responses", data=flat)

        offset += limit
        if offset >= total:
            break

    op.checkpoint(state={"response_start_date": end_date})


# --- Query API Helpers --------------------------------------------------------
def get_scrunch_peformance(start_date, end_date, token):
    """
    Pull overall Scrunch performance aggregates and upsert into 'overall_scrunch_performance'.

    Fields include dates, prompt/persona info, platform, branded flag, and aggregate metrics.

    Args:
        start_date (str): Start date (YYYY-MM-DD).
        end_date   (str): End date   (YYYY-MM-DD).
        token     (str): Bearer token for Scrunch API.
    """
    response = requests.get(
        f"https://api.scrunchai.com/v1/2812/query"
        f"?start_date={start_date}&end_date={end_date}"
        f"&fields=date_week,date_month,date_year,date,"
        f"prompt_id,prompt,persona_id,persona_name,"
        f"ai_platform,branded,"
        f"responses,brand_presence_percentage,brand_position_score,"
        f"brand_sentiment_score,competitor_presence_percentage",
        headers={"Authorization": f"Bearer {token}"},
    )

    response.raise_for_status()
    data = response.json()  # list[dict]

    counter = 0
    for rec in data:
        op.upsert(table="overall_scrunch_performance", data=rec)
        counter += 1
        if counter % 100 == 0:
            print(f"Processed {counter} records...")

    print("completed overall_scrunch_performance!")
    op.checkpoint(state={"scrunch_performance_start_date": end_date})


def get_competitor_performance(start_date, end_date, token):
    """
    Pull competitor performance aggregates and upsert into 'competitor_performance'.

    Dimensions cover date granularity, prompt, platform, and competitor identifiers.
    Metrics include response counts and brand/competitor presence percentages.

    Args:
        start_date (str): Start date (YYYY-MM-DD).
        end_date   (str): End date   (YYYY-MM-DD).
        token     (str): Bearer token for Scrunch API.
    """
    DIMS = [
        "date", "date_month", "date_week", "date_year",
        "prompt_id", "prompt", "ai_platform", "competitor_id", "competitor_name",
    ]
    METRICS = [
        "responses", "brand_presence_percentage", "competitor_presence_percentage",
    ]
    FIELDS = DIMS + METRICS

    response = requests.get(
        "https://api.scrunchai.com/v1/2812/query",
        headers={"Authorization": f"Bearer {token}"},
        params={
            "start_date": start_date,
            "end_date": end_date,
            "fields": ",".join(FIELDS),
            "group_by": ",".join(DIMS),
        },
    )

    response.raise_for_status()
    data = response.json()  # list[dict]

    counter = 0
    for rec in data:
        op.upsert(table="competitor_performance", data=rec)
        counter += 1
        if counter % 100 == 0:
            print(f"Processed {counter} records...")

    print("completed competitor_performance!")
    op.checkpoint(state={"competitor_performance_start_date": end_date})


def get_daily_citations(start_date, end_date, token):
    """
    Pull daily citation aggregates and upsert into 'daily_citations'.

    Dimensions include date granularity, prompt, platform, and source metadata.
    Metric currently includes the number of responses.

    Args:
        start_date (str): Start date (YYYY-MM-DD).
        end_date   (str): End date   (YYYY-MM-DD).
        token     (str): Bearer token for Scrunch API.
    """
    DIMS = [
        "date", "date_month", "date_week", "date_year",
        "prompt_id", "prompt", "ai_platform",
        "source_type", "source_url",
    ]
    METRICS = ["responses"]
    FIELDS = DIMS + METRICS

    response = requests.get(
        "https://api.scrunchai.com/v1/2812/query",
        headers={"Authorization": f"Bearer {token}"},
        params={
            "start_date": start_date,
            "end_date": end_date,
            "fields": ",".join(FIELDS),
            "group_by": ",".join(DIMS),
        },
    )

    response.raise_for_status()
    data = response.json()  # list[dict]

    counter = 0
    for rec in data:
        op.upsert(table="daily_citations", data=rec)
        counter += 1
        if counter % 100 == 0:
            print(f"Processed {counter} records for Citations table...")

    print("completed!")
    op.checkpoint(state={"daily_citations_start_date": end_date})


# --- Fivetran Update Entrypoint ----------------------------------------------
def update(configuration: dict, state: dict):
    """
    Main sync loop executed by Fivetran.

    Loads configuration, computes date windows for historical/incremental sync,
    fetches response data and aggregates, writes to destination tables, and finally
    records state for incremental progress.

    See technical reference for more details:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update

    Args:
        configuration (dict): Deployment-time secrets and settings.
        state         (dict): State from previous runs (empty on first/full sync).
    """
    # Grab token from configuration
    token = configuration["api_token"]
    if not token:
        raise RuntimeError("SCRUNCH_API_TOKEN not set in configuration")

    # --- RESPONSES API (historical vs. incremental windows) -------------------
    if "response_start_date" not in state:
        # Historical: 1-day lookback
        response_start_date = (datetime.now() - relativedelta(days=7)).strftime("%Y-%m-%d")
    else:
        # Incremental: 3-day lookback
        response_start_date = (datetime.now() - relativedelta(days=3)).strftime("%Y-%m-%d")

    end_date = datetime.now().strftime("%Y-%m-%d")

    # Pull and upsert responses
    get_all_responses(response_start_date, end_date, token)

    # --- QUERY API: overall_scrunch_performance -------------------------------
    if "scrunch_performance_start_date" not in state:
        scrunch_performance_start_date = (datetime.now() - relativedelta(days=7)).strftime("%Y-%m-%d")
    else:
        scrunch_performance_start_date = (datetime.now() - relativedelta(days=3)).strftime("%Y-%m-%d")

    get_scrunch_peformance(scrunch_performance_start_date, end_date, token)

    # --- QUERY API: competitor_performance ------------------------------------
    if "competitor_performance_start_date" not in state:
        competitor_performance_start_date = (datetime.now() - relativedelta(days=7)).strftime("%Y-%m-%d")
    else:
        competitor_performance_start_date = (datetime.now() - relativedelta(days=3)).strftime("%Y-%m-%d")

    get_competitor_performance(competitor_performance_start_date, end_date, token)

    # --- QUERY API: daily_citations -------------------------------------------
    if "daily_citations_start_date" not in state:
        daily_citations_start_date = (datetime.now() - relativedelta(days=7)).strftime("%Y-%m-%d")
    else:
        daily_citations_start_date = (datetime.now() - relativedelta(days=3)).strftime("%Y-%m-%d")

    get_daily_citations(daily_citations_start_date, end_date, token)


# --- Connector Object & Local Debug Harness ----------------------------------
# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

# Standard Python entry point for local testing (not used by Fivetran in production)
# Please test using the Fivetran debug command prior to finalizing and deploying.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents
    with open("configuration.json", 'r') as f:
        configuration = json.load(f)

    # Test the connector locally
    connector.debug(configuration=configuration)
