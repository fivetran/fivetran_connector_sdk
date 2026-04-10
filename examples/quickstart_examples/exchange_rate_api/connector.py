"""
Fivetran Connector SDK connector for syncing historical exchange rates from the Frankfurter API.
See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details.
"""

import json
import time
from dataclasses import dataclass
from datetime import datetime, timedelta

import requests
from fivetran_connector_sdk import Connector, Logging as log, Operations as op

# Constants
__API_URL = "https://api.frankfurter.dev/v2/rates?base={from_currency}&quotes={to_currency}&from={from_date}&to={to_date}"
__MAX_RETRIES = 3
__BASE_DELAY = 2  # seconds, used for exponential backoff
__REQUEST_TIMEOUT = 60  # seconds
__DEFAULT_CURSOR = "2020-01-01"


@dataclass
class ExchangeRate:
    """Represents a single exchange rate record returned by the Frankfurter API."""
    date: str
    base: str
    quote: str
    rate: float


def build_api_url(from_currency: str, to_currency: str, from_date: str, to_date: str) -> str:
    """Build the API request URL from the given currency pair and date range."""
    return __API_URL.format(
        from_currency=from_currency,
        to_currency=to_currency,
        from_date=from_date,
        to_date=to_date,
    )


def schema(configuration: dict):
    """
    Define the schema for the exchange_rates table.
    See: https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    """
    return [
        {
            "table": "exchange_rates",
            "primary_key": ["date", "from_currency", "to_currency"],
            "columns": {"date": "NAIVE_DATE", "from_currency": "STRING", "to_currency": "STRING",
                        "rate": {"type": "DECIMAL", "precision": 15, "scale": 2}}
        }
    ]


def update(configuration: dict, state: dict):
    """
    Called by Fivetran during each sync. Fetches exchange rates incrementally using cursor-based state.
    See: https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    """
    # Validate required configuration keys
    for key in ("from_currency", "to_currency"):
        if key not in configuration or not configuration[key]:
            log.severe(f"Missing required configuration key: '{key}'")
            return

    from_currency = configuration["from_currency"]
    to_currency = configuration["to_currency"]
    log.fine(f"DEBUG configuration: {configuration}")
    log.fine(f"DEBUG state: {state}")

    # Determine from_date: day after last checkpoint, or default cursor for first sync
    if state.get("last_synced_date"):
        last_synced = datetime.strptime(state["last_synced_date"], "%Y-%m-%d")
        from_date = (last_synced + timedelta(days=1)).strftime("%Y-%m-%d")
    else:
        from_date = __DEFAULT_CURSOR

    to_date = datetime.today().strftime("%Y-%m-%d")

    if from_date > to_date:
        log.info("Already synced up to today. Nothing to fetch.")
        return

    url = build_api_url(from_currency, to_currency, from_date, to_date)
    log.fine(f"DEBUG request URL: {url}")
    log.info(f"Fetching exchange rates from {from_date} to {to_date} for {from_currency} -> {to_currency}")

    # Fetch data with retry and exponential backoff
    exchange_rates = []
    for attempt in range(1, __MAX_RETRIES + 1):
        try:
            response = requests.get(url, timeout=__REQUEST_TIMEOUT)
            response.raise_for_status()
            data = response.json()
            if not isinstance(data, list):
                log.severe(f"Unexpected API response type: {type(data).__name__}. Expected a list.")
                return
            exchange_rates = data
            log.fine(f"DEBUG API response length: {len(exchange_rates)}")
            if exchange_rates:
                log.fine(f"DEBUG first record: {exchange_rates[0]}")
            break
        except requests.RequestException as e:
            log.warning(f"Request attempt {attempt}/{__MAX_RETRIES} failed: {e}")
            if attempt < __MAX_RETRIES:
                delay = __BASE_DELAY ** attempt
                log.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                log.severe("All retry attempts exhausted. Aborting sync.")
                return


    # exchange_rates = exchange_rates[:5]

    # Upsert each record and track the latest date for checkpointing
    last_synced_date = None
    upsert_count = 0
    for exchange_rate in exchange_rates:
        try:
            record = ExchangeRate(**exchange_rate)
            op.upsert(
                "exchange_rates",
                {
                    "date": record.date,
                    "from_currency": record.base,
                    "to_currency": record.quote,
                    "rate": str(record.rate),
                },
            )
            upsert_count += 1
            last_synced_date = record.date
        except Exception as e:
            record_id = exchange_rate.get("date", "unknown") if isinstance(exchange_rate, dict) else str(exchange_rate)
            log.fine(f"DEBUG failed raw record: {exchange_rate}")
            log.warning(f"Failed to process exchange rate record ({record_id}): {e}.")
            break

    log.info(f"Upserted {upsert_count}/{len(exchange_rates)} exchange rates.")

    # Checkpoint so the next sync resumes from the day after last_synced_date
    if last_synced_date:
        op.checkpoint(state={"last_synced_date": last_synced_date})
        log.info(f"Checkpointed last_synced_date: {last_synced_date}")


connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    try:
        # Try loading the configuration from the file
        with open("configuration.json", "r") as f:
            configuration = json.load(f)
    except FileNotFoundError:
        # Fallback to an empty configuration if the file is not found
        configuration = {}
    # Allows testing the connector directly
    connector.debug(configuration=configuration)
