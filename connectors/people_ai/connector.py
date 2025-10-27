from __future__ import annotations

# The unused imports 'Sequence' and 'Literal' have been removed.
from typing import Any, Dict, List

# Import the json module to handle JSON data.
import json

# Import time module to handle time-related tasks, suchs as delays.
import time

# Import requests to make HTTP calls to API
import requests

# Import required classes from fivetran_connector_sdk.
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

# --- Configuration ---
__API_BASE = "https://api.people.ai"

activity = "activity"
__ACTIVITY_TYPES = ["participants"]

__MAX_RETRIES = 5
__INITIAL_DELAY_SECONDS = 2
__REAUTH_RETRY_COUNT = 1
# --- End Configuration ---


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
    required_configs = ["api_key", "api_secret"]

    # Check for missing keys or empty values
    for key in required_configs:
        value = configuration.get(key)
        if not value:
            raise ValueError(f"Missing required configuration value: {key}")


def schema(configuration: dict) -> List[Dict[str, Any]]:
    """
    Define the schema function which configures
    the tables delivered by the connector.
    Updated to only include the base 'activity' table
    and the 'participants' table.
    """
    return [
        {
            "table": "activity",  # The base 'activity' table
            "primary_key": ["uid"],
        },
        {
            "table": "participants",  # The 'participants' table
            "primary_key": ["uid", "email"],
        },
    ]


# The 'update' function will now need to pass a reauth function to get_page
def get_page(
    access_token: str,
    reauth_func: callable,  # Added reauth function for 401 handling
    activity_type: str | None = None,  # Made optional for base endpoint
    limit: int = 50,
    offset: int = 0,
    timeout: int = 30,
) -> List[Dict[str, Any]]:
    """
    Fetches a single page of data with
    exponential backoff for 502/server errors
    and a token refresh attempt for 401 errors.
    """
    # Construct the URL: /activities or /activities/{type}
    url = f"{__API_BASE}/v0/public/activities"
    if activity_type:
        url += f"/{activity_type}"

    params: Dict[str, Any] = {"limit": limit, "offset": offset}

    # Retry loop setup
    current_token = access_token

    # Outer loop for re-authentication attempt on 401
    for reauth_attempt in range(__REAUTH_RETRY_COUNT + 1):

        # Inner loop for exponential backoff on server errors (5xx)
        for attempt in range(__MAX_RETRIES + 1):

            headers = {"Authorization": f"Bearer {current_token}"}

            try:
                r = requests.get(
                    url, headers=headers,
                    params=params, timeout=timeout
                )
                r.raise_for_status()
                payload = r.json()

                # Success, return the data
                if isinstance(payload, dict):
                    return payload.get("data", [])
                return payload

            except requests.exceptions.HTTPError as e:
                status_code = e.response.status_code

                # --- Handle 401 (Unauthorized) ---
                if status_code == 401:
                    log.warning(
                        f"Received 401 Client Error at offset {offset}."
                    )
                    break  # Break inner loop to trigger re-auth attempt

                # --- Handle 502/5xx (Server/Gateway Errors) ---
                elif 500 <= status_code <= 599:
                    if attempt < __MAX_RETRIES:
                        # Exponential backoff calculation:
                        # BASE_DELAY * (2^attempt)
                        delay = __INITIAL_DELAY_SECONDS * (2**attempt)
                        log.warning(
                            f"Received {status_code}. Retrying... "
                        )
                        time.sleep(delay)
                        continue  # Continue inner loop for another request
                    else:
                        # Max retries reached
                        log.error(
                            f"Failed to fetch page after {__MAX_RETRIES + 1} "
                            f"due to {status_code} error."
                        )
                        raise e

                # --- Handle other HTTP errors (e.g., 400, 404) ---
                else:
                    log.error(
                        f"Received unrecoverable HTTP error {status_code}"
                    )
                    raise e

            except requests.exceptions.RequestException as e:
                # Handle connection/timeout errors
                if attempt < __MAX_RETRIES:
                    delay = __INITIAL_DELAY_SECONDS * (2**attempt)
                    time.sleep(delay)
                    continue
                else:
                    # Max retries reached
                    log.error(
                        f"Failed to fetch page after {__MAX_RETRIES + 1} "
                        "retries due to connection issues."
                    )
                    raise e

        # --- 401 Handling Logic ---
        # This point is reached only if a 401 error
        # caused the inner loop to 'break'
        if reauth_attempt < __REAUTH_RETRY_COUNT:
            log.info("Attempting to refresh access token...")
            try:
                # Call the passed re-authentication function
                current_token = reauth_func()
                access_token = current_token  # Update the token
                log.info("Token refreshed successfully. Retrying request.")
                continue  # Continue outer loop with new token
            except Exception as e:
                log.error(f"Failed to refresh token: {e}")
                raise e  # If re-auth fails, raise the exception
        else:
            # If 401 and max reauth retries reached
            log.error(
                f"Authentication failed after {__REAUTH_RETRY_COUNT + 1}"
                " re-authentication attempts."
            )
            raise requests.exceptions.HTTPError(
                "401 Client Error: Unauthorized (Max re-auth retries reached)"
            )


def sync_base_activities(
    access_token: str, reauth_func: callable, op: op, *, limit: int = 50
) -> int:
    """
    Handles the full pagination and upsert logic
    for the base /activities endpoint.
    Passes the reauth_func to get_page.
    """
    offset = 0
    total = 0

    while True:
        try:
            # Pass the reauth_func
            page = get_page(
                access_token,
                reauth_func,
                activity_type=None,
                limit=limit,
                offset=offset,
            )
        except requests.exceptions.HTTPError as e:
            # The retry logic is now inside get_page,
            # so an error here means it failed permanently
            log.error(
                f"Permanent failure fetching base activities"
                f" at offset {offset}: {e}"
            )
            break

        if not page:
            break

        for rec in page:
            data_to_upsert = rec.copy()

            # The base endpoint may contain the 'subject' field too
            # so we rename it
            if "subject" in data_to_upsert:
                data_to_upsert["api_subject"] = data_to_upsert.pop("subject")

            # The 'upsert' operation is used to insert or update data in the
            # destination table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data
            #   into.
            # - The second argument is a dictionary containing the data to be
            #   upserted
            op.upsert(table=activity, data=data_to_upsert)

        total += len(page)

        if len(page) < limit:
            break

        offset += limit

    return total


def sync_activity_type(
    access_token: str,
    reauth_func: callable,
    op: op,
    activity_type: str,
    *,
    limit: int = 50,
) -> int:
    """
    Handles the full pagination and upsert logic
    for a single specific activity type (/activities/{type}).
    Passes the reauth_func to get_page.
    """
    offset = 0
    total = 0

    while True:
        try:
            # Pass the reauth_func
            page = get_page(
                access_token, reauth_func, activity_type,
                limit=limit, offset=offset
            )
        except requests.exceptions.HTTPError as e:
            # The retry logic is now inside get_page,
            # so an error here means it failed permanently
            log.error(
                f"Permanent failure fetching {activity_type}"
                f" at offset {offset}: {e}"
            )
            break

        if not page:
            break

        for rec in page:
            data_to_upsert = rec.copy()

            # The original logic for renaming 'subject'
            # for certain types is kept
            if "subject" in data_to_upsert and activity_type in [
                "meeting",
                "email",
                "call",
            ]:
                data_to_upsert["api_subject"] = data_to_upsert.pop("subject")

            # The 'upsert' operation is used to insert or update data in the
            # destination table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data
            #   into.
            # - The second argument is a dictionary containing the data to be
            #   upserted
            op.upsert(table=activity_type, data=data_to_upsert)

        total += len(page)

        if len(page) < limit:
            break

        offset += limit

    log.info(
        f"Completed sync for '{activity_type}'. Total records synced: {total}"
    )
    return total


def get_access_token(api_key: str, api_secret: str) -> str:
    """
    Fetches the OAuth access token using client credentials.
    """
    url = f"{__API_BASE}/auth/v1/tokens"
    data = {
        "grant_type": "client_credentials",
        "client_id": api_key,
        "client_secret": api_secret,
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    resp = requests.post(url, data=data, headers=headers)
    resp.raise_for_status()
    return resp.json()["access_token"]


def update(configuration: dict, state: dict):
    """
    The main sync function
    It authenticates, then syncs the base 'activity' table
    and only the 'participants' activity type.
    """
    validate_configuration(configuration)

    # Define a closure function for re-authentication to pass to sync functions
    api_key = configuration["api_key"]
    api_secret = configuration["api_secret"]

    def reauthenticate():
        nonlocal access_token
        access_token = get_access_token(api_key, api_secret)
        return access_token

    # 1. Initial Authentication
    log.info("Starting initial authentication process.")
    try:
        access_token = get_access_token(api_key, api_secret)
    except requests.exceptions.RequestException as e:
        log.error(f"FATAL: Initial authentication failed: {e}")
        return  # Stop the sync

    total_records_synced = 0

    # 2. Sync the base /activities endpoint to the 'activity' table
    log.info("\n--- Starting synchronization for activity table ---")
    count = sync_base_activities(
        access_token=access_token,
        reauth_func=reauthenticate,  # Pass the reauth function
        op=op,
        limit=100000,
    )
    total_records_synced += count

    # 3. Sync only the specific activity type
    # endpoints defined in __ACTIVITY_TYPES
    log.info(
        f"\nStarting sync for the {len(__ACTIVITY_TYPES)} "
        "specific activity type(s)."
    )

    for activity_type in __ACTIVITY_TYPES:
        count = sync_activity_type(
            access_token=access_token,
            reauth_func=reauthenticate,  # Pass the reauth function
            op=op,
            activity_type=activity_type,
            limit=100000,
        )
        total_records_synced += count

    log.info(
        f"\n--- All tables synced. Total records processed: "
        f"{total_records_synced} ---"
    )


# This creates the connector object
# that will use the update and schema functions.
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
if __name__ == "__main__":
    try:
        with open("configuration.json", "r") as f:
            configuration = json.load(f)

        if (
            not configuration.get("api_key")
            or configuration.get("api_key") == "YOUR_CLIENT_ID"
        ):
            log.warning(
                "Please update configuration.json "
                "with actual api_key and api_secret."
            )

        connector.debug(configuration=configuration)

    except FileNotFoundError:
        log.error(
            "Error: configuration.json not found. "
            "Please create it for local testing."
        )
    except Exception as e:
        log.error(f"An unexpected error occurred during debug execution: {e}")
