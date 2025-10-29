"""Keycloak Admin API Connector for Fivetran.
This connector syncs identity and access management data from Keycloak including users, groups, roles,
clients, and authentication events to enable security analytics, compliance reporting, and user behavior analysis.
See the Technical Reference documentation
(https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
and the Best Practices documentation
(https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

# For reading configuration from a JSON file
import json

# For making HTTP requests to Keycloak Admin API
import requests

# For time-based operations and retry delays
import time

# For date and time manipulation
from datetime import datetime, timezone

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# Constants for API configuration
__MAX_RETRIES = 3
__BASE_DELAY_SECONDS = 1
__CHECKPOINT_INTERVAL = 100
__PAGE_SIZE = 100
__TOKEN_EXPIRY_BUFFER_SECONDS = 60


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    This function is called at the start of the update method to ensure that the connector has all necessary
    configuration values.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing.
    """
    required_configs = ["keycloak_url", "realm", "client_id", "client_secret"]
    for key in required_configs:
        if key not in configuration:
            raise ValueError(f"Missing required configuration value: {key}")


def get_access_token(keycloak_url: str, realm: str, client_id: str, client_secret: str):
    """
    Obtain OAuth2 access token using client credentials grant type.
    Args:
        keycloak_url: The base URL of the Keycloak server.
        realm: The Keycloak realm name.
        client_id: The client ID for authentication.
        client_secret: The client secret for authentication.
    Returns:
        A tuple containing the access token and expiration timestamp.
    """
    token_url = f"{keycloak_url}/realms/{realm}/protocol/openid-connect/token"
    data = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials",
    }

    for attempt in range(__MAX_RETRIES):
        try:
            response = requests.post(token_url, data=data, timeout=30)

            if response.status_code == 200:
                token_data = response.json()
                expires_in = token_data.get("expires_in", 300)
                expiry_time = time.time() + expires_in - __TOKEN_EXPIRY_BUFFER_SECONDS
                log.info("Successfully obtained access token, expires in %d seconds" % expires_in)
                return token_data["access_token"], expiry_time
            elif response.status_code in [429, 500, 502, 503, 504]:
                if attempt < __MAX_RETRIES - 1:
                    delay = __BASE_DELAY_SECONDS * (2**attempt)
                    log.warning(
                        f"Token request failed with status {response.status_code}, "
                        f"retrying in {delay} seconds (attempt {attempt + 1}/{__MAX_RETRIES})"
                    )
                    time.sleep(delay)
                    continue
                else:
                    log.severe(
                        f"Failed to obtain access token after {__MAX_RETRIES} attempts. "
                        f"Last status: {response.status_code}"
                    )
                    raise RuntimeError(
                        f"Token API returned {response.status_code} after {__MAX_RETRIES} attempts: "
                        f"{response.text}"
                    )
            else:
                log.severe(
                    f"Authentication failed with status {response.status_code}: {response.text}"
                )
                raise RuntimeError(
                    f"Authentication failed: {response.status_code} - {response.text}"
                )

        except requests.exceptions.RequestException as e:
            if attempt < __MAX_RETRIES - 1:
                delay = __BASE_DELAY_SECONDS * (2**attempt)
                log.warning(
                    f"Request exception during token fetch: {str(e)}, retrying in {delay} seconds"
                )
                time.sleep(delay)
                continue
            else:
                log.severe(
                    f"Failed to obtain access token after {__MAX_RETRIES} attempts due to: {str(e)}"
                )
                raise RuntimeError(f"Failed to obtain access token: {str(e)}")

    raise RuntimeError("Failed to obtain access token after all retry attempts")


def make_api_request(url: str, headers: dict, params: dict = None):
    """
    Make API request to Keycloak with retry logic and error handling.
    Args:
        url: The API endpoint URL.
        headers: HTTP headers including authorization token.
        params: Query parameters for the request.
    Returns:
        JSON response from the API.
    """
    for attempt in range(__MAX_RETRIES):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)

            if response.status_code == 200:
                return response.json()
            elif response.status_code == 401:
                log.severe(f"Unauthorized access to {url}. Token may be expired or invalid.")
                raise RuntimeError("Unauthorized: Check token and permissions")
            elif response.status_code == 403:
                log.severe(
                    "Forbidden access to %s. Service account may lack required roles." % url
                )
                raise RuntimeError("Forbidden: Service account needs view permissions")
            elif response.status_code == 404:
                log.warning(f"Resource not found at {url}")
                return []
            elif response.status_code in [429, 500, 502, 503, 504]:
                if attempt < __MAX_RETRIES - 1:
                    delay = __BASE_DELAY_SECONDS * (2**attempt)
                    log.warning(
                        f"Request failed with status {response.status_code}, "
                        f"retrying in {delay} seconds (attempt {attempt + 1}/{__MAX_RETRIES})"
                    )
                    time.sleep(delay)
                    continue
                else:
                    log.severe(
                        f"Failed to fetch data from {url} after {__MAX_RETRIES} attempts. "
                        f"Last status: {response.status_code}"
                    )
                    raise RuntimeError(
                        f"API returned {response.status_code} after {__MAX_RETRIES} attempts: {response.text}"
                    )
            else:
                log.severe(f"Unexpected status {response.status_code} from {url}: {response.text}")
                raise RuntimeError(f"API error {response.status_code}: {response.text}")

        except requests.exceptions.RequestException as e:
            if attempt < __MAX_RETRIES - 1:
                delay = __BASE_DELAY_SECONDS * (2**attempt)
                log.warning(f"Request exception: {str(e)}, retrying in {delay} seconds")
                time.sleep(delay)
                continue
            else:
                log.severe(
                    f"Failed to fetch data from {url} after {__MAX_RETRIES} attempts due to: {str(e)}"
                )
                raise RuntimeError(f"Request failed: {str(e)}")

    raise RuntimeError("API request failed after all retry attempts")


def flatten_user_record(user: dict):
    """
    Flatten user record by extracting nested access fields into parent level.
    Args:
        user: User record from Keycloak API.
    Returns:
        Flattened user record.
    """
    flattened = user.copy()

    if "access" in flattened and isinstance(flattened["access"], dict):
        access_data = flattened.pop("access")
        flattened["access_manage_group_membership"] = access_data.get("manageGroupMembership")
        flattened["access_view"] = access_data.get("view")
        flattened["access_map_roles"] = access_data.get("mapRoles")
        flattened["access_impersonate"] = access_data.get("impersonate")
        flattened["access_manage"] = access_data.get("manage")

    if "attributes" in flattened:
        flattened.pop("attributes")

    if "realmRoles" in flattened:
        flattened.pop("realmRoles")

    if "clientRoles" in flattened:
        flattened.pop("clientRoles")

    if "requiredActions" in flattened:
        flattened.pop("requiredActions")

    return flattened


def sync_users(keycloak_url: str, realm: str, headers: dict, state: dict):
    """
    Sync users from Keycloak and create breakout tables for attributes and roles.
    Args:
        keycloak_url: The base URL of the Keycloak server.
        realm: The Keycloak realm name.
        headers: HTTP headers including authorization token.
        state: State dictionary for tracking sync progress.
    Returns:
        Updated state with last sync timestamp.
    """
    users_url = f"{keycloak_url}/admin/realms/{realm}/users"
    first_index = 0
    record_count = 0
    max_created_timestamp = state.get("users_last_created_timestamp", 0)

    log.info(f"Starting user sync from timestamp: {max_created_timestamp}")

    while True:
        params = {"first": first_index, "max": __PAGE_SIZE}

        users = make_api_request(users_url, headers, params)

        if not users:
            log.info(f"No more users to sync. Total synced: {record_count}")
            break

        for user in users:
            flattened_user = flatten_user_record(user)

            # The 'upsert' operation is used to insert or update data in the destination table.
            # The first argument is the name of the destination table.
            # The second argument is a dictionary containing the record to be upserted.
            op.upsert(table="user", data=flattened_user)
            record_count += 1

            user_id = user.get("id")
            created_timestamp = user.get("createdTimestamp", 0)

            if created_timestamp > max_created_timestamp:
                max_created_timestamp = created_timestamp

            if "attributes" in user and isinstance(user["attributes"], dict):
                for attribute_key, attribute_values in user["attributes"].items():
                    if isinstance(attribute_values, list):
                        for attribute_value in attribute_values:
                            attribute_record = {
                                "user_id": user_id,
                                "attribute_key": attribute_key,
                                "attribute_value": attribute_value,
                            }
                            # The 'upsert' operation is used to insert or update data in the destination table.
                            # The first argument is the name of the destination table.
                            # The second argument is a dictionary containing the record to be upserted.
                            op.upsert(table="user_attribute", data=attribute_record)

            if "realmRoles" in user and isinstance(user["realmRoles"], list):
                for role_name in user["realmRoles"]:
                    role_record = {"user_id": user_id, "role_name": role_name}
                    # The 'upsert' operation is used to insert or update data in the destination table.
                    # The first argument is the name of the destination table.
                    # The second argument is a dictionary containing the record to be upserted.
                    op.upsert(table="user_realm_role", data=role_record)

            if "requiredActions" in user and isinstance(user["requiredActions"], list):
                for action in user["requiredActions"]:
                    action_record = {"user_id": user_id, "required_action": action}
                    # The 'upsert' operation is used to insert or update data in the destination table.
                    # The first argument is the name of the destination table.
                    # The second argument is a dictionary containing the record to be upserted.
                    op.upsert(table="user_required_action", data=action_record)

        if record_count % __CHECKPOINT_INTERVAL == 0 and record_count > 0:
            state["users_last_created_timestamp"] = max_created_timestamp
            # Save the progress by checkpointing the state. This is important for ensuring that
            # the sync process can resume from the correct position in case of next sync or interruptions.
            # Learn more about how and where to checkpoint by reading our best practices documentation
            # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
            op.checkpoint(state)
            log.info(f"Checkpointed after syncing {record_count} users")

        if len(users) < __PAGE_SIZE:
            log.info(f"Reached last page of users. Total synced: {record_count}")
            break

        first_index += __PAGE_SIZE

    state["users_last_created_timestamp"] = max_created_timestamp
    return state


def sync_groups(keycloak_url: str, realm: str, headers: dict, state: dict):
    """
    Sync groups from Keycloak and create breakout table for group members.
    Args:
        keycloak_url: The base URL of the Keycloak server.
        realm: The Keycloak realm name.
        headers: HTTP headers including authorization token.
        state: State dictionary for tracking sync progress.
    Returns:
        Updated state with last sync timestamp.
    """
    groups_url = f"{keycloak_url}/admin/realms/{realm}/groups"
    first_index = 0
    record_count = 0

    log.info("Starting group sync")

    while True:
        params = {"first": first_index, "max": __PAGE_SIZE}

        groups = make_api_request(groups_url, headers, params)

        if not groups:
            log.info(f"No more groups to sync. Total synced: {record_count}")
            break

        for group in groups:
            group_data = {
                "id": group.get("id"),
                "name": group.get("name"),
                "path": group.get("path"),
            }

            # The 'upsert' operation is used to insert or update data in the destination table.
            # The first argument is the name of the destination table.
            # The second argument is a dictionary containing the record to be upserted.
            op.upsert(table="group", data=group_data)
            record_count += 1

            group_id = group.get("id")
            members_url = f"{keycloak_url}/admin/realms/{realm}/groups/{group_id}/members"

            try:
                members = make_api_request(members_url, headers)

                if members and isinstance(members, list):
                    for member in members:
                        member_record = {"group_id": group_id, "user_id": member.get("id")}
                        # The 'upsert' operation is used to insert or update data in the destination table.
                        # The first argument is the name of the destination table.
                        # The second argument is a dictionary containing the record to be upserted.
                        op.upsert(table="group_member", data=member_record)
            except RuntimeError as e:
                log.warning(f"Could not fetch members for group {group_id}: {str(e)}")

        if record_count % __CHECKPOINT_INTERVAL == 0 and record_count > 0:
            # Save the progress by checkpointing the state. This is important for ensuring that
            # the sync process can resume from the correct position in case of next sync or interruptions.
            # Learn more about how and where to checkpoint by reading our best practices documentation
            # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
            op.checkpoint(state)
            log.info(f"Checkpointed after syncing {record_count} groups")

        if len(groups) < __PAGE_SIZE:
            log.info(f"Reached last page of groups. Total synced: {record_count}")
            break

        first_index += __PAGE_SIZE

    return state


def sync_roles(keycloak_url: str, realm: str, headers: dict, state: dict):
    """
    Sync realm roles from Keycloak.
    Args:
        keycloak_url: The base URL of the Keycloak server.
        realm: The Keycloak realm name.
        headers: HTTP headers including authorization token.
        state: State dictionary for tracking sync progress.
    Returns:
        Updated state.
    """
    roles_url = f"{keycloak_url}/admin/realms/{realm}/roles"
    record_count = 0

    log.info("Starting role sync")

    try:
        roles = make_api_request(roles_url, headers)

        if roles and isinstance(roles, list):
            for role in roles:
                role_data = {
                    "id": role.get("id"),
                    "name": role.get("name"),
                    "description": role.get("description"),
                    "composite": role.get("composite"),
                    "client_role": role.get("clientRole"),
                }

                # The 'upsert' operation is used to insert or update data in the destination table.
                # The first argument is the name of the destination table.
                # The second argument is a dictionary containing the record to be upserted.
                op.upsert(table="role", data=role_data)
                record_count += 1

            log.info(f"Synced {record_count} roles")
        else:
            log.info("No roles found in realm")
    except RuntimeError as e:
        log.warning(f"Could not fetch roles: {str(e)}")

    return state


def sync_clients(keycloak_url: str, realm: str, headers: dict, state: dict):
    """
    Sync OAuth clients from Keycloak.
    Args:
        keycloak_url: The base URL of the Keycloak server.
        realm: The Keycloak realm name.
        headers: HTTP headers including authorization token.
        state: State dictionary for tracking sync progress.
    Returns:
        Updated state.
    """
    clients_url = f"{keycloak_url}/admin/realms/{realm}/clients"
    record_count = 0

    log.info("Starting client sync")

    try:
        clients = make_api_request(clients_url, headers)

        if clients and isinstance(clients, list):
            for client in clients:
                client_data = {
                    "id": client.get("id"),
                    "client_id": client.get("clientId"),
                    "name": client.get("name"),
                    "description": client.get("description"),
                    "enabled": client.get("enabled"),
                    "public_client": client.get("publicClient"),
                    "protocol": client.get("protocol"),
                    "base_url": client.get("baseUrl"),
                }

                # The 'upsert' operation is used to insert or update data in the destination table.
                # The first argument is the name of the destination table.
                # The second argument is a dictionary containing the record to be upserted.
                op.upsert(table="client", data=client_data)
                record_count += 1

            log.info(f"Synced {record_count} clients")
        else:
            log.info("No clients found in realm")
    except RuntimeError as e:
        log.warning(f"Could not fetch clients: {str(e)}")

    return state


def sync_events(keycloak_url: str, realm: str, headers: dict, state: dict, start_date: str):
    """
    Sync authentication events from Keycloak with date range filtering.
    Args:
        keycloak_url: The base URL of the Keycloak server.
        realm: The Keycloak realm name.
        headers: HTTP headers including authorization token.
        state: State dictionary for tracking sync progress.
        start_date: Start date for event filtering in YYYY-MM-DD format.
    Returns:
        Updated state with last event timestamp.
    """
    events_url = f"{keycloak_url}/admin/realms/{realm}/events"
    record_count = 0
    last_event_time = state.get("events_last_time")

    if not last_event_time:
        last_event_time = start_date

    log.info(f"Starting event sync from date: {last_event_time}")

    params = {"dateFrom": last_event_time, "max": __PAGE_SIZE}

    try:
        events = make_api_request(events_url, headers, params)

        if events and isinstance(events, list):
            max_event_time = last_event_time

            for event in events:
                event_time_ms = event.get("time")
                if event_time_ms:
                    event_datetime = datetime.fromtimestamp(event_time_ms / 1000, tz=timezone.utc)
                    event_time_str = event_datetime.isoformat()

                    if event_time_str > max_event_time:
                        max_event_time = event_time_str
                else:
                    event_time_str = None

                event_data = {
                    "id": f"{event.get('time')}_{event.get('type')}_{event.get('userId', 'unknown')}",
                    "time": event_time_str,
                    "type": event.get("type"),
                    "user_id": event.get("userId"),
                    "session_id": event.get("sessionId"),
                    "ip_address": event.get("ipAddress"),
                    "client_id": event.get("clientId"),
                }

                # The 'upsert' operation is used to insert or update data in the destination table.
                # The first argument is the name of the destination table.
                # The second argument is a dictionary containing the record to be upserted.
                op.upsert(table="event", data=event_data)
                record_count += 1

            log.info(f"Synced {record_count} events")
            state["events_last_time"] = max_event_time
        else:
            log.info("No events found for the specified date range")
    except RuntimeError as e:
        log.warning(f"Could not fetch events: {str(e)}")

    return state


def sync_admin_events(keycloak_url: str, realm: str, headers: dict, state: dict, start_date: str):
    """
    Sync admin events from Keycloak with date range filtering.
    Args:
        keycloak_url: The base URL of the Keycloak server.
        realm: The Keycloak realm name.
        headers: HTTP headers including authorization token.
        state: State dictionary for tracking sync progress.
        start_date: Start date for event filtering in YYYY-MM-DD format.
    Returns:
        Updated state with last admin event timestamp.
    """
    admin_events_url = f"{keycloak_url}/admin/realms/{realm}/admin-events"
    record_count = 0
    last_admin_event_time = state.get("admin_events_last_time")

    if not last_admin_event_time:
        last_admin_event_time = start_date

    log.info(f"Starting admin event sync from date: {last_admin_event_time}")

    params = {"dateFrom": last_admin_event_time, "max": __PAGE_SIZE}

    try:
        admin_events = make_api_request(admin_events_url, headers, params)

        if admin_events and isinstance(admin_events, list):
            max_admin_event_time = last_admin_event_time

            for event in admin_events:
                event_time_ms = event.get("time")
                if event_time_ms:
                    event_datetime = datetime.fromtimestamp(event_time_ms / 1000, tz=timezone.utc)
                    event_time_str = event_datetime.isoformat()

                    if event_time_str > max_admin_event_time:
                        max_admin_event_time = event_time_str
                else:
                    event_time_str = None

                auth_details = event.get("authDetails", {})
                admin_event_data = {
                    "id": f"{event.get('time')}_{event.get('operationType')}_{event.get('resourceType', 'unknown')}",
                    "time": event_time_str,
                    "operation_type": event.get("operationType"),
                    "resource_type": event.get("resourceType"),
                    "resource_path": event.get("resourcePath"),
                    "auth_realm_id": auth_details.get("realmId") if auth_details else None,
                    "auth_client_id": auth_details.get("clientId") if auth_details else None,
                    "auth_user_id": auth_details.get("userId") if auth_details else None,
                }

                # The 'upsert' operation is used to insert or update data in the destination table.
                # The first argument is the name of the destination table.
                # The second argument is a dictionary containing the record to be upserted.
                op.upsert(table="admin_event", data=admin_event_data)
                record_count += 1

            log.info(f"Synced {record_count} admin events")
            state["admin_events_last_time"] = max_admin_event_time
        else:
            log.info("No admin events found for the specified date range")
    except RuntimeError as e:
        log.warning(f"Could not fetch admin events: {str(e)}")

    return state


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    return [
        {"table": "user", "primary_key": ["id"]},
        {"table": "user_attribute", "primary_key": ["user_id", "attribute_key"]},
        {"table": "user_realm_role", "primary_key": ["user_id", "role_name"]},
        {"table": "user_required_action", "primary_key": ["user_id", "required_action"]},
        {"table": "group", "primary_key": ["id"]},
        {"table": "group_member", "primary_key": ["group_id", "user_id"]},
        {"table": "role", "primary_key": ["id"]},
        {"table": "client", "primary_key": ["id"]},
        {"table": "event", "primary_key": ["id"]},
        {"table": "admin_event", "primary_key": ["id"]},
    ]


def update(configuration: dict, state: dict):
    """
    Define the update function which lets you configure how your connector fetches data.
    See the technical reference documentation for more details on the update function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
        state: a dictionary that holds the state of the connector.
    """
    log.warning("Example: Source Connector : Keycloak Admin API")

    validate_configuration(configuration=configuration)

    keycloak_url = configuration.get("keycloak_url")
    realm = configuration.get("realm")
    client_id = configuration.get("client_id")
    client_secret = configuration.get("client_secret")
    sync_events_enabled = configuration.get("sync_events", "true").lower() == "true"
    start_date = configuration.get("start_date", "2024-01-01")

    try:
        access_token, token_expiry = get_access_token(
            keycloak_url, realm, client_id, client_secret
        )

        headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}

        state = sync_users(keycloak_url, realm, headers, state)

        if time.time() > token_expiry:
            log.info("Access token expired, refreshing token")
            access_token, token_expiry = get_access_token(
                keycloak_url, realm, client_id, client_secret
            )
            headers["Authorization"] = f"Bearer {access_token}"

        state = sync_groups(keycloak_url, realm, headers, state)

        if time.time() > token_expiry:
            log.info("Access token expired, refreshing token")
            access_token, token_expiry = get_access_token(
                keycloak_url, realm, client_id, client_secret
            )
            headers["Authorization"] = f"Bearer {access_token}"

        state = sync_roles(keycloak_url, realm, headers, state)

        if time.time() > token_expiry:
            log.info("Access token expired, refreshing token")
            access_token, token_expiry = get_access_token(
                keycloak_url, realm, client_id, client_secret
            )
            headers["Authorization"] = f"Bearer {access_token}"

        state = sync_clients(keycloak_url, realm, headers, state)

        if sync_events_enabled:
            if time.time() > token_expiry:
                log.info("Access token expired, refreshing token")
                access_token, token_expiry = get_access_token(
                    keycloak_url, realm, client_id, client_secret
                )
                headers["Authorization"] = f"Bearer {access_token}"

            state = sync_events(keycloak_url, realm, headers, state, start_date)

            if time.time() > token_expiry:
                log.info("Access token expired, refreshing token")
                access_token, token_expiry = get_access_token(
                    keycloak_url, realm, client_id, client_secret
                )
                headers["Authorization"] = f"Bearer {access_token}"

            state = sync_admin_events(keycloak_url, realm, headers, state, start_date)

        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        op.checkpoint(state)

        log.info("Keycloak sync completed successfully")

    except Exception as e:
        log.severe(f"Failed to sync Keycloak data: {str(e)}")
        raise RuntimeError(f"Failed to sync data: {str(e)}")


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line
# or IDE 'run' button. This is useful for debugging while you write your code.
# Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents
    with open("configuration.json", "r") as f:
        configuration = json.load(f)

    # Test the connector locally
    connector.debug(configuration=configuration)
