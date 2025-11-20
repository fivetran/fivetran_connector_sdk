"""
Fivetran Connector SDK – Linear API Source
Supports incremental sync (updatedAt), delete capture (archived), and dynamic schema discovery.
"""

import requests  # HTTP client library used to send requests to external APIs (GET/POST, etc.)
from typing import Dict, List, Any, Optional  # Type hints for dictionaries, lists, generic values, and optional fields
from datetime import datetime, timezone  # Utilities for working with timezone-aware UTC timestamps

# Import required classes from fivetran_connector_sdk
# For supporting Connector operations like Update() and Schema()
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

__PAGE_SIZE = 250

def graphql_query(api_key, query, variables=None):
    """
    Perform a GraphQL query against the Linear API.
    Args: api_key: Linear API key
          query: GraphQL query string
          variables: Optional variables for the query
    Returns: JSON response data
    """
    headers = {"Content-Type": "application/json", "Authorization": api_key.strip()}
    payload = {"query": query, "variables": variables or {}}
    r = requests.post("https://api.linear.app/graphql", headers=headers, json=payload, timeout=60)

    if r.status_code != 200:
        log.severe(f"Linear returned {r.status_code}: {r.text}")
        r.raise_for_status()

    data = r.json()
    if "errors" in data:
        log.severe(f"GraphQL errors: {data['errors']}")
        raise Exception(f"GraphQL errors: {data['errors']}")
    return data["data"]


def to_iso(dt: Optional[str]) -> Optional[str]:
    """
    Convert a datetime string to ISO 8601 format in UTC.
    Args: dt: datetime string
    Returns: ISO 8601 formatted datetime string in UTC or None
    """
    if not dt:
        return None
    try:
        # Ensure UTC ISO 8601
        return datetime.fromisoformat(dt.replace("Z", "+00:00")).astimezone(timezone.utc).isoformat()
    except Exception:
        return dt


def schema(configuration: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    api_key = configuration.get("linear_api_key")
    if not api_key:
        raise ValueError("linear_api_key is required")

    # These entities are stable and exposed by the API
    tables = ["issues", "projects", "teams", "users", "comments"]

    # Basic type mapping
    default_columns = {
        "id": "STRING",
        "createdAt": "UTC_DATETIME",
        "updatedAt": "UTC_DATETIME",
        "archivedAt": "UTC_DATETIME"
    }

    schemas = []
    for table in tables:
        schemas.append({
            "table": table,
            "primary_key": ["id"],
            "columns": default_columns
        })

    return schemas


def update(configuration: Dict[str, Any], state: Dict[str, Any]):
    """
    Define the update function, which is a required function, and is called by Fivetran during each sync.
    See the technical reference documentation for more details on the update function
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    Args:
        configuration: A dictionary containing connection details
        state: A dictionary containing state information from previous runs
        The state dictionary is empty for the first sync or for any full re-sync
    """
    api_key = configuration.get("linear_api_key")
    if not api_key:
        raise ValueError("linear_api_key is required")

    entities = ["issues", "projects", "teams", "users"]
    page_size = __PAGE_SIZE

    merged_state: Dict[str, Any] = dict(state or {})

    ies = IsolatedEndpointSync(logger=log)

    for entity in entities:
        last_updated = (merged_state.get(entity) or {}).get("last_updated")
        log.info(f"[IES] {entity}: starting (last_updated={last_updated})")

        def sync_one_entity(entity=entity, last_updated=last_updated):
            new_max_updated = sync_entity(api_key, entity, last_updated, page_size)
            merged_state[entity] = {
                "last_updated": new_max_updated
                                or last_updated
                                or datetime.now(timezone.utc).isoformat()
            }
            log.info(f"[IES] {entity}: done (new_last_updated={merged_state[entity]['last_updated']})")

        ies.run(entity, sync_one_entity)

    log.info(f"[IES] emitting merged checkpoint for {list(merged_state.keys())}")
    op.checkpoint(merged_state)

    ies.raise_if_failures()


def sync_entity(api_key: str, entity: str, last_updated: Optional[str], page_size: int):
    """
    Sync a specific Linear entity type.
    Args: api_key: Linear API key
          entity: Entity type to sync (issues, projects, teams, users, comments)
          last_updated: Last updated timestamp for incremental sync
          page_size: Number of records to fetch per page
    Returns: New state with updated last_updated timestamp
    """
    valid_entities = {"issues", "projects", "teams", "users", "comments"}
    if entity not in valid_entities:
        raise ValueError(f"Unsupported Linear entity '{entity}'. Allowed: {', '.join(sorted(valid_entities))}")

    cursor: Optional[str] = None
    has_more = True
    new_max_updated = last_updated  # default to previous watermark

    while has_more:
        updated_filter = (
            f'filter: {{ updatedAt: {{ gt: "{last_updated}" }} }}' if last_updated else ""
        )

        query = f"""
        query {entity.capitalize()}List($cursor: String, $count: Int) {{
          {entity}(
            first: $count,
            after: $cursor,
            orderBy: updatedAt,
            {updated_filter}
          ) {{
            nodes {{
              id
              createdAt
              updatedAt
              archivedAt
            }}
            pageInfo {{
              hasNextPage
              endCursor
            }}
          }}
        }}
        """

        variables = {"count": page_size, "cursor": cursor}
        data = graphql_query(api_key, query, variables)

        entity_data = data.get(entity, {}) or {}
        nodes = entity_data.get("nodes", []) or []
        page_info = entity_data.get("pageInfo", {}) or {}

        for record in nodes:
            record["updatedAt"] = to_iso(record.get("updatedAt"))
            record["createdAt"] = to_iso(record.get("createdAt"))
            record["archivedAt"] = to_iso(record.get("archivedAt"))

            if record.get("archivedAt"):
                op.delete(entity, {"id": record["id"]})
            else:
                op.upsert(entity, record)

            upd = record.get("updatedAt")
            if upd and (new_max_updated is None or upd > new_max_updated):
                new_max_updated = upd

        has_more = bool(page_info.get("hasNextPage"))
        cursor = page_info.get("endCursor")

    return new_max_updated


# ------------------- Isolated Endpoint Sync Utility -------------------

class IsolatedEndpointSync:
    """
    Mirrors Fivetran's Java IsolatedEndpointSync semantics:
    runs each endpoint in isolation, captures and reports failures.
    """
    def __init__(self, logger=log):
        self.failures: Dict[str, Exception] = {}
        self.logger = logger

    def run(self, endpoint: str, func, *args, **kwargs) -> None:
        """
        Runs a single endpoint sync and captures its exceptions.
        'func' is a regular function (not a generator) that performs sync work
        and emits operations via op.upsert/op.delete/op.checkpoint.
        """
        try:
            func(*args, **kwargs)
        except Exception as e:
            self.logger.severe(f"{endpoint} sync failed: {e}")
            self.failures[endpoint] = e

    def has_failures(self) -> bool:
        return bool(self.failures)

    def raise_if_failures(self) -> None:
        if self.failures:
            msgs = "; ".join(f"{k}: {str(v)}" for k, v in self.failures.items())
            raise Exception(f"IsolatedEndpointSync detected failed endpoints -> {msgs}")


# This creates the connector object that will use the update and schema functions defined in this connector.py file.
connector = Connector(update=update)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the "fivetran debug" command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from your IDE.
    connector.debug()



