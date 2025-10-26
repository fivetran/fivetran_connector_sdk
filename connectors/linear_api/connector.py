"""
Fivetran Connector SDK – Linear API Source
Supports incremental sync (updatedAt), delete capture (archived), and dynamic schema discovery.
"""

import requests
from typing import Dict, List, Any, Optional
from datetime import datetime, timezone
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Operations as op
from fivetran_connector_sdk import Logging as log

LINEAR_API_URL = "https://api.linear.app/graphql"

def graphql_query(api_key, query, variables=None):
    """
    Perform a GraphQL query against the Linear API.
    Args: param api_key: Linear API key
          param query: GraphQL query string
          param variables: Optional variables for the query
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
    Args: param dt: datetime string
    Returns: ISO 8601 formatted datetime string in UTC or None
    """
    if not dt:
        return None
    try:
        # Ensure UTC ISO 8601
        return datetime.fromisoformat(dt.replace("Z", "+00:00")).astimezone(timezone.utc).isoformat()
    except Exception:
        return dt


# Define the schema function, which lets you configure the schema your connector delivers.
# See the technical reference documentation for more details on the schema function:
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
# The schema function takes one parameter:
# - configuration: a dictionary that holds the configuration settings for the connector.
def schema(configuration: Dict[str, Any]) -> List[Dict[str, Any]]:
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


# Define the update function, which is a required function, and is called by Fivetran during each sync.
# See the technical reference documentation for more details on the update function
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
# The function takes two parameters:
# - configuration: a dictionary that contains any secrets or payloads you configure when deploying the connector
# - state: a dictionary that contains whatever state you have chosen to checkpoint during the prior sync
# The state dictionary is empty for the first sync or for any full re-sync.
def update(configuration: Dict[str, Any], state: Dict[str, Any]):
    """
    Isolated endpoint sync for Linear.
    - Runs each entity independently (errors don't stop others).
    - Streams row operations.
    - Accumulates per-entity watermarks.
    - Emits ONE merged checkpoint at the end (so nothing gets lost).
    """
    api_key = configuration.get("linear_api_key")
    if not api_key:
        raise ValueError("linear_api_key is required")

    entities = ["issues", "projects", "teams", "users"]
    page_size = 250

    # Start from current state and update it as we go.
    merged_state: Dict[str, Any] = dict(state or {})
    failures: Dict[str, Exception] = {}

    for entity in entities:
        last_updated = (merged_state.get(entity) or {}).get("last_updated")
        log.info(f"[IES] {entity}: starting (last_updated={last_updated})")

        try:
            # Get a generator for ops AND the new watermark value
            gen, new_max_updated = sync_entity(api_key, entity, last_updated, page_size)

            # Stream row operations (upserts/deletes) to Fivetran
            for op_res in gen:
                yield op_res

            # Update in-memory state for this entity (even if no rows were returned)
            merged_state[entity] = {
                "last_updated": new_max_updated or last_updated or datetime.now(timezone.utc).isoformat()
            }
            log.info(f"[IES] {entity}: done (new_last_updated={merged_state[entity]['last_updated']})")

        except Exception as e:
            failures[entity] = e
            log.severe(f"[IES] {entity}: failed with error: {e}")

    # Emit ONE final checkpoint that includes every entity's watermark
    log.info(f"[IES] emitting merged checkpoint for {list(merged_state.keys())}")

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
    yield op.checkpoint(merged_state)

    # After we’ve safely checkpointed what we could, surface any failures
    if failures:
        # Raise a combined error so it shows up clearly in logs/tests
        joined = "; ".join(f"{k}: {v}" for k, v in failures.items())
        raise Exception(f"Isolated endpoints failed -> {joined}")


def sync_entity(api_key: str, entity: str, last_updated: Optional[str], page_size: int):
    """
    Sync a specific Linear entity type.
    Args: param api_key: Linear API key
          param entity: Entity type to sync (issues, projects, teams, users, comments)
          param last_updated: Last updated timestamp for incremental sync
          param page_size: Number of records to fetch per page
    Returns: New state with updated last_updated timestamp
    """
    valid_entities = {"issues", "projects", "teams", "users", "comments"}
    if entity not in valid_entities:
        raise ValueError(f"Unsupported Linear entity '{entity}'. Allowed: {', '.join(sorted(valid_entities))}")

    cursor: Optional[str] = None
    has_more = True
    new_max_updated = last_updated  # default to previous watermark

    def _ops_gen():
        nonlocal cursor, has_more, new_max_updated

        while has_more:
            # Inline the incremental filter only when we have a previous watermark
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

            # Stream row ops
            for record in nodes:
                # Normalize timestamps to UTC ISO
                record["updatedAt"] = to_iso(record.get("updatedAt"))
                record["createdAt"] = to_iso(record.get("createdAt"))
                record["archivedAt"] = to_iso(record.get("archivedAt"))

                # Delete capture via archivedAt
                if record.get("archivedAt"):
                    yield op.delete(entity, {"id": record["id"]})
                else:
                    yield op.upsert(entity, record)

                # Track high-watermark
                upd = record.get("updatedAt")
                if upd and (new_max_updated is None or upd > new_max_updated):
                    new_max_updated = upd

            has_more = bool(page_info.get("hasNextPage"))
            cursor = page_info.get("endCursor")

    # Return the generator and the new watermark (caller will yield ops and checkpoint once)
    return _ops_gen(), new_max_updated


# def sync_entity(api_key: str, entity: str, last_updated: Optional[str], page_size: int):
#     """
#     Sync a specific Linear entity type.
#     Args: param api_key: Linear API key
#           param entity: Entity type to sync (issues, projects, teams, users, comments)
#           param last_updated: Last updated timestamp for incremental sync
#           param page_size: Number of records to fetch per page
#     Returns: New state with updated last_updated timestamp
#     """
#     valid_entities = {"issues", "projects", "teams", "users", "comments"}
#     if entity not in valid_entities:
#         raise ValueError(f"Unsupported Linear entity '{entity}'.")
#
#     has_more = True
#     cursor = None
#     new_max_updated = last_updated
#
#     while has_more:
#         # inline incremental filter (avoids DateTimeOrDuration variable type errors)
#         updated_filter = (
#             f'filter: {{ updatedAt: {{ gt: "{last_updated}" }} }}' if last_updated else ""
#         )
#
#         query = f"""
#         query {entity.capitalize()}List($cursor: String, $count: Int) {{
#           {entity}(
#             first: $count,
#             after: $cursor,
#             orderBy: updatedAt,
#             {updated_filter}
#           ) {{
#             nodes {{
#               id
#               createdAt
#               updatedAt
#               archivedAt
#             }}
#             pageInfo {{
#               hasNextPage
#               endCursor
#             }}
#           }}
#         }}
#         """
#
#         variables = {"count": page_size, "cursor": cursor}
#         data = graphql_query(api_key, query, variables)
#
#         entity_data = data.get(entity, {})
#         nodes = entity_data.get("nodes", [])
#         page_info = entity_data.get("pageInfo", {})
#
#         for record in nodes:
#             record["updatedAt"] = to_iso(record.get("updatedAt"))
#             record["createdAt"] = to_iso(record.get("createdAt"))
#             record["archivedAt"] = to_iso(record.get("archivedAt"))
#
#             # delete capture via archivedAt timestamp
#             if record.get("archivedAt"):
#                 yield op.delete(entity, {"id": record["id"]})
#             else:
#                 yield op.upsert(entity, record)
#
#             upd = record.get("updatedAt")
#             if upd and (new_max_updated is None or upd > new_max_updated):
#                 new_max_updated = upd
#
#         has_more = page_info.get("hasNextPage", False)
#         cursor = page_info.get("endCursor")
#
#     # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
#     # from the correct position in case of next sync or interruptions.
#     # Learn more about how and where to checkpoint by reading our best practices documentation
#     # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
#     yield op.checkpoint({
#         entity: {"last_updated": new_max_updated or datetime.now(timezone.utc).isoformat()}
#     })



# def sync_entity(api_key: str, entity: str, last_updated: Optional[str], page_size: int):
#     """
#     Sync a specific Linear entity type.
#     Args: param api_key: Linear API key
#           param entity: Entity type to sync (issues, projects, teams, users, comments)
#           param last_updated: Last updated timestamp for incremental sync
#           param page_size: Number of records to fetch per page
#     Returns: New state with updated last_updated timestamp
#     """
#     valid_entities = {"issues", "projects", "teams", "users", "comments"}
#     if entity not in valid_entities:
#         raise ValueError(f"Unsupported Linear entity '{entity}'.")
#
#     has_more = True
#     cursor = None
#     new_max_updated = last_updated
#
#     while has_more:
#         # ✅ Build inline updatedAt filter only if we have a checkpoint
#         updated_filter = (
#             f'filter: {{ updatedAt: {{ gt: "{last_updated}" }} }}' if last_updated else ""
#         )
#
#         query = f"""
#         query {entity.capitalize()}List($cursor: String, $count: Int) {{
#           {entity}(
#             first: $count,
#             after: $cursor,
#             orderBy: updatedAt,
#             {updated_filter}
#           ) {{
#             nodes {{
#               id
#               createdAt
#               updatedAt
#               archivedAt
#             }}
#             pageInfo {{
#               hasNextPage
#               endCursor
#             }}
#           }}
#         }}
#         """
#
#         variables = {"count": page_size, "cursor": cursor}
#
#         data = graphql_query(api_key, query, variables)
#         entity_data = data.get(entity, {})
#         nodes = entity_data.get("nodes", [])
#         page_info = entity_data.get("pageInfo", {})
#
#         for record in nodes:
#             record["updatedAt"] = to_iso(record.get("updatedAt"))
#             record["createdAt"] = to_iso(record.get("createdAt"))
#             record["archivedAt"] = to_iso(record.get("archivedAt"))
#
#             # delete capture
#             if record.get("archivedAt"):
#                 yield op.delete(entity, {"id": record["id"]})
#             else:
#                 # The 'upsert' operation is used to insert or update data in the destination table.
#                 # The op.upsert method is called with two arguments:
#                 # - The first argument is the name of the table to upsert the data into.
#                 # - The second argument is a dictionary containing the data to be upserted
#                 yield op.upsert(entity, record)
#
#             upd = record.get("updatedAt")
#             if upd and (new_max_updated is None or upd > new_max_updated):
#                 new_max_updated = upd
#
#         has_more = page_info.get("hasNextPage", False)
#         cursor = page_info.get("endCursor")
#
#     return {entity: {"last_updated": new_max_updated or datetime.now(timezone.utc).isoformat()}}


# ------------------- Isolated Endpoint Sync Utility -------------------

class IsolatedEndpointSync:
    """
    Mirrors Fivetran's Java IsolatedEndpointSync semantics:
    runs each endpoint in isolation, captures and reports failures.
    """
    def __init__(self, logger=log):
        self.failures: dict[str, Exception] = {}
        self.logger = logger

    def run(self, endpoint: str, func, *args, **kwargs):
        """Runs a single endpoint sync and captures its exceptions."""
        try:
            for op_result in func(*args, **kwargs):
                yield op_result
        except Exception as e:
            self.logger.severe(f"{endpoint} sync failed: {e}")
            self.failures[endpoint] = e

    def has_failures(self) -> bool:
        return bool(self.failures)

    def raise_if_failures(self):
        if self.failures:
            msgs = "; ".join(f"{k}: {str(v)}" for k, v in self.failures.items())
            raise Exception(f"IsolatedEndpointSync detected failed endpoints -> {msgs}")



connector = Connector(update=update, schema=schema)

# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    # Open the configuration.json file and load its contents
    with open("configuration.json", "r") as f:
        configuration = json.load(f)

    # Test the connector locally
    connector.debug(configuration=configuration)

# Fivetran debug results
# Operation       | Calls
# ----------------+------------
# Upserts         | 55
# Updates         | 0
# Deletes         | 0
# Truncates       | 0
# SchemaChanges   | 5
# Checkpoints     | 1



