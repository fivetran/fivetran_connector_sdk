from fivetran_connector_sdk import Connector, Operations as op, Logging as log
import requests
import datetime as dt

USERFLOW_VERSION = "2020-01-03"
MAX_LIMIT = 100


def _headers(api_key: str):
    return {
        "Authorization": f"Bearer {api_key}",
        "Userflow-Version": USERFLOW_VERSION,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def _timestamp():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def _table_state(state, table):
    """Create or return per-table state."""
    if "bookmarks" not in state:
        state["bookmarks"] = {}
    if table not in state["bookmarks"]:
        state["bookmarks"][table] = {}
    return state["bookmarks"][table]


def schema(configuration):
    """Define the users table schema."""
    return [
        {
            "table": "users",
            "primary_key": ["id"],
            "column": {
                "id": "STRING",
                "email": "STRING",
                "name": "STRING",
                "created_at": "UTC_DATETIME",
                "signed_up_at": "UTC_DATETIME",
                "raw": "JSON",
                "_synced_at": "UTC_DATETIME",
            },
        }
    ]


def _fetch_users(base_url, headers, limit, starting_after=None):
    """Helper: Fetch a single page of users."""
    params = {"limit": limit}
    if starting_after:
        params["starting_after"] = starting_after

    url = f"{base_url.rstrip('/')}/users"
    resp = requests.get(url, headers=headers, params=params, timeout=60)
    resp.raise_for_status()
    payload = resp.json()

    return (
        payload.get("data", []),
        payload.get("has_more"),
        payload.get("next_page_url"),
    )


def update(configuration, state):
    """Incrementally sync Userflow users."""
    base_url = configuration.get("base_url", "https://api.userflow.com")
    api_key = configuration["userflow_api_key"]
    limit = int(configuration.get("page_size", MAX_LIMIT))
    headers = _headers(api_key)

    tb_state = _table_state(state, "users")
    starting_after = tb_state.get("last_seen_id")

    total = 0
    has_more = True
    next_page_url = None

    while has_more:
        users, has_more, next_page_url = _fetch_users(
            base_url, headers, limit, starting_after
        )
        if not users:
            break

        for user in users:
            attrs = user.get("attributes", {})
            op.upsert(
                table="users",
                data={
                    "id": user.get("id"),
                    "email": attrs.get("email"),
                    "name": attrs.get("name"),
                    "created_at": user.get("created_at"),
                    "signed_up_at": attrs.get("signed_up_at"),
                    "raw": user,
                    "_synced_at": _timestamp(),
                },
            )
            # Incremental bookmark (keep the last ID seen)
            tb_state["last_seen_id"] = user.get("id")
            total += 1

        log.info(f"Fetched {len(users)} users (total {total}) so far...")

        # If using `next_page_url`, override base_url/params for next loop
        if next_page_url:
            base_url = base_url.rstrip("/") + next_page_url

    op.checkpoint(state)
    log.info(f"✅ Incremental sync completed: {total} users fetched")


connector = Connector(update=update, schema=schema)
