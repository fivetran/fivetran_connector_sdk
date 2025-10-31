"""
Fivetran Connector SDK – Databricks Unity Catalog
Pulls catalog / schema / table / column metadata from the
Databricks Unity Catalog REST API v2.1
"""

import time
from typing import Any, Dict, List, Optional
import requests

from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Operations as op
from fivetran_connector_sdk import Logging as log


class DatabricksClient:
    def __init__(self, api_base: str, token: str, timeout: int = 30):
        self.base = api_base.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "User-Agent": "fivetran-connector-sdk-unitycatalog/1.0",
            }
        )

    def request(self, method: str, path: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        url = f"{self.base}{path}"
        for attempt in range(1, 6):
            r = self.session.request(method, url, timeout=self.timeout, params=params or {})
            if r.status_code == 429:
                sleep_for = int(r.headers.get("Retry-After", 2))
                log.info(f"Rate limited – sleeping {sleep_for}s (attempt {attempt})")
                time.sleep(sleep_for)
                continue
            if r.status_code >= 500:
                log.warning(f"{r.status_code} from {url}; retrying (attempt {attempt})")
                time.sleep(2**attempt)
                continue
            r.raise_for_status()
            return r.json()
        raise RuntimeError(f"Failed after retries: {url}")

    def list_catalogs(self) -> List[Dict[str, Any]]:
        data = self.request("GET", "/api/2.1/unity-catalog/catalogs")
        catalogs = data.get("catalogs", [])
        log.info(f"Fetched {len(catalogs)} catalogs: {[c['name'] for c in catalogs]}")
        return catalogs

    def list_schemas(self, catalog_name: str) -> List[Dict[str, Any]]:
        try:
            data = self.request("GET", f"/api/2.1/unity-catalog/catalogs/{catalog_name}/schemas")
            return data.get("schemas", [])
        except requests.HTTPError as e:
            if e.response.status_code == 404:
                log.warning(f"Catalog '{catalog_name}' not accessible – skipping")
                return []
            raise

    def list_tables(self, catalog: str, schema: str) -> List[Dict[str, Any]]:
        results = []
        token = None
        while True:
            params = {"max_results": 1000}
            if token:
                params["page_token"] = token
            data = self.request("GET", "/api/2.1/unity-catalog/tables", params=params)
            items = [
                t
                for t in data.get("tables", [])
                if t.get("catalog_name") == catalog and t.get("schema_name") == schema
            ]
            results.extend(items)
            token = data.get("next_page_token")
            if not token:
                break
        return results

    def list_columns(self, full_table_name: str) -> List[Dict[str, Any]]:
        data = self.request("GET", f"/api/2.1/unity-catalog/tables/{full_table_name}")
        return data.get("columns", [])


TABLES = {
    "unity_catalogs": {
        "pk": ["name"],
        "updated": "updated_at",
        "schema": {
            "name": "STRING",
            "owner": "STRING",
            "comment": "STRING",
            "catalog_type": "STRING",
            "created_at": "UTC_DATETIME",
            "updated_at": "UTC_DATETIME",
            "raw": "JSON",
        },
    },
    "unity_schemas": {
        "pk": ["catalog_name", "name"],
        "updated": "updated_at",
        "schema": {
            "catalog_name": "STRING",
            "name": "STRING",
            "owner": "STRING",
            "comment": "STRING",
            "created_at": "UTC_DATETIME",
            "updated_at": "UTC_DATETIME",
            "raw": "JSON",
        },
    },
    "unity_tables": {
        "pk": ["full_name"],
        "updated": "updated_at",
        "schema": {
            "full_name": "STRING",
            "catalog_name": "STRING",
            "schema_name": "STRING",
            "name": "STRING",
            "table_type": "STRING",
            "data_source_format": "STRING",
            "owner": "STRING",
            "comment": "STRING",
            "created_at": "UTC_DATETIME",
            "updated_at": "UTC_DATETIME",
            "raw": "JSON",
        },
    },
    "unity_columns": {
        "pk": ["table_full_name", "name"],
        "schema": {
            "table_full_name": "STRING",
            "name": "STRING",
            "type_text": "STRING",
            "type_name": "STRING",
            "position": "INTEGER",
            "nullable": "BOOLEAN",
            "comment": "STRING",
            "raw": "JSON",
        },
    },
}


def schema(configuration: Dict[str, Any]):
    declared = []
    for t, meta in TABLES.items():
        declared.append(
            {
                "table": t,
                "primary_key": meta["pk"],
                "column": meta["schema"],
            }
        )
    return declared


# -----------------------------
# Helpers for incremental sync
# -----------------------------
def get_last_cursor(state: Dict[str, Any], table: str) -> Optional[str]:
    return (state or {}).get("cursors", {}).get(table)


def set_last_cursor(state: Dict[str, Any], table: str, value: Optional[str]) -> Dict[str, Any]:
    new_state = dict(state or {})
    cursors = dict(new_state.get("cursors", {}))
    if value:
        cursors[table] = value
    new_state["cursors"] = cursors
    return new_state


def update(configuration: Dict[str, Any], state: Dict[str, Any]):
    base = configuration["api_base"]
    token = configuration["access_token"]
    client = DatabricksClient(api_base=base, token=token)

    # --- 1. Catalogs ---
    all_catalogs = client.list_catalogs()

    # keep only user-managed catalogs
    catalogs = [c for c in all_catalogs if c.get("catalog_type") == "MANAGED_CATALOG"]
    max_seen = get_last_cursor(state, "unity_catalogs")

    for c in catalogs:
        row = {
            "name": c["name"],
            "owner": c.get("owner"),
            "comment": c.get("comment"),
            "catalog_type": c.get("catalog_type"),
            "created_at": c.get("created_at"),
            "updated_at": c.get("updated_at"),
            "raw": c,
        }
        op.upsert(table="unity_catalogs", data=row)
        if c.get("updated_at") and (not max_seen or c["updated_at"] > max_seen):
            max_seen = c["updated_at"]

    state = set_last_cursor(state, "unity_catalogs", max_seen)
    op.checkpoint(state)

    # --- 2. Schemas ---
    for c in catalogs:
        schemas = client.list_schemas(str(c["name"]))
        for s in schemas:
            row = {
                "catalog_name": s["catalog_name"],
                "name": s["name"],
                "owner": s.get("owner"),
                "comment": s.get("comment"),
                "created_at": s.get("created_at"),
                "updated_at": s.get("updated_at"),
                "raw": s,
            }
            op.upsert(table="unity_schemas", data=row)
    op.checkpoint(state)

    # --- 3. Tables and Columns ---
    for c in catalogs:
        for s in client.list_schemas(str(c["name"])):
            tables = client.list_tables(c["name"], s["name"])
            for t in tables:
                row = {
                    "full_name": t["full_name"],
                    "catalog_name": t["catalog_name"],
                    "schema_name": t["schema_name"],
                    "name": t["name"],
                    "table_type": t.get("table_type"),
                    "data_source_format": t.get("data_source_format"),
                    "owner": t.get("owner"),
                    "comment": t.get("comment"),
                    "created_at": t.get("created_at"),
                    "updated_at": t.get("updated_at"),
                    "raw": t,
                }
                op.upsert(table="unity_tables", data=row)

                # columns
                cols = client.list_columns(t["full_name"])
                for col in cols:
                    op.upsert(
                        table="unity_columns",
                        data={
                            "table_full_name": t["full_name"],
                            "name": col["name"],
                            "type_text": col.get("type_text"),
                            "type_name": col.get("type_name"),
                            "position": col.get("position"),
                            "nullable": col.get("nullable"),
                            "comment": col.get("comment"),
                            "raw": col,
                        },
                    )

    op.checkpoint(state)
    log.info("Unity Catalog sync complete.")


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents
    with open("configuration.json", "r") as f:
        configuration = json.load(f)

    # Test the connector locally
    connector.debug(configuration=configuration)


# Fivetran debug results:
# Operation       | Calls
# ----------------+------------
# Upserts         | 73
# Updates         | 0
# Deletes         | 0
# Truncates       | 0
# SchemaChanges   | 4
# Checkpoints     | 3
