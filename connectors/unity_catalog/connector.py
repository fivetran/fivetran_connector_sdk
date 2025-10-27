"""
Fivetran Connector SDK – Databricks Unity Catalog
Pulls metadata about catalogs, schemas, tables, and columns via REST API v2.1
"""

import time
from typing import Any, Dict, List, Iterable, Optional
import requests

from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Operations as op
from fivetran_connector_sdk import Logging as log

# -----------------------------
# HTTP client
# -----------------------------

class DatabricksClient:
    def __init__(self, api_base: str, token: str, timeout: int = 30):
        self.base = api_base.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "User-Agent": "fivetran-connector-sdk-unitycatalog/1.0"
        })

    def _request(self, method: str, path: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        url = f"{self.base}{path}"
        for attempt in range(1, 6):
            r = self.session.request(method, url, timeout=self.timeout, params=params or {})
            if r.status_code == 429:
                sleep_for = int(r.headers.get("Retry-After", 2))
                log.info(f"Rate limited. Sleeping {sleep_for}s (attempt {attempt})")
                time.sleep(sleep_for)
                continue
            if r.status_code >= 500:
                time.sleep(2 ** attempt)
                continue
            r.raise_for_status()
            return r.json()
        raise RuntimeError(f"Failed after retries: {url}")

    def list_catalogs(self) -> List[Dict[str, Any]]:
        data = self._request("GET", "/api/2.1/unity-catalog/catalogs")
        return data.get("catalogs", [])

    def list_schemas(self, catalog: str) -> List[Dict[str, Any]]:
        data = self._request("GET", f"/api/2.1/unity-catalog/catalogs/{catalog}/schemas")
        return data.get("schemas", [])

    def list_tables(self, catalog: str, schema: str) -> List[Dict[str, Any]]:
        results = []
        token = None
        while True:
            params = {"max_results": 1000}
            if token:
                params["page_token"] = token
            data = self._request("GET", f"/api/2.1/unity-catalog/tables", params=params)
            items = data.get("tables", [])
            results.extend([t for t in items if t.get("catalog_name") == catalog and t.get("schema_name") == schema])
            token = data.get("next_page_token")
            if not token:
                break
        return results

    def list_columns(self, full_table_name: str) -> List[Dict[str, Any]]:
        # GET /api/2.1/unity-catalog/tables/{full_name}
        data = self._request("GET", f"/api/2.1/unity-catalog/tables/{full_table_name}")
        return data.get("columns", [])


# -----------------------------
# Tables metadata
# -----------------------------

TABLES = {
    "unity_catalogs": {
        "pk": ["name"],
        "updated": "updated_at",
        "schema": {
            "name": "STRING",
            "comment": "STRING",
            "owner": "STRING",
            "created_at": "UTC_DATETIME",
            "updated_at": "UTC_DATETIME",
            "raw": "JSON"
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
            "raw": "JSON"
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
            "comment": "STRING",
            "owner": "STRING",
            "created_at": "UTC_DATETIME",
            "updated_at": "UTC_DATETIME",
            "raw": "JSON"
        },
    },
    "unity_columns": {
        "pk": ["table_full_name", "name"],
        "updated": None,
        "schema": {
            "table_full_name": "STRING",
            "name": "STRING",
            "type_text": "STRING",
            "type_name": "STRING",
            "position": "INTEGER",
            "comment": "STRING",
            "nullable": "BOOLEAN",
            "raw": "JSON"
        },
    }
}

# -----------------------------
# schema()
# -----------------------------

def schema(configuration: Dict[str, Any]) -> List[Dict[str, Any]]:
    declared = []
    for t, meta in TABLES.items():
        declared.append({
            "table": t,
            "primary_key": meta["pk"],
            "column": meta["schema"]
        })
    return declared


# -----------------------------
# Helpers
# -----------------------------

def _get_last_cursor(state: Dict[str, Any], table: str) -> Optional[str]:
    return (state or {}).get("cursors", {}).get(table)

def _set_last_cursor(state: Dict[str, Any], table: str, value: Optional[str]) -> Dict[str, Any]:
    new_state = dict(state or {})
    cursors = dict(new_state.get("cursors", {}))
    if value:
        cursors[table] = value
    new_state["cursors"] = cursors
    return new_state

# -----------------------------
# update()
# -----------------------------

def update(configuration: Dict[str, Any], state: Dict[str, Any]):
    base = configuration["api_base"]
    token = configuration["access_token"]
    client = DatabricksClient(api_base=base, token=token)

    # 1. Catalogs
    catalogs = client.list_catalogs()
    max_seen = _get_last_cursor(state, "unity_catalogs")
    for c in catalogs:
        row = {
            "name": c["name"],
            "comment": c.get("comment"),
            "owner": c.get("owner"),
            "created_at": c.get("created_at"),
            "updated_at": c.get("updated_at"),
            "raw": c
        }
        op.Upsert(table="unity_catalogs", data=row)
        if c.get("updated_at") and (not max_seen or c["updated_at"] > max_seen):
            max_seen = c["updated_at"]
    state = _set_last_cursor(state, "unity_catalogs", max_seen)
    op.checkpoint(state)

    # 2. Schemas
    for c in catalogs:
        schemas = client.list_schemas(c["name"])
        for s in schemas:
            row = {
                "catalog_name": s["catalog_name"],
                "name": s["name"],
                "owner": s.get("owner"),
                "comment": s.get("comment"),
                "created_at": s.get("created_at"),
                "updated_at": s.get("updated_at"),
                "raw": s
            }
            op.Upsert(table="unity_schemas", data=row)
    op.checkpoint(state)

    # 3. Tables & Columns
    for c in catalogs:
        for s in client.list_schemas(c["name"]):
            tables = client.list_tables(c["name"], s["name"])
            for t in tables:
                row = {
                    "full_name": t["full_name"],
                    "catalog_name": t["catalog_name"],
                    "schema_name": t["schema_name"],
                    "name": t["name"],
                    "table_type": t.get("table_type"),
                    "data_source_format": t.get("data_source_format"),
                    "comment": t.get("comment"),
                    "owner": t.get("owner"),
                    "created_at": t.get("created_at"),
                    "updated_at": t.get("updated_at"),
                    "raw": t
                }
                op.Upsert(table="unity_tables", data=row)
                # Columns
                cols = client.list_columns(t["full_name"])
                for col in cols:
                    op.Upsert(table="unity_columns", data={
                        "table_full_name": t["full_name"],
                        "name": col["name"],
                        "type_text": col.get("type_text"),
                        "type_name": col.get("type_name"),
                        "position": col.get("position"),
                        "comment": col.get("comment"),
                        "nullable": col.get("nullable"),
                        "raw": col
                    })
    op.checkpoint(state)


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    connector.debug()
