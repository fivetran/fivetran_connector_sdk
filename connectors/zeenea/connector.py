"""
Zeenea connector for Fivetran Connector SDK.
Fetches data catalog metadata from Zeenea API.
"""

import requests
from fivetran_connector_sdk import connector, config, state, records, log, schema

CONFIG = config.Config(
    base_url=config.StringField(description="Zeenea API base URL"),
    api_token=config.SecretField(description="Zeenea API token")
)

SCHEMA = schema.Schema(
    name="zeenea_assets",
    columns={
        "id": schema.StringColumn(),
        "name": schema.StringColumn(),
        "type": schema.StringColumn(),
        "updatedAt": schema.StringColumn(),
        "metadata": schema.JSONColumn(),
    }
)

@connector(
    name="ZeeneaConnector",
    version="0.1.0",
    config=CONFIG,
    schema=SCHEMA,
)
def run_connector(ctx: state.Context):
    headers = {"Authorization": f"Bearer {ctx.config.api_token}"}
    response = requests.get(f"{ctx.config.base_url}/api/assets", headers=headers)
    response.raise_for_status()

    for asset in response.json().get("items", []):
        records.write("zeenea_assets", asset)

    return ctx.update_state({"last_sync": "now"})
