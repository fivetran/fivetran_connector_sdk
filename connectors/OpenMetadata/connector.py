"""
OpenMetadata connector for Fivetran Connector SDK.
Fetches metadata entities via OpenMetadata REST API.
"""

import requests
from fivetran_connector_sdk import connector, config, state, records, log, schema

CONFIG = config.Config(
    base_url=config.StringField(description="OpenMetadata API base URL"),
    api_token=config.SecretField(description="OpenMetadata auth token")
)

SCHEMA = schema.Schema(
    name="openmetadata_entities",
    columns={
        "id": schema.StringColumn(),
        "name": schema.StringColumn(),
        "type": schema.StringColumn(),
        "updatedAt": schema.StringColumn(),
        "data": schema.JSONColumn(),
    }
)

@connector(
    name="OpenMetadataConnector",
    version="0.1.0",
    config=CONFIG,
    schema=SCHEMA,
)
def run_connector(ctx: state.Context):
    headers = {"Authorization": f"Bearer {ctx.config.api_token}"}
    url = f"{ctx.config.base_url}/api/v1/tables"

    response = requests.get(url, headers=headers)
    response.raise_for_status()
    items = response.json().get("data", [])

    for item in items:
        records.write("openmetadata_entities", item)

    return ctx.update_state({"last_sync": "now"})
