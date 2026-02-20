"""
Workday Extend connector for Fivetran Connector SDK.
Fetches data from Workday Extend APIs.
"""

import requests
from fivetran_connector_sdk import connector, config, state, records, log, schema

CONFIG = config.Config(
    base_url=config.StringField(description="Workday API base URL"),
    client_id=config.SecretField(),
    client_secret=config.SecretField(),
    tenant=config.StringField()
)

SCHEMA = schema.Schema(
    name="workday_objects",
    columns={
        "id": schema.StringColumn(),
        "type": schema.StringColumn(),
        "created": schema.StringColumn(),
        "data": schema.JSONColumn(),
    }
)

@connector(
    name="WorkdayExtendConnector",
    version="0.1.0",
    config=CONFIG,
    schema=SCHEMA,
)
def run_connector(ctx: state.Context):
    headers = {"Authorization": f"Bearer {ctx.config.client_secret}"}
    url = f"{ctx.config.base_url}/ccx/api/v1/{ctx.config.tenant}/workers"

    response = requests.get(url, headers=headers)
    response.raise_for_status()

    for item in response.json().get("data", []):
        records.write("workday_objects", item)

    return ctx.update_state({"last_sync": "now"})
