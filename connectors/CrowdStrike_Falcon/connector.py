"""
CrowdStrike Falcon connector for Fivetran Connector SDK.
Pulls endpoint detection and response data.
"""

import requests
from fivetran_connector_sdk import connector, config, state, records, log, schema

CONFIG = config.Config(
    base_url=config.StringField(default="https://api.crowdstrike.com"),
    client_id=config.SecretField(),
    client_secret=config.SecretField()
)

SCHEMA = schema.Schema(
    name="falcon_detections",
    columns={
        "id": schema.StringColumn(),
        "created_timestamp": schema.StringColumn(),
        "status": schema.StringColumn(),
        "severity": schema.StringColumn(),
        "behavior": schema.JSONColumn(),
    }
)

@connector(
    name="CrowdStrikeFalconConnector",
    version="0.1.0",
    config=CONFIG,
    schema=SCHEMA,
)
def run_connector(ctx: state.Context):
    # Normally you'd authenticate via OAuth2 first
    headers = {"Authorization": f"Bearer {ctx.config.client_secret}"}
    url = f"{ctx.config.base_url}/detects/queries/detects/v1"

    response = requests.get(url, headers=headers)
    response.raise_for_status()

    for item in response.json().get("resources", []):
        records.write("falcon_detections", {"id": item})

    return ctx.update_state({"last_sync": "now"})
