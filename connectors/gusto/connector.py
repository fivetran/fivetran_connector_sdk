"""
Gusto connector for Fivetran Connector SDK.
Fetches HR and payroll data from Gusto API.
"""

import requests
from fivetran_connector_sdk import connector, config, state, records, log, schema

CONFIG = config.Config(
    base_url=config.StringField(default="https://api.gusto.com"),
    access_token=config.SecretField(description="OAuth2 access token")
)

SCHEMA = schema.Schema(
    name="gusto_employees",
    columns={
        "id": schema.StringColumn(),
        "first_name": schema.StringColumn(),
        "last_name": schema.StringColumn(),
        "email": schema.StringColumn(),
        "status": schema.StringColumn(),
    }
)

@connector(
    name="GustoConnector",
    version="0.1.0",
    config=CONFIG,
    schema=SCHEMA,
)
def run_connector(ctx: state.Context):
    headers = {"Authorization": f"Bearer {ctx.config.access_token}"}
    response = requests.get(f"{ctx.config.base_url}/v1/employees", headers=headers)
    response.raise_for_status()

    for emp in response.json():
        records.write("gusto_employees", emp)

    return ctx.update_state({"last_sync": "now"})
