"""
Swift connector for Fivetran Connector SDK.
Fetches payment data via SWIFT API (mock endpoint for base setup).
"""

import requests
from fivetran_connector_sdk import connector, config, state, records, log, schema

CONFIG = config.Config(
    base_url=config.StringField(description="SWIFT API base URL"),
    api_key=config.SecretField(description="SWIFT API key")
)

SCHEMA = schema.Schema(
    name="swift_transactions",
    columns={
        "transaction_id": schema.StringColumn(),
        "amount": schema.StringColumn(),
        "currency": schema.StringColumn(),
        "timestamp": schema.StringColumn(),
    }
)

@connector(
    name="SwiftConnector",
    version="0.1.0",
    config=CONFIG,
    schema=SCHEMA,
)
def run_connector(ctx: state.Context):
    headers = {"Authorization": f"Bearer {ctx.config.api_key}"}
    response = requests.get(f"{ctx.config.base_url}/transactions", headers=headers)
    response.raise_for_status()

    for tx in response.json().get("data", []):
        records.write("swift_transactions", tx)

    return ctx.update_state({"last_sync": "now"})
