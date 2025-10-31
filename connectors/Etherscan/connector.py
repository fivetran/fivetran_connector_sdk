"""
Etherscan connector for Fivetran Connector SDK
Fetches blockchain data from the Etherscan API.
Docs: https://fivetran.com/docs/connector-sdk
"""

import requests
from fivetran_connector_sdk import connector, config, state, records, log, schema

CONFIG = config.Config(
    api_key=config.SecretField(description="Etherscan API Key"),
    base_url=config.StringField(default="https://api.etherscan.io/api"),
    start_block=config.StringField(description="Starting block number")
)

SCHEMA = schema.Schema(
    name="etherscan_transactions",
    columns={
        "blockNumber": schema.StringColumn(),
        "timeStamp": schema.StringColumn(),
        "hash": schema.StringColumn(),
        "from": schema.StringColumn(),
        "to": schema.StringColumn(),
        "value": schema.StringColumn(),
    }
)

@connector(
    name="EtherscanConnector",
    version="0.1.0",
    config=CONFIG,
    schema=SCHEMA,
)
def run_connector(ctx: state.Context):
    log.info("Starting Etherscan sync...")

    params = {
        "module": "account",
        "action": "txlist",
        "address": "<WALLET_ADDRESS>",
        "startblock": ctx.config.start_block,
        "endblock": "latest",
        "sort": "asc",
        "apikey": ctx.config.api_key
    }

    response = requests.get(ctx.config.base_url, params=params)
    response.raise_for_status()
    data = response.json().get("result", [])

    for tx in data:
        records.write("etherscan_transactions", tx)

    log.info(f"Fetched {len(data)} transactions.")
    return ctx.update_state({"last_block": data[-1]["blockNumber"] if data else ctx.config.start_block})
