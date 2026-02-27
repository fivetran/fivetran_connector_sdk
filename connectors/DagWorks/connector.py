"""
DagWorks connector for Fivetran Connector SDK.
Fetches pipeline metadata and run status from DagWorks API.
"""

import requests
from fivetran_connector_sdk import connector, config, state, records, log, schema

CONFIG = config.Config(
    base_url=config.StringField(description="DagWorks API base URL"),
    api_key=config.SecretField()
)

SCHEMA = schema.Schema(
    name="dagworks_runs",
    columns={
        "id": schema.StringColumn(),
        "dag_id": schema.StringColumn(),
        "status": schema.StringColumn(),
        "start_time": schema.StringColumn(),
        "end_time": schema.StringColumn(),
    }
)

@connector(
    name="DagWorksConnector",
    version="0.1.0",
    config=CONFIG,
    schema=SCHEMA,
)
def run_connector(ctx: state.Context):
    headers = {"Authorization": f"Bearer {ctx.config.api_key}"}
    response = requests.get(f"{ctx.config.base_url}/runs", headers=headers)
    response.raise_for_status()

    for run in response.json().get("runs", []):
        records.write("dagworks_runs", run)

    return ctx.update_state({"last_sync": "now"})
