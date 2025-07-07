from fivetran_connector_sdk import Connector, Logging as log, Operations as op
import json
import requests
from typing import Dict, Any


def schema(configuration: dict):
    # Only define the table name and primary key, let Fivetran infer the rest
    return [{"table": "tobacco_problem_reports", "primary_key": ["report_id"]}]


def flatten_record(record: Dict[str, Any], parent_key: str = "", sep: str = "_") -> Dict[str, Any]:
    # Flattens nested dicts and arrays for upsert
    items = {}
    for k, v in record.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.update(flatten_record(v, new_key, sep=sep))
        elif isinstance(v, list):
            for idx, item in enumerate(v):
                if isinstance(item, dict):
                    items.update(flatten_record(item, f"{new_key}{sep}{idx}", sep=sep))
                else:
                    items[f"{new_key}{sep}{idx}"] = item
        else:
            items[new_key] = v
    return items


def update(configuration: dict, state: dict = None):
    # Fetch first 10 records from the endpoint
    url = "https://api.fda.gov/tobacco/problem.json?limit=10"
    try:
        log.info(f"Requesting: {url}")
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        results = data.get("results", [])
        log.info(f"Fetched {len(results)} records from API.")
        count = 0
        for record in results:
            flat = flatten_record(record)
            # Upsert each record
            yield op.upsert("tobacco_problem_reports", flat)
            count += 1
        log.info(f"Upserted {count} records. Exiting after 10 records as requested.")
        # Checkpoint state (not needed for this run, but included for best practice)
        yield op.checkpoint(state={"last_run_count": count})
    except Exception as e:
        log.severe(f"Error fetching or processing data: {e}")
        raise


# Initialize the connector with the defined update and schema functions
connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    with open("/configuration.json", "r") as f:
        configuration = json.load(f)
    connector.debug(configuration=configuration)
