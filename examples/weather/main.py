
from fivetran_custom_sdk import Connector, upsert, checkpoint
from datetime import datetime
import requests as rq


def schema(configuration: dict):
    return [
            {
                "table": "period",
                "primary_key": ["startTime"],
                "columns": {
                    "name": "STRING",
                    "startTime": "UTC_DATETIME",
                    "endTime": "UTC_DATETIME",
                    "temperature": "INT",
                },
            }
    ]


def str2dt(incoming: str) -> datetime:
    return datetime.strptime(incoming, "%Y-%m-%dT%H:%M:%S%z")


def update(configuration: dict, state: dict):
    cursor = state['startTime'] if 'startTime' in state else '0001-01-01T00:00:00Z'

    # Get weather forecast for Myrtle Beach, SC from National Weather Service
    response = rq.get("https://api.weather.gov/gridpoints/ILM/58,40/forecast")

    data = response.json()
    periods = data['properties']['periods']
    for period in periods:
        # Skip data points we already synced
        if str2dt(period['startTime']) < str2dt(cursor):
            continue

        yield upsert("period", data={
                "name": period["name"],
                "startTime": period["startTime"],
                "endTime": period["endTime"],
                "temperature": period["temperature"]
        })

        cursor = period['endTime']

    # save cursor for next sync
    yield checkpoint({
        "startTime": cursor
    })


connector = Connector(update=update, schema=schema)


# The following code block optional, to be able to run the connector code in an IDE easily
if __name__ == "__main__":
    connector.debug()

