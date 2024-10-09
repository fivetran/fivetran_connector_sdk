# This is a simple example for how to work with the fivetran_connector_sdk module.
# It shows the use of a requirements.txt file and a connector that calls a publicly available API to get the weather forecast data for Myrtle Beach in South Carolina, USA.
# It also shows how to use the logging functionality provided by fivetran_connector_sdk, by logging important steps using log.info() and log.fine()
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details.

from datetime import datetime  # Import datetime for handling date and time conversions.

import requests as rq  # Import the requests module for making HTTP requests, aliased as rq.
# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector  # Import the Connector class from the fivetran_connector_sdk module.
from fivetran_connector_sdk import Logging as log  # Import the Logging class from the fivetran_connector_sdk module, aliased as log.
from fivetran_connector_sdk import Operations as op  # Import the Operations class from the fivetran_connector_sdk module, aliased as op.


# Define the schema function which lets you configure the schema your connector delivers.
# See the technical reference documentation for more details on the schema function:
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
# The schema function takes one parameter:
# - configuration: a dictionary that holds the configuration settings for the connector.
def schema(configuration: dict):
    return [
        {
            "table": "period",  # Name of the table in the destination.
            "primary_key": ["name", "startTime"],  # Primary key column(s) for the table.
            "columns": {  # Define the columns and their data types.
                "name": "STRING",  # String column for the period name.
                "startTime": "UTC_DATETIME",  # UTC date-time column for the start time.
                "endTime": "UTC_DATETIME",  # UTC date-time column for the end time.
                "temperature": "INT",  # Integer column for the temperature.
            },
        }
    ]


# Define a helper function to convert a string to a datetime object.
def str2dt(incoming: str) -> datetime:
    return datetime.strptime(incoming, "%Y-%m-%dT%H:%M:%S%z")


# Define the update function, which is a required function, and is called by Fivetran during each sync.
# See the technical reference documentation for more details on the update function:
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
# The function takes two parameters:
# - configuration: dictionary containing any secrets or payloads you configure when deploying the connector.
# - state: a dictionary containing the state checkpointed during the prior sync.
#   The state dictionary is empty for the first sync or for any full re-sync.
def update(configuration: dict, state: dict):
    # Retrieve the cursor from the state to determine the current position in the data sync.
    # If the cursor is not present in the state, start from the beginning of time ('0001-01-01T00:00:00Z').
    cursor = state['startTime'] if 'startTime' in state else '0001-01-01T00:00:00Z'

    # Get weather forecast for Myrtle Beach, SC from National Weather Service API.
    response = rq.get("https://api.weather.gov/gridpoints/ILM/58,40/forecast")

    # Parse the JSON response to get the periods of the weather forecast.
    data = response.json()
    periods = data['properties']['periods']

    # This message will show both during debugging and in production.
    log.info(f"number of periods={len(periods)}")

    for period in periods:
        # Skip data points we already synced by comparing their start time with the cursor.
        if str2dt(period['startTime']) < str2dt(cursor):
            continue

        # This log message will only show while debugging.
        log.fine(f"period={period['name']}")

        # Yield an upsert operation to insert/update the row in the "period" table.
        yield op.upsert(table="period",
                        data={
                            "name": period["name"],  # Name of the period.
                            "startTime": period["startTime"],  # Start time of the period.
                            "endTime": period["endTime"],  # End time of the period.
                            "temperature": period["temperature"]  # Temperature during the period.
                        })

        # Update the cursor to the end time of the current period.
        cursor = period['endTime']

    # Save the cursor for the next sync by yielding a checkpoint operation.
    yield op.checkpoint(state={
        "startTime": cursor
    })


# This creates the connector object that will use the update and schema functions defined in this connector.py file.
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from your IDE.
    connector.debug()

# Resulting table:
# ┌───────────────────────────┬─────────────────┬───────────────────────────┬─────────────┐
# │         starttime         │      name       │          endtime          │ temperature │
# │ timestamp with time zone  │     varchar     │ timestamp with time zone  │    int16    │
# ├───────────────────────────┼─────────────────┼───────────────────────────┼─────────────┤
# │ 2024-08-06 14:30:00+05:30 │ Overnight       │ 2024-08-06 15:30:00+05:30 │          77 │
# │ 2024-08-06 15:30:00+05:30 │ Tuesday         │ 2024-08-07 03:30:00+05:30 │          82 │
# │ 2024-08-07 03:30:00+05:30 │ Tuesday Night   │ 2024-08-07 15:30:00+05:30 │          78 │
# │ 2024-08-07 15:30:00+05:30 │ Wednesday       │ 2024-08-08 03:30:00+05:30 │          83 │
# │ 2024-08-08 03:30:00+05:30 │ Wednesday Night │ 2024-08-08 15:30:00+05:30 │          78 │
# │ 2024-08-08 15:30:00+05:30 │ Thursday        │ 2024-08-09 03:30:00+05:30 │          82 │
# │ 2024-08-09 03:30:00+05:30 │ Thursday Night  │ 2024-08-09 15:30:00+05:30 │          78 │
# │ 2024-08-09 15:30:00+05:30 │ Friday          │ 2024-08-10 03:30:00+05:30 │          84 │
# │ 2024-08-10 03:30:00+05:30 │ Friday Night    │ 2024-08-10 15:30:00+05:30 │          78 │
# │ 2024-08-10 15:30:00+05:30 │ Saturday        │ 2024-08-11 03:30:00+05:30 │          85 │
# │ 2024-08-11 03:30:00+05:30 │ Saturday Night  │ 2024-08-11 15:30:00+05:30 │          76 │
# │ 2024-08-11 15:30:00+05:30 │ Sunday          │ 2024-08-12 03:30:00+05:30 │          83 │
# │ 2024-08-12 03:30:00+05:30 │ Sunday Night    │ 2024-08-12 15:30:00+05:30 │          74 │
# │ 2024-08-12 15:30:00+05:30 │ Monday          │ 2024-08-13 03:30:00+05:30 │          84 │
# ├───────────────────────────┴─────────────────┴───────────────────────────┴─────────────┤
# │ 14 rows                                                                     4 columns │
# └───────────────────────────────────────────────────────────────────────────────────────┘
