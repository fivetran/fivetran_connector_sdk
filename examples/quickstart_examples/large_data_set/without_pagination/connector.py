# This is a simple example for how to work with the fivetran_connector_sdk module.
# This example demonstrates how to work with API which has large data set in the response.
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector # For supporting Connector operations like Update() and Schema()
from fivetran_connector_sdk import Logging as log # For enabling Logs in your connector code
from fivetran_connector_sdk import Operations as op # For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()

import pandas as pd

# Import the requests module for making HTTP requests, aliased as rq.
import requests as rq

# Define the constant values
MAX_PAGE_LIMIT = 100000
BATCH_SIZE = 100
BASE_URL = "https://pokeapi.co/api/v2/pokemon"


# Define the update function, which is a required function, and is called by Fivetran during each sync.
# See the technical reference documentation for more details on the update function
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
# The function takes two parameters:
# - configuration: dictionary contains any secrets or payloads you configure when deploying the connector
# - state: a dictionary contains whatever state you have chosen to checkpoint during the prior sync
# The state dictionary is empty for the first sync or for any full re-sync
def update(configuration: dict, state: dict):
    log.warning("Example: QuickStart Examples - Large Data Set Without Pagination")

    offset = 0
    api_endpoint = BASE_URL + "?offset=" + str(offset) + "&limit=" + str(MAX_PAGE_LIMIT)
    next_url, pokemons_df = get_data(api_endpoint, offset)
    pokemon_batches = divide_into_batches(pokemons_df)
    for batch in pokemon_batches:
        for index, row in batch.iterrows():
            yield op.upsert(table="pokemons", data={col: row[col] for col in batch.columns})
        offset = offset + len(batch)
        state["offset"] = offset
        yield op.checkpoint(state)


# Function to divide large data frame into small batches
def divide_into_batches(pokemons):
    for index in range(0, len(pokemons), BATCH_SIZE):
        yield pokemons.iloc[index: index + BATCH_SIZE]


# Function to fetch data from an API
def get_data(url, offset):
    response = rq.get(url)
    data = response.json()
    next_url = data["next"]
    pokemons = data["results"]
    pokemons_df = pd.DataFrame([])
    for i in range(len(pokemons)):
        pokemon_data = {
            "name": pokemons[i]["name"],
            "url": pokemons[i]["url"]
        }
        pokemons_df = pd.concat([pokemons_df, pd.DataFrame([pokemon_data])], ignore_index=True)
    return next_url, pokemons_df


# This creates the connector object that will use the update and schema functions defined in this connector.py file.
connector = Connector(update=update)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the "fivetran debug" command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from your IDE.
    connector.debug()
