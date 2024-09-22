# This is a simple example for how to work with the fivetran_connector_sdk module.
# It shows the use of a cursor to fetch the data and a connector that calls a publicly available API to get random stock data
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Operations as op
# Import the requests module for making HTTP requests, aliased as rq.
import requests as rq
import json
from datetime import date
import time
import traceback

# Define the schema function which lets you configure the schema your connector delivers.
# See the technical reference documentation for more details on the schema function:
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
# The schema function takes one parameter:
# - configuration: a dictionary that holds the configuration settings for the connector.


def schema(configuration: dict):
    """This is a function to get schema

    Returns:
        json: schema
    """
    return [
        {
            "table": "tickers",
            "primary_key": ["symbol"],
            # Columns and data types will be inferred by Fivetran
        },
        {
            "table": "tickers_price",
            "primary_key": ['symbol', 'date'],
            # Columns and data types will be inferred by Fivetran
        }
    ]


# Define the update function, which is a required function, and is called by Fivetran during each sync.
# See the technical reference documentation for more details on the update function:
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
# The function takes two parameters:
# - configuration: dictionary containing any secrets or payloads you configure when deploying the connector.
# - state: a dictionary containing the state checkpointed during the prior sync.
#   The state dictionary is empty for the first sync or for any full re-sync.


def update(configuration: dict, state: dict):
    try:
        # Initialize state
        updated_state = initialize_state(state)

        # Fetch records using api calls
        (updated_state, insert) = api_response(updated_state, configuration)
        for ticker in insert["tickers"]:
            yield op.upsert("tickers", ticker)

        for ticker_price in insert["tickers_price"]:
            yield op.upsert("tickers_price",ticker_price)
            
        yield op.checkpoint(state=updated_state)

    except Exception as e:
        # Return error response
        exception_message = str(e)
        stack_trace = traceback.format_exc()
        detailed_message = f"Error Message: {exception_message}\nStack Trace:\n{stack_trace}"
        raise RuntimeError(detailed_message)
    

def api_response(state, configuration):

    ticker_offset = state["ticker_offset"]
    ticker_start_cursor = state["ticker_start_cursor"]
    ticker_end_cursor = state["ticker_end_cursor"]

    # Fetch all the tickers
    insert_tickers = get_tickers(configuration["apiKey"], ticker_offset)

    # Fetch the records of prices of tickers.
    # If time exceeds 1s then return intermediate response and fetch other records in subsequent calls
    # After price for a ticker is fetched we increment ticker offset by 1
    insert_ticker_price = []
    insert_ticker_actual = []
    start_time = time.time()
    for ticker in insert_tickers:
        temp_list = get_ticker_price(
            configuration["apiKey"], ticker['symbol'], ticker_start_cursor, ticker_end_cursor)
        ticker_offset += 1
        if(temp_list):
            insert_ticker_price += temp_list
            insert_ticker_actual.append(ticker)
        end_time = time.time()
        if(end_time-start_time > 1):
            break

    state, insert = {}, {}

    insert['tickers'] = insert_ticker_actual
    insert['tickers_price'] = insert_ticker_price

    # Update the state
    state['ticker_offset'] = ticker_offset
    state['ticker_start_cursor'] = ticker_start_cursor
    state['ticker_end_cursor'] = ticker_end_cursor

    return (state, insert)


def get_tickers(api_key, ticker_offset):
    """This is a function to list all the tickers presently available

    Args:
        api_key (String): The api token for accessing data
        ticker_offset (int): Ticker cursor value

    Raises:
        Exception: When request fails or tickers cannot be fetched from response

    Returns:
        list: tickers
    """
    params = {
        'access_key': api_key,
        'offset': ticker_offset,
        'limit': 100
    }
    try:
        api_result = rq.get(
            'http://api.marketstack.com/v1/tickers', params)
        api_response = api_result.json()
        insert_ticker_records = api_response["data"]
    except:
        raise Exception("Failed Fetching tickers, Error: " +
                        json.dumps(api_response))
    return insert_ticker_records


def get_ticker_price(api_key, symbols, ticker_start_cursor, ticker_end_cursor):
    """This is a function to fetch the prices of a particular ticker from start date to end date

    Args:
        api_key (String): The api token for accessing data
        symbols (String): Ticker for which price is to be calculated
        ticker_start_cursor (String): Starting date to fetch records
        ticker_end_cursor (String): End date to fetch records

    Raises:
        Exception: When request fails or ticker prices cannot be fetched from response

    Returns:
        list: ticker prices for particular ticker
    """
    ticker_price_offset = 0
    insert_ticker_price_records = []
    while(True):
        params = {
            'access_key': api_key,
            'symbols': symbols,
            'limit': 1000,
            'offset': ticker_price_offset,
            'date_from': ticker_start_cursor,
            'date_to': ticker_end_cursor
        }
        try:
            api_result = rq.get(
                'http://api.marketstack.com/v1/eod', params)
            api_response = api_result.json()
            insert_ticker_price_records_temp = api_response["data"]
            if(insert_ticker_price_records_temp):
                insert_ticker_price_records += insert_ticker_price_records_temp
                ticker_price_offset += 1000
            else:
                break
        except:
            raise Exception(
                "Failed Fetching ticker prices, Error: " + json.dumps(api_response))
    return insert_ticker_price_records


def initialize_state(state):
    """This is a function to initialize state

    Args:
        state (json): State of the connector

    Returns:
        json: State of the connector
    """

    if(not state):
        state["ticker_offset"] = 0
        state["ticker_start_cursor"] = "2000-01-01"
        state["ticker_end_cursor"] = str(date.today())

    # Fetch data till the latest date if ticker_offset is 0
    if(state["ticker_offset"] == 0):
        state["ticker_end_cursor"] = str(date.today())
    return state


# This creates the connector object that will use the update and schema functions defined in this connector.py file.
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the "fivetran debug" command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from your IDE.
    connector.debug()
    