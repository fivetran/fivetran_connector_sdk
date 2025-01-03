# This is a simple example for how to work with the fivetran_connector_sdk module.
# It shows the use of a cursor to fetch the data and a connector that calls a publicly available API to get random stock data
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details

# Refer to the Marketstack documentation (https://marketstack.com/documentation) for API endpoint details
# Please get your API key from here: https://marketstack.com/

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Operations as op
from fivetran_connector_sdk import Logging as log
# Import the requests module for making HTTP requests, aliased as rq.
import requests as rq
import json
from datetime import date, timedelta
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
    log.warning("Example: Common Patterns For Connectors - Cursors - Marketstack")

    try:
        # Initialize state
        updated_state = initialize_state(state)

        # Fetch records using api calls
        (updated_state, insert) = api_response(updated_state, configuration)

        for ticker_price in insert["tickers_price"]:
            yield op.upsert("tickers_price", ticker_price)

        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
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

    # Fetch the tickers for which information is needed.
    insert_tickers = get_tickers()

    # Fetch the records of prices of tickers.
    # After price for a ticker is fetched we increment ticker offset by 1
    insert_ticker_price = []
    for ticker in insert_tickers:
        temp_list = get_ticker_price(
            configuration["apiKey"], ticker, ticker_start_cursor, ticker_end_cursor)
        ticker_offset += 1
        if temp_list:
            insert_ticker_price += temp_list

    state, insert = {}, {}

    insert['tickers_price'] = insert_ticker_price

    # Update the state
    state['ticker_offset'] = ticker_offset
    state['ticker_start_cursor'] = ticker_start_cursor
    state['ticker_end_cursor'] = ticker_end_cursor

    return state, insert


def get_tickers():
    """This is a function to list all the tickers for which information is needed
    Returns:
        list: tickers
    """
    """
    If you need to fetch the list of tickers from the API, 
    please refer to the documentation at https://marketstack.com/documentation for more information.
    """
    return ["AAPL", "MSFT", "GOOG", "INTC"]


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
    while True:
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
            if insert_ticker_price_records_temp:
                insert_ticker_price_records += insert_ticker_price_records_temp
                ticker_price_offset += 1000
            else:
                break
        except rq.exceptions.RequestException as e:
            raise RuntimeError(f"Request failed: {str(e)}")
        except json.JSONDecodeError as e:
            raise RuntimeError(f"Failed to parse JSON response: {str(e)}")
        except KeyError as e:
            raise RuntimeError(f"Missing expected key in response: {str(e)}")
        except Exception as e:
            raise RuntimeError(
                "Failed Fetching ticker prices, Error: " + str(e))

    return insert_ticker_price_records


def initialize_state(state: dict):
    """This is a function to initialize state
    Args:
        state (json): State of the connector
    Returns:
        json: State of the connector
    """

    if not state:
        state["ticker_offset"] = 0
        state["ticker_start_cursor"] = date.today - timedelta(days=15)
        state["ticker_end_cursor"] = str(date.today())

    # Fetch data till the latest date if ticker_offset is 0
    if state["ticker_offset"] == 0:
        state["ticker_end_cursor"] = str(date.today())
    return state


# This creates the connector object that will use the update and schema functions defined in this connector.py file.
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the "fivetran debug" command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents into a dictionary.
    with open("configuration.json", 'r') as f:
        configuration = json.load(f)
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from your IDE.
    connector.debug(configuration=configuration)

"""
Resulting table:
Table tickers_price
┌─────────┬──────────────────────┬──────────────┬────────────┬──────────┬───┬───────────┬──────────┬────────┬─────────┬─────────┐
│ symbol  │         date         │ split_factor │   volume   │   high   │ … │ adj_close │ exchange │ close  │  open   │ adj_low │
│ varchar │       varchar        │    float     │   float    │  float   │   │   float   │ varchar  │ float  │  float  │  float  │
├─────────┼──────────────────────┼──────────────┼────────────┼──────────┼───┼───────────┼──────────┼────────┼─────────┼─────────┤
│ AAPL    │ 2024-10-14T00:00:0…  │          1.0 │ 32607920.0 │ 231.7278 │ … │     231.3 │ XNAS     │  231.3 │   228.7 │   228.6 │
│ AAPL    │ 2024-10-11T00:00:0…  │          1.0 │ 31668000.0 │   229.41 │ … │    227.55 │ XNAS     │ 227.55 │   229.3 │  227.34 │
│ AAPL    │ 2024-10-10T00:00:0…  │          1.0 │ 27959432.0 │    229.5 │ … │    229.04 │ XNAS     │ 229.04 │  227.78 │  227.17 │
│ AAPL    │ 2024-10-09T00:00:0…  │          1.0 │ 32108384.0 │   229.75 │ … │    229.54 │ XNAS     │ 229.54 │  225.17 │  224.83 │
├─────────┴──────────────────────┴──────────────┴────────────┴──────────┴───┴───────────┴──────────┴────────┴─────────┴─────────┤

# List of columns present:
┌──────────────┐
│ column_name  │
├──────────────┤
│ symbol       │
│ date         │
│ split_factor │
│ volume       │
│ high         │
│ adj_open     │
│ adj_volume   │
│ adj_high     │
│ low          │
│ dividend     │
│ adj_close    │
│ exchange     │
│ close        │
│ open         │
│ adj_low      │
├──────────────┤
"""
