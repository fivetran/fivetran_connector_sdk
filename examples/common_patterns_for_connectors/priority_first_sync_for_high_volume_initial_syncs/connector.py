# Read Priority-first sync documentation (https://fivetran.com/docs/using-fivetran/features#priorityfirstsync)
# Priority-first sync, pfs for short, is very helpful for high volume historical syncs. It is a sync strategy
# which prioritises fetching the most recent data first so that it's quickly ready for you to use.
# This is a simple example of how the pfs strategy looks like.
# There are 2 tables/endpoints in the example, "user" and "customer".
# The `update` method is the starting point for the sync strategy.
# 1st step is to create a list of endpoints which will sync using pfs flow.
# 2nd step is to initialize bidirectional cursors for EACH endpoint, if not already created.
# Bidirectional cursor is dict with fields initialized as below:
# * forward_cursor: initialized to time.now() minus recent data fetch duration, 1 day is used here
# * backward_cursor: initialized to time.now() minus recent data fetch duration, 1 day is used here
# * backward_limit: earliest timestamp upto which to fetch data in historical sync, it can be EPOCH or more recent
# state['isForwardSync'] is a boolean which decides whether to do a forward or backward sync. It's value is alternated
# every sync, so that syncs alternate between forward and backward sync to keep fetching recent data as the historical
# sync is in progress.
# Bidirection cursor fields are used as explained below:
# * forward_cursor: incremental sync cursor, to fetch recent data
# * backward_cursor: historical sync cursor, sync fetch happens in the reverse direction,i.e. it moves towards backward_limit
# * backward_limit: it's used to check if historical sync has completed. If backward_cursor <= backward_limit, historical sync
# is complete.
# Forward sync strategy is straight forward. It syncs all data from forward_cursor until now for each endpoint. Plain simple.
# Backward sync strategy is as follows:
# * Fetch data in batches, with each batch moving the backward_cursor by 1 day in this example. The 1 day duration can be tweaked.
# Duration should be such that volume is not too high or too low.
# * For each batch `backward_sync` method is called which will fetch 1 days worth of data.
# * If backward sync completes, then set backward_cursor to backward_limit, so that future syncs can check it is completed

# Import required classes from fivetran_connector_sdk.
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op
from datetime import datetime, timedelta, timezone
import traceback

import customers_sync
import users_sync

base_url = "http://127.0.0.1:5001/pagination/keyset"
syncStart = datetime.now().astimezone(timezone.utc)

# Define the schema function which lets you configure the schema your connector delivers.
# See the technical reference documentation for more details on the schema function:
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
# The schema function takes one parameter:
# - configuration: a dictionary that holds the configuration settings for the connector.
def schema(configuration: dict):
    return [
        {
            "table": "user",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "name": "STRING",
                "email": "STRING",
                "address": "STRING",
                "company": "STRING",
                "job": "STRING",
                "updated_at": "UTC_DATETIME",
                "created_at": "UTC_DATETIME",
            },
        },
        {
            "table": "customer",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "name": "STRING",
                "email": "STRING",
                "address": "STRING",
                "company": "STRING",
                "job": "STRING",
                "updated_at": "UTC_DATETIME",
                "created_at": "UTC_DATETIME",
            },
        }
    ]

def update(configuration: dict, state: dict):
    try:
        endpoints = []
        for table_schema in schema(configuration):
            endpoints.append(table_schema["table"])

        initBidirectionalCursors(state, endpoints)

        if state['isForwardSync'] is True or isPendingBackwardSyncsEmpty(state, endpoints):
            yield from run_forward_syncs(state, endpoints)
        else:
            yield from run_backward_syncs(state, endpoints)
            state['isForwardSync'] = True

        yield op.checkpoint(state)
    except Exception:
        log.severe(traceback.format_exc())

def isSyncDurationLongerThan6Hrs():
    if ((datetime.now().astimezone(timezone.utc) - syncStart).total_seconds() / 60 / 60) >= 6:
        return True
    return False

def isPendingBackwardSyncsEmpty(state, endpoints):
    for endpoint in endpoints:
        if datetime.fromisoformat(state[endpoint]['backward_cursor']) > datetime.fromisoformat(state[endpoint]['backward_limit']):
            return False
    return True

def run_forward_syncs(state, endpoints):
    for endpoint in endpoints:
        yield from forward_sync(state, endpoint)
    state['isForwardSync'] = False

def run_backward_syncs(state, endpoints):
    for endpoint in endpoints:
        while datetime.fromisoformat(state[endpoint]['backward_cursor']) > datetime.fromisoformat(
                state[endpoint]['backward_limit']):
            yield from backward_sync(state, endpoint)
            if isSyncDurationLongerThan6Hrs():
                log.info("Stopping sync to flush data to destination...")
                return

def forward_sync(state, endpoint):
    params = {
        "updated_since": state[endpoint]["forward_cursor"]
    }

    if endpoint == 'user':
        yield from users_sync.sync_users(base_url, params, state, False)
    if endpoint == 'customer':
        yield from customers_sync.sync_customers(base_url, params, state, False)

def backward_sync(state, endpoint):
        updated_since = (datetime.fromisoformat(state[endpoint]['backward_cursor']) - timedelta(days=1))
        if updated_since < datetime.fromisoformat(state[endpoint]['backward_limit']):
            updated_since = datetime.fromisoformat(state[endpoint]['backward_limit'])
        params = {
            "updated_since": updated_since.isoformat()
        }

        if endpoint == 'user':
            yield from users_sync.sync_users(base_url, params, state, True)
        if endpoint == 'customer':
            yield from customers_sync.sync_customers(base_url, params, state, True)

def initBidirectionalCursors(state, endpoints):
    # init bidirectional cursors
    for endpoint in endpoints:
        # Retrieve the cursor from the state to determine the current position in the data sync.
        # If the cursor is not present in the state, start from the beginning of time ('0001-01-01T00:00:00Z').
        if endpoint not in state:
            state[endpoint] = {}
            # forward cursor is used in incremental syncs
            # backward cursor is used in historical syncs
            # backward_limit is the earliest timestamp upto which to fetch for historical syncs
            # backward_limit may be EPOCH in actual usage. 5 days is used here.
            state[endpoint]['forward_cursor'] = (datetime.now() - timedelta(days=1)).astimezone(timezone.utc).isoformat()
            state[endpoint]['backward_cursor'] = state[endpoint]['forward_cursor']
            state[endpoint]['backward_limit'] = (datetime.now() - timedelta(days=5)).astimezone(timezone.utc).isoformat()
    if 'isForwardSync' not in state:
        state['isForwardSync'] = True

def should_continue_pagination(response_page):
    return response_page.get("has_more")

def formatIsoDatetime(date_time):
    return date_time.strftime('%Y-%m-%dT%H:%M:%SZ')


# This creates the connector object that will use the update and schema functions defined in this connector.py file.
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module. This is Python's standard entry method allowing your script to
# be run directly from the command line or IDE 'run' button. This is useful for debugging while you write your code.
# Note this method is not called by Fivetran when executing your connector in production. Please test using the
# Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from
    # your IDE.
    connector.debug()