# Read Priority-first sync documentation (https://fivetran.com/docs/using-fivetran/features#priorityfirstsync)
# Priority-first sync, pfs for short, is very helpful for high volume historical syncs. It is a sync strategy
# which prioritises fetching the most recent data first so that it's quickly ready for you to use.
# This is a simple example of how the pfs strategy looks like.
# There are 2 tables/endpoints in the example, "user" and "customer".
# The `update` method is the starting point for the sync strategy.
# 1st step is to create a list of endpoints which will sync using pfs flow.
# 2nd step is to initialize bidirectional cursors for EACH endpoint, if not already created.
# Bidirectional cursor is dict with fields initialized as below:
# * incremental_cursor: initialized to time.now() minus recent data fetch duration, 1 day is used here
# * historical_cursor: initialized to time.now() minus recent data fetch duration, 1 day is used here
# * historical_limit: earliest timestamp upto which to fetch data in historical sync, it can be EPOCH or more recent
# state['is_incremental_sync'] is a boolean which decides whether to do a forward or backward sync. It's value is alternated
# every sync, so that syncs alternate between forward and backward sync to keep fetching recent data as the historical
# sync is in progress.
# Bidirection cursor fields are used as explained below:
# * incremental_cursor: incremental sync cursor, to fetch recent data
# * historical_cursor: historical sync cursor, sync fetch happens in the reverse direction,i.e. it moves towards historical_limit
# * historical_limit: it's used to check if historical sync has completed. If historical_cursor <= historical_limit, historical sync
# is complete.
# Forward sync strategy is straight forward. It syncs all data from incremental_cursor until now for each endpoint. Plain simple.
# Backward sync strategy is as follows:
# * Fetch data in batches, with each batch moving the historical_cursor by 1 day in this example. The 1 day duration can be tweaked.
# Duration should be such that volume is not too high or too low.
# * For each batch `backward_sync` method is called which will fetch 1 days worth of data.
# * If backward sync completes, then set historical_cursor to historical_limit, so that future syncs can check it is completed

# Import required classes from fivetran_connector_sdk.
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op
from datetime import datetime, timedelta, timezone
import traceback

import users_sync

base_url = "http://127.0.0.1:5001/pagination/keyset"
sync_start = datetime.now().astimezone(timezone.utc)

# Constants
PFS_CURSORS = 'pfs_cursors'
INCREMENTAL_CURSOR = 'incremental_cursor'
HISTORICAL_CURSOR = 'historical_cursor'
HISTORICAL_LIMIT = 'historical_limit'
IS_INCREMENTAL_SYNC = 'is_incremental_sync'
SYNC_DURATION_THRESHOLD = 6

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
        }
    ]

def update(configuration: dict, state: dict):
    try:
        # list of endpoints for pfs flow mapped from table names
        endpoints = []
        for table_schema in schema(configuration):
            endpoints.append(table_schema['table'])

        # initializes pfs cursors in state for new endpoints
        initialize_pfs_cursors(state, endpoints)

        # perform incremental sync based on `is_incremental_sync` state field and also checking
        # if all historical syncs have completed else perform historical sync and finally
        # switch value of `is_incremental_sync` state field to try alternate incremental and
        # historical sync
        if is_pfs_incremental_sync(state) or is_historical_syncs_complete(state, endpoints):
            log.info('starting incremental syncs')
            yield from run_incremental_syncs(state, endpoints)
            set_pfs_incremental_sync(state, False)
        else:
            log.info('starting historical syncs')
            yield from run_historical_syncs(state, endpoints)
            set_pfs_incremental_sync(state, True)

        yield op.checkpoint(state)
    except Exception:
        # logs stack trace
        log.severe(traceback.format_exc())

def is_historical_syncs_complete(state, endpoints):
    for endpoint in endpoints:
        if datetime.fromisoformat(get_pfs_historical_cursor(state, endpoint)) > datetime.fromisoformat(get_pfs_historical_limit(state, endpoint)):
            return False
    return True

def run_incremental_syncs(state, endpoints):
    for endpoint in endpoints:
        log.info('starting incremental sync for ' + endpoint + ' endpoint')
        yield from incremental_sync(state, endpoint)

def run_historical_syncs(state, endpoints):
    for endpoint in endpoints:
        log.info('starting historical sync for ' + endpoint + ' endpoint')
        while datetime.fromisoformat(get_pfs_historical_cursor(state, endpoint)) > datetime.fromisoformat(
                get_pfs_historical_limit(state, endpoint)):
            yield from historical_sync(state, endpoint)
            if is_sync_duration_threshold_breached():
                log.info('Sync duration breached sync_duration_threshold. Stopping sync to flush data to destination...')
                return

def is_sync_duration_threshold_breached():
    if ((datetime.now().astimezone(timezone.utc) - sync_start).total_seconds() / 60 / 60) >= SYNC_DURATION_THRESHOLD:
        return True
    return False

def incremental_sync(state, endpoint):
    params = {
        "updated_since": get_pfs_incremental_cursor(state, endpoint)
    }

    if endpoint == 'user':
        yield from users_sync.sync_users(base_url, params, state, False)

def historical_sync(state, endpoint):
        updated_since = (datetime.fromisoformat(get_pfs_historical_cursor(state, endpoint)) - timedelta(days=1))
        if updated_since < datetime.fromisoformat(get_pfs_historical_limit(state, endpoint)):
            updated_since = datetime.fromisoformat(get_pfs_historical_limit(state, endpoint))
        params = {
            "updated_since": updated_since.isoformat()
        }

        if endpoint == 'user':
            yield from users_sync.sync_users(base_url, params, state, True)

def initialize_pfs_cursors(state, endpoints):
    if PFS_CURSORS not in state:
        state[PFS_CURSORS] = {}
    # init bidirectional cursors
    for endpoint in endpoints:
        # Retrieve the cursor from the state to determine the current position in the data sync.
        # If the cursor is not present in the state, start from the beginning of time ('0001-01-01T00:00:00Z').
        if endpoint not in state[PFS_CURSORS]:
            state[PFS_CURSORS][endpoint] = {}
            # forward cursor is used in incremental syncs
            # backward cursor is used in historical syncs
            # historical_limit is the earliest timestamp upto which to fetch for historical syncs
            # historical_limit may be EPOCH in actual usage. 5 days is used here.
            state[PFS_CURSORS][endpoint][INCREMENTAL_CURSOR] = (datetime.now() - timedelta(days=1)).astimezone(timezone.utc).isoformat()
            state[PFS_CURSORS][endpoint][HISTORICAL_CURSOR] = state[PFS_CURSORS][endpoint][INCREMENTAL_CURSOR]
            state[PFS_CURSORS][endpoint][HISTORICAL_LIMIT] = (datetime.now() - timedelta(days=5)).astimezone(timezone.utc).isoformat()
    if IS_INCREMENTAL_SYNC not in state[PFS_CURSORS]:
        state[PFS_CURSORS][IS_INCREMENTAL_SYNC] = True

def is_pfs_incremental_sync(state):
    return state[PFS_CURSORS][IS_INCREMENTAL_SYNC]

def set_pfs_incremental_sync(state, value):
    state[PFS_CURSORS][IS_INCREMENTAL_SYNC] = value

def set_pfs_incremental_cursor(state, endpoint, value):
    state[PFS_CURSORS][endpoint][INCREMENTAL_CURSOR] = value

def set_pfs_historical_cursor(state, endpoint, value):
    state[PFS_CURSORS][endpoint][HISTORICAL_CURSOR] = value

def set_pfs_historical_limit(state, endpoint, value):
    state[PFS_CURSORS][endpoint][HISTORICAL_LIMIT] = value

def get_pfs_incremental_cursor(state, endpoint):
    return state[PFS_CURSORS][endpoint][INCREMENTAL_CURSOR]

def get_pfs_historical_cursor(state, endpoint):
    return state[PFS_CURSORS][endpoint][HISTORICAL_CURSOR]

def get_pfs_historical_limit(state, endpoint):
    return state[PFS_CURSORS][endpoint][HISTORICAL_LIMIT]

def format_datetime(date_time):
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