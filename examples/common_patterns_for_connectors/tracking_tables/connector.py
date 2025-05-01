"""
This is an example of how to add endpoints or tables to sync to an existing Fivetran Connector SDK.
The list of tables to sync can be retrieved from connector configs if they are not present in state object.
The list of tables to sync is thereafter stored in the state object.
This example is the simplest possible as it doesn't define a schema() function,
it does not therefore provide a good template for writing a real connector.

To observe how adding a new table would work:
1. Run fivetran debug --configuration configuration.json (configuration.json provides initialSyncStart)
2. Observe that files/state.json contains "synced_tables": ["A", "B", "C"]
3. In connector.py, add another table name to the list in TABLES_TO_SYNC, e.g. TABLES_TO_SYNC = ["A", "B", "C", "D"]
4. Run fivetran debug --configuration configuration.json again
5. Log output should show "... FINE: D is new, needs history from ..."
6. Observe that files/state.json synced_tables now contains all tables in TABLES_TO_SYNC
7. Observe that the new table is synced for its full history for each iteration

See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""
# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op
from datetime import datetime, timezone

# Add any new tables to sync to list below
TABLES_TO_SYNC = ["A", "B", "C"]
tables_synced_this_sync = []

def update(configuration: dict, state: dict):
    """
    Required Fivetran SDK function called during each sync.
    Determines which tables to sync and when, based on previous sync state and config.
    The state dictionary is empty for the first sync or for any full re-sync
    :param configuration: dictionary contains any secrets or payloads you configure when deploying the connector
    :param state: a dictionary contains whatever state you have chosen to checkpoint during the prior sync
    :return:
    """
    start_timestamp = datetime.now(timezone.utc).isoformat("T", "milliseconds").replace("+00:00", "Z")
    from_timestamp, to_timestamp, initial_timestamp = set_timeranges(state, configuration, start_timestamp)

    """
    Get list of synced tables from state. 
    If state doesn't contain a list of tables, getting it from configuration allows
    implementing this with connectors that don't yet track which tables are synced.
    """
    if state.get("synced_tables"):
        tables_previously_synced = state["synced_tables"]
        log.fine(f"tables synced from state: {tables_previously_synced}")
    elif configuration.get("synced_tables"):
        tables_previously_synced = configuration["synced_tables"].split(",")
        log.fine(f"tables synced from config: {tables_previously_synced}")
    else:
        tables_previously_synced = []

    new_state = {"to_timestamp": to_timestamp}
    log.fine(f"tables to sync: {TABLES_TO_SYNC}")
    log.fine(f"tables previously synced: {tables_previously_synced}")

    yield from sync_tables(tables_previously_synced, from_timestamp, initial_timestamp)

    """
    Save the progress by checkpointing the state. This stores the list of tables that are up-to-date.
    Learn more about how and where to checkpoint by reading our best practices documentation
    (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
    """
    tables_synced_this_sync.sort()
    new_state["synced_tables"] = tables_synced_this_sync
    log.fine(new_state)
    yield op.checkpoint(new_state)

def sync_tables(tables_previously_synced: list, from_timestamp, initial_timestamp):

    for table in TABLES_TO_SYNC:
        if table in tables_previously_synced:
            log.fine(f"{table} was synced before, go back to {from_timestamp}")
            data = {"message": f"syncing {table} since {from_timestamp}"}
        else:
            log.fine(f"{table} is new, needs history from {initial_timestamp}")
            data = {"message": f"syncing {table} since {initial_timestamp}"}
        yield op.upsert(table=table, data=data)

        if table not in tables_synced_this_sync:
            tables_synced_this_sync.append(table)

def set_timeranges(state, configuration, start_timestamp):
    """
    Takes in current state and start timestamp of current sync.
    from_timestamp is always either the end of the last sync or the initialSyncStart found in the config file.
    to_timestamp is the time that this sync was triggered.
    initial_timestamp is taken from configuration["initialSyncStart"]
    :param state: a dictionary contains whatever state you have chosen to checkpoint during the prior sync
    :param configuration: dictionary contains any secrets or payloads you configure when deploying the connector
    :param start_timestamp: timestamp of the start of the sync
    :return: from_timestamp, to_timestamp, initial_timestamp
    """
    if 'to_timestamp' in state:
        from_timestamp = state['to_timestamp']
    else:
        from_timestamp = configuration["initialSyncStart"]

    to_timestamp = start_timestamp
    initial_timestamp_dt = datetime.fromisoformat(configuration["initialSyncStart"].replace("Z", "+00:00"))
    initial_timestamp = initial_timestamp_dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")

    return from_timestamp, to_timestamp, initial_timestamp


# This creates the connector object that will use the update function defined in this connector.py file.
# This example does not use the schema() function. If it did, it would need to be included in the connector object definition.
connector = Connector(update=update)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from your IDE:
    connector.debug()
