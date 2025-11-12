"""QuickBase API Connector for Fivetran Connector SDK.

This connector demonstrates how to fetch data from QuickBase API and upsert
it into destination using the Fivetran Connector SDK.
Supports querying data from Applications, Tables, Fields, and Records.

See the Technical Reference documentation:
https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
and the Best Practices documentation:
https://fivetran.com/docs/connectors/connector-sdk/best-practices
"""

# For reading configuration from a JSON file
import json

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op
from typing import Dict, List, Any

# Import helper modules
from config import (
    parse_configuration,
    validate_configuration,
    determine_sync_type,
    SyncType,
)
from api_client import QuickBaseAPIClient
from data_processor import QuickBaseDataProcessor


def schema(configuration: dict) -> List[Dict[str, Any]]:
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """

    return [
        {
            "table": "application",
            "primary_key": ["app_id"],
        },
        {
            "table": "table",
            "primary_key": ["table_id"],
        },
        {
            "table": "field",
            "primary_key": ["field_id", "table_id"],
        },
        {
            "table": "record",
            "primary_key": ["table_id", "record_id"],
        },
        {
            "table": "sync_metadata",
            "primary_key": ["table_id"],
        },
    ]


def update(configuration: dict, state: dict) -> None:
    """
     Define the update function, which is a required function, and is called by Fivetran during each sync.
    See the technical reference documentation for more details on the update function
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    Args:
        configuration: A dictionary containing connection details
        state: A dictionary containing state information from previous runs
        The state dictionary is empty for the first sync or for any full re-sync
    """

    log.warning("Example: Source example connector for QuickBase")

    try:
        # Parse and validate configuration
        config = parse_configuration(configuration)
        validate_configuration(config)

        # Initialize API client and data processor
        api_client = QuickBaseAPIClient(config)
        processor = QuickBaseDataProcessor()

        # Determine sync type
        sync_type = determine_sync_type(state, config.enable_incremental_sync)
        last_sync_time = state.get("last_sync_time")

        # Sync applications data
        app_data = api_client.get_application()
        if app_data:
            processed_app = processor.process_application_data(app_data)
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into.
            # - The second argument is a dictionary containing the data to be upserted,
            op.upsert(table="application", data=processed_app)
            log.info("Applications data synced")

        # Sync tables data (simplified - just create entry for app_id)
        table_data = processor.process_table_data(config.app_id)
        # The 'upsert' operation is used to insert or update data in the destination table.
        # The op.upsert method is called with two arguments:
        # - The first argument is the name of the table to upsert the data into.
        # - The second argument is a dictionary containing the data to be upserted,
        op.upsert(table="table", data=table_data)
        log.info("Tables data synced")

        # Determine tables to sync
        tables_to_sync = config.table_ids if config.table_ids else [config.app_id]

        # Sync each table
        for table_id in tables_to_sync:
            log.info(f"Processing table: {table_id}")

            # Sync fields data
            if config.enable_fields_sync:
                fields = api_client.get_fields(table_id)
                for field in fields:
                    processed_field = processor.process_field_data(field, table_id)
                    # The 'upsert' operation is used to insert or update data in the destination table.
                    # The op.upsert method is called with two arguments:
                    # - The first argument is the name of the table to upsert the data into.
                    # - The second argument is a dictionary containing the data to be upserted,
                    op.upsert(table="field", data=processed_field)
                log.info(f"Fields synced for table {table_id}: {len(fields)} fields")

            # Sync records data
            if config.enable_records_sync:
                table_last_sync = (
                    state.get(f"table_{table_id}_last_sync", last_sync_time)
                    if sync_type == SyncType.INCREMENTAL
                    else None
                )

                response = api_client.get_records(table_id, table_last_sync)

                for record in response.records:
                    processed_record = processor.process_record_data(
                        record, table_id, response.fields_map
                    )
                    # The 'upsert' operation is used to insert or update data in the destination table.
                    # The op.upsert method is called with two arguments:
                    # - The first argument is the name of the table to upsert the data into.
                    # - The second argument is a dictionary containing the data to be upserted,
                    op.upsert(table="record", data=processed_record)

                # Create sync metadata with API metadata
                sync_metadata = processor.create_sync_metadata(
                    table_id, response.records_count, metadata=response.metadata
                )
                # The 'upsert' operation is used to insert or update data in the destination table.
                # The op.upsert method is called with two arguments:
                # - The first argument is the name of the table to upsert the data into.
                # - The second argument is a dictionary containing the data to be upserted,
                op.upsert(table="sync_metadata", data=sync_metadata)

                log.info(f"Records synced for table {table_id}: {response.records_count} records")

        # Update state
        from datetime import datetime

        current_time = datetime.now().isoformat()
        new_state = {"last_sync_time": current_time}

        # Add table-specific sync times
        for table_id in tables_to_sync:
            new_state[f"table_{table_id}_last_sync"] = current_time

        # Update state with the current sync time for the next run
        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        op.checkpoint(new_state)
        log.info("QuickBase connector sync completed successfully")

    except Exception as e:
        log.severe(f"QuickBase sync failed: {e}")
        raise RuntimeError(f"QuickBase sync failed: {e}")


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents
    with open("configuration.json", "r") as f:
        configuration = json.load(f)

    # Test the connector locally
    connector.debug(configuration=configuration)
