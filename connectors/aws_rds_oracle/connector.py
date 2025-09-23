# This is a example for how to work with the fivetran_connector_sdk module.
# This is an example to show how we can sync records from AWS RDS Oracle DB via
# Connector SDK.
# You would need to provide your Oracle credentials for this example to work.
# See the Technical Reference documentation:
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
# and the Best Practices documentation:
# https://fivetran.com/docs/connectors/connector-sdk/best-practices for details

# Oracle DB driver for Python
import oracledb

# Import required classes from fivetran_connector_sdk
from typing import Dict, List, Any
from datetime import datetime

# For supporting Connector operations like Update() and Schema()
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# Table definitions: update/add as needed for your Oracle source.
# You can add more tables to this list to sync additional tables.
TABLES: List[Dict[str, Any]] = [
    {
        "schema": "ADMIN",
        "table": "FIVETRAN_LOGMINER_TEST",
        "pk": ["ID"],
        "columns": ["ID", "NAME", "LAST_UPDATED"],
    },
]


def get_config_value(configuration: dict, key: str, default: str = "") -> str:
    """
    Safely get a config value as a string from the configuration dictionary.
    All values in configuration.json must be strings.
    """
    return str(configuration.get(key, default))


def connect_oracle(configuration: dict) -> "oracledb.Connection":
    """
    Establish a connection to Oracle using config values from
    configuration.json.
    The configuration file should contain host, port, service_name,
    user, and password.
    """
    host = get_config_value(configuration, "host")
    port = int(get_config_value(configuration, "port", "1521"))
    service = get_config_value(configuration, "service_name")
    user = get_config_value(configuration, "user")
    password = get_config_value(configuration, "password")
    dsn = oracledb.makedsn(host=host, port=port, service_name=service)
    return oracledb.connect(user=user, password=password, dsn=dsn)


def schema(configuration: dict) -> List[Dict[str, Any]]:
    """
    Define the schema function which lets you configure the schema your
    connector delivers.
    See the technical reference documentation for more details on the schema
    function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    Args:
        configuration: a dictionary that holds the configuration settings for
        the connector.
    Returns:
        List of table schemas for Fivetran.
    """
    return [{"table": table_def["table"], "primary_key": table_def["pk"]} for table_def in TABLES]


def update(configuration: dict, state: dict) -> None:
    """
    Main sync logic for Fivetran Connector SDK.
    This function is called by Fivetran during each sync.
    Args:
        configuration: A dictionary containing connection details.
        state: A dictionary containing state information from previous runs.
    The state dictionary is empty for the first sync or for any full re-sync.
    """
    log.info("Starting update function")
    try:
        conn = connect_oracle(configuration)
        log.info(f"Connected to Oracle DB: {conn.version}")
    except Exception as e:
        log.warning(f"Oracle connection failed: {e}")
        raise

    # Use last_sync_time from state or default to the Unix for initial sync.
    last_sync_time = state.get("last_sync_time", "1970-01-01 00:00:00")
    log.info(f"Last sync time: {last_sync_time}")

    for table_def in TABLES:
        schema_name = table_def["schema"]
        table_name = table_def["table"]
        columns = table_def["columns"]
        full_table_name = f"{schema_name}.{table_name}"

        log.info(f"Syncing table: {full_table_name}")

        try:
            with conn.cursor() as cur:
                query = (
                    f"SELECT {', '.join(columns)} FROM {full_table_name} "
                    "WHERE LAST_UPDATED > TO_TIMESTAMP(:last_sync_time, "
                    "'YYYY-MM-DD HH24:MI:SS')"
                )
                cur.execute(query, {"last_sync_time": last_sync_time})
                rows = cur.fetchall()
        except Exception as e:
            log.warning(f"Failed to fetch data from {full_table_name}: {e}")
            continue

        for row in rows:
            record = dict(zip(columns, row))
            try:
                # The op.upsert method is called with two arguments:
                # - The first argument is the name of the table to
                # upsert the data into,
                # in this case, "customers".
                # - The second argument is a dictionary containing
                # the data to be upserted,
                op.upsert(table=table_name, data=record)
            except Exception as e:
                log.warning(f"Upsert failed for record {record}: {e}")

    # Save new sync time for incremental syncs.
    new_sync_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Save the progress by checkpointing the state. This is important for
    # ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # Learn more about how and where to checkpoint by reading our best
    # practices documentation
    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices
    # largedatasetrecommendation).
    op.checkpoint(state={"last_sync_time": new_sync_time})
    log.info("Update function completed successfully")


# This creates the connector object that will use the update function defined
# in this connector.py file.
connector = Connector(update=update, schema=schema)

# Source table:
# ┌─────────────┬─────────────┬─────────────┬─────────────┬──────────┐
# │     ID      │    NAME     │ LAST_UPDATED│   ...other columns...  │
# │   int       │  varchar    │ timestamp   │                        │
# ├─────────────┼─────────────┼─────────────┼─────────────────────── ┤
# │     1       │   Mathew    │ 2023-12-31 23:59:59.000 │ ...        │
# │     2       │   Joe       │ 2024-01-31 23:04:39.000 │ ...        │
# │     3       │   Jake      │ 2023-11-01 23:59:59.000 │ ...        │
# │     4       │   John      │ 2024-02-14 22:59:59.000 │ ...        │
# │     5       │   Ricky     │ 2024-03-16 16:40:29.000 │ ...        │
# ├─────────────┴─────────────┴─────────────┴────────────────────────┤
# │ 5 rows                                              3 columns+   │
# └──────────────────────────────────────────────────────────────────┘

# Resulting table:
# ┌─────────────┬─────────────┬─────────────┬────────────────────────────┐
# │     ID      │    NAME     │ LAST_UPDATED                            │
# │   int       │  varchar    │ timestamp                               │
# ├─────────────┼─────────────┼──────────────────────────────────────────┤
# │     2       │   Joe       │ 2024-01-31T23:04:39Z                    │
# │     4       │   John      │ 2024-02-14T22:59:59Z                    │
# │     5       │   Ricky     │ 2024-03-16T16:40:29Z                    │
# ├─────────────┴─────────────┴──────────────────────────────────────────┤
# │ 3 rows                                                   3 columns   │
# └──────────────────────────────────────────────────────────────────────┘
