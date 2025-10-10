"""Example connector demonstrating how to sync data from AWS RDS Oracle using the Fivetran Connector SDK.
See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

import oracledb

from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

__TABLES: List[Dict[str, Any]] = [
    {
        "schema": "ADMIN",
        "table": "FIVETRAN_LOGMINER_TEST",
        "pk": ["ID"],
        "columns": ["ID", "NAME", "LAST_UPDATED"],
    },
]

__DEFAULT_PORT = 1521
__FETCH_BATCH_SIZE = 500
__CHECKPOINT_INTERVAL = 500
__DEFAULT_SYNC_DATETIME = datetime(1970, 1, 1, 0, 0, 0)


def validate_configuration(configuration: dict) -> None:
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing.
    """

    required_configs = ["host", "port", "service_name", "user", "password"]
    missing = [key for key in required_configs if not configuration.get(key)]
    if missing:
        raise ValueError(
            f"Missing required configuration value(s): {', '.join(missing)}"
        )


def _parse_timestamp(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        candidate = value.strip()
        if not candidate:
            return None
        if candidate.endswith("Z"):
            candidate = f"{candidate[:-1]}+00:00"
        try:
            return datetime.fromisoformat(candidate)
        except ValueError:
            try:
                return datetime.strptime(candidate, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                return None
    return None


def _format_timestamp(value: datetime) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S")


def connect_oracle(configuration: dict) -> "oracledb.Connection":
    """
    Establish a connection to Oracle using configuration values.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Returns:
        Active Oracle database connection.
    Raises:
        ValueError: if the provided port value is not an integer.
        oracledb.Error: if the database connection cannot be established.
    """

    port_value = configuration.get("port", __DEFAULT_PORT)
    try:
        port = int(port_value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid port value in configuration: {port_value}") from exc

    dsn = oracledb.makedsn(
        host=configuration.get("host"),
        port=port,
        service_name=configuration.get("service_name"),
    )
    return oracledb.connect(
        user=configuration.get("user"),
        password=configuration.get("password"),
        dsn=dsn,
    )


def schema(configuration: dict) -> List[Dict[str, Any]]:
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Returns:
        List of table schemas for Fivetran.
    """

    return [
        {
            "table": table_def["table"],
            "primary_key": table_def["pk"],
        }
        for table_def in __TABLES
    ]


def update(configuration: dict, state: dict) -> None:
    """
    Define the update function, which is a required function, and is called by Fivetran during each sync.
    See the technical reference documentation for more details on the update function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    Args:
        configuration: A dictionary containing connection details.
        state: A dictionary containing state information from previous runs.
        The state dictionary is empty for the first sync or for any full re-sync.
    """

    log.info("Starting AWS RDS Oracle update sync")

    validate_configuration(configuration=configuration)

    raw_last_sync_time = state.get("last_sync_time")
    parsed_last_sync_time = _parse_timestamp(raw_last_sync_time)
    if raw_last_sync_time and parsed_last_sync_time is None:
        log.warning(
            f"Unable to parse last_sync_time '{raw_last_sync_time}', defaulting to {_format_timestamp(__DEFAULT_SYNC_DATETIME)}."
        )
    last_sync_time_dt = parsed_last_sync_time or __DEFAULT_SYNC_DATETIME
    last_sync_time_str = _format_timestamp(last_sync_time_dt)
    new_sync_time_dt = last_sync_time_dt

    try:
        conn = connect_oracle(configuration=configuration)
        log.info("Connected to Oracle database")
    except Exception as exc:
        log.error(f"Oracle connection failed: {exc}")
        raise

    try:
        for table_def in __TABLES:
            schema_name = table_def["schema"]
            table_name = table_def["table"]
            columns = table_def["columns"]
            full_table_name = f"{schema_name}.{table_name}"

            log.info(f"Syncing table {full_table_name}")

            table_latest_sync_dt: Optional[datetime] = None
            processed_rows = 0

            try:
                with conn.cursor() as cursor:
                    cursor.arraysize = __FETCH_BATCH_SIZE
                    query = (
                        f"SELECT {', '.join(columns)} FROM {full_table_name} "
                        "WHERE LAST_UPDATED > TO_TIMESTAMP(:last_sync_time, 'YYYY-MM-DD HH24:MI:SS') "
                        "ORDER BY LAST_UPDATED"
                    )
                    cursor.execute(query, {"last_sync_time": last_sync_time_str})

                    while True:
                        rows = cursor.fetchmany()
                        if not rows:
                            break

                        for row in rows:
                            record = dict(zip(columns, row))
                            op.upsert(table=table_name, data=record)

                            record_sync_dt = _parse_timestamp(
                                record.get("LAST_UPDATED")
                            )
                            if record_sync_dt and (
                                table_latest_sync_dt is None or record_sync_dt > table_latest_sync_dt
                            ):
                                table_latest_sync_dt = record_sync_dt

                            processed_rows += 1
                            if processed_rows % __CHECKPOINT_INTERVAL == 0:
                                checkpoint_dt = (
                                    table_latest_sync_dt or new_sync_time_dt or __DEFAULT_SYNC_DATETIME
                                )
                                op.checkpoint(
                                    {"last_sync_time": _format_timestamp(checkpoint_dt)}
                                )
                                log.info(
                                    f"Checkpointed state after {processed_rows} rows for table {full_table_name}"
                                )

            except Exception as exc:
                log.error(f"Failed to sync table {full_table_name}: {exc}")
                continue

            if table_latest_sync_dt and table_latest_sync_dt > new_sync_time_dt:
                new_sync_time_dt = table_latest_sync_dt

            log.info(
                f"Finished syncing table {full_table_name}, processed {processed_rows} rows"
            )

    finally:
        conn.close()

    final_state = {"last_sync_time": _format_timestamp(new_sync_time_dt)}
    op.checkpoint(final_state)
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
