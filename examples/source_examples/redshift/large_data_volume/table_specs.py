"""
This file contains table specifications and constants for Redshift connector.
Each table specification includes details such as table name, primary keys,
replication strategy, replication key, and columns to include or exclude.
You can modify the lists to add or change table configurations as needed.
"""

# Preferred timestamp column names for inferring replication keys
# If your tables have any of these column names, they will be prioritized when automatically selecting a replication key
# You can add or modify these names based on your database schema conventions
PREFERRED_TS_COLUMN_NAMES = [
    "updated_at",
    "last_updated",
    "last_update",
    "last_modified",
    "modified_at",
    "modified_on",
    "updated_on",
    "update_time",
    "updated",
]

# Set of Redshift data types that represent timestamps or dates
# These are used to identify potential replication key columns if not explicitly specified
# This is only used when replication_key is not set in table spec and strategy is INCREMENTAL
TIMESTAMP_TYPE_NAMES = {
    "timestamp",
    "timestamp without time zone",
    "timestamp with time zone",
    "timestamptz",
    "date",
}

# Number of rows after which to checkpoint progress
# This ensures that the connector can resume from the last successful sync in case of interruptions
CHECKPOINT_EVERY_ROWS = 1000

# List of table specifications for the Redshift connector
# Each dictionary in the list defines a table and its sync configuration
# You can modify this list to add or change table configurations as needed
# Each table spec includes:
# - name: The name of the table in the format "schema.table"
# - primary_keys: List of primary key columns for the table
# - strategy: Replication strategy, either "FULL" or "INCREMENTAL"
# - replication_key: Column used for incremental replication (if applicable).
# - include: List of columns to include in the sync (empty list means all columns)
# - exclude: List of columns to exclude from the sync (empty list means no exclusions)
TABLE_SPECS = [
    {
        "name": "public.customers",  # Name of the table from the Redshift database
        "primary_keys": [
            "customer_id"
        ],  # Primary key column(s) for the table. If None, the connector will fetch the primary key(s) from the source database.
        "strategy": "INCREMENTAL",  # Replication strategy: FULL or INCREMENTAL
        "replication_key": "updated_at",  # Column used for incremental replication. If None, the connector will attempt to infer it for INCREMENTAL sync strategy.
        "include": [],  # List of columns to include in the sync. An empty list means all columns are included.
        "exclude": [],  # List of columns to exclude from the sync. An empty list means no columns are excluded.
    },
    {
        "name": "public.orders",
        "primary_keys": ["order_id"],
        "strategy": "INCREMENTAL",
        "include": ["order_id", "customer_id", "amount_usd", "currency", "placed_at"],
        "exclude": [],
    },  # No replication_key specified. The replication key will be inferred as the sync strategy is INCREMENTAL
    {
        "name": "public.products",
        "strategy": "INCREMENTAL",
        "replication_key": "last_updated",
        "include": ["product_id", "sku", "title", "price_usd", "last_updated"],
        "exclude": [],
    },  # No primary_keys specified. The primary key(s) will be fetched from source database
    {
        "name": "public.events",
        "primary_keys": ["event_id"],
        "strategy": "FULL",
        "replication_key": None,  # replication_key is set to None for FULL sync strategy. You can also omit this field for FULL sync strategy
        "include": ["event_id", "event_name", "severity", "event_time"],
        "exclude": [],
    },
    {
        "name": "public.shifts",
        "primary_keys": ["shift_id"],
        "strategy": "FULL",
        "replication_key": None,
        "include": ["shift_id", "worker_name", "role", "shift_time"],
        "exclude": [],
    },
    {
        "name": "public.page_views",
        "primary_keys": ["view_id"],
        "strategy": "INCREMENTAL",
        "replication_key": "viewed_at",
        "include": ["view_id", "user_id", "page_path", "session_id", "viewed_at"],
        "exclude": [],
    },
    {
        "name": "public.devices",
        "primary_keys": ["device_id"],
        "strategy": "INCREMENTAL",
        "replication_key": "activated_at",
        "include": ["device_id", "serial", "model", "attrs", "activated_at"],
        "exclude": [],
    },
]
