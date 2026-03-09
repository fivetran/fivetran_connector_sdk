# Apache Druid Connector Example

## Connector overview

This connector syncs data from Apache Druid datasources into Fivetran using the PyDruid Python library, allowing you to replicate event and analytics data into your destination. The connector leverages PyDruid's native query capabilities and supports incremental sync using time-based filtering. Each datasource in your Druid cluster is synced as a separate table.

Apache Druid is a real-time analytics database designed for fast aggregations and queries on large event datasets. This PyDruid-based connector provides enhanced performance and native Druid query optimization for accessing your Druid datasources for analytics and reporting in your data warehouse.

## Requirements

- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)
- PyDruid library for native Druid connectivity

## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

- Incremental sync - Fetches only new records since the last sync using time-based filtering on the `__time` column
- Multi-datasource support - Syncs multiple Druid datasources in a single connector, each mapped to a separate destination table
- Native PyDruid queries - Uses PyDruid's native query capabilities for optimized data retrieval
- Per-datasource state tracking - Tracks sync progress independently per datasource
- Retry logic with exponential backoff - Automatically retries failed requests
- Basic authentication support - Optional username and password authentication for secured Druid clusters
- Periodic checkpointing - Saves sync progress regularly to enable safe resumption

## Configuration file

The connector requires a `configuration.json` file with the Druid connection details.

```json
{
  "host": "<YOUR_DRUID_HOST>",
  "port": "<YOUR_DRUID_PORT>",
  "datasources": "<YOUR_DRUID_DATASOURCES>"
}
```

For Druid clusters that require authentication:

```json
{
  "host": "<YOUR_DRUID_HOST>",
  "port": "<YOUR_DRUID_PORT>",
  "protocol": "<http_or_https>",
  "datasources": "<YOUR_DRUID_DATASOURCES>",
  "username": "<YOUR_DRUID_USERNAME>",
  "password": "<YOUR_DRUID_PASSWORD>"
}
```

Configuration parameters:
- `host` - Hostname or IP address of your Druid broker node (required)
- `port` - Port number for the Druid broker, typically 8888 (required)
- `datasources` - Comma-separated list of Druid datasource names to sync (required)
- `protocol` - Connection protocol: "http" or "https" (optional, defaults to "http")
- `username` - Username for basic authentication (optional)
- `password` - Password for basic authentication (optional)

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file

This connector requires the PyDruid library for native Druid connectivity:

```
pydruid>=0.6.5
```

Note: The `fivetran_connector_sdk` and `requests` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication

This connector supports optional basic authentication for Druid clusters that require credentials. If `username` and `password` are provided in the configuration, the connector uses PyDruid's built-in authentication support to add the proper authentication headers to all requests. If no credentials are provided, requests are sent without authentication.

## Data handling

The connector processes Druid data through the following steps:

- Configuration validation - Validates all required fields before making any API calls
- PyDruid client setup - Initializes PyDruid client with authentication settings and connection parameters
- Incremental filtering - Reads the per-datasource last sync timestamp from state. If none exists, defaults to epoch to trigger a full fetch
- Native query execution - Uses PyDruid scan queries with time-based filtering, advancing the cursor after each batch
- Data upserting - Each record is upserted to the corresponding destination table
- State checkpointing - Saves the maximum `__time` seen per datasource after processing

## Error handling

The connector implements error handling to ensure reliable data synchronization:

- PyDruid client error handling - Handles PyDruid client exceptions and connection errors
- Retry logic with exponential backoff - Automatically retries failed requests
- Timeout handling - Each request times out after 30 seconds
- Configuration validation - Validates all configuration parameters before making any API calls
- State checkpointing - Checkpoints sync state regularly, enabling safe resumption

## Tables created

The connector creates one table per Druid datasource specified in the `datasources` configuration. Table names are derived from datasource names by replacing any character that is not a letter, digit, or underscore with an underscore.

Column types are automatically detected from PyDruid query results and may vary depending on your datasource schema. All Druid datasources include a `__time` column representing when each event occurred. Dimension and metric columns vary per datasource.

Since Druid rows do not have a guaranteed unique key, no primary key is defined in the schema. Fivetran automatically generates a `_fivetran_id` column as a hash of all row values to uniquely identify each record.