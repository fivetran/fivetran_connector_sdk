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
- Native PyDruid queries - Uses PyDruid's native query capabilities for optimized data retrieval, leveraging Druid's native segment pruning for performance
- Per-datasource state tracking - Tracks sync progress independently per datasource so adding a new datasource triggers only a full fetch for that datasource
- Retry logic with exponential backoff - Automatically retries failed requests up to 3 times with delays of 2s, 4s, and 8s
- Basic authentication support - Optional username and password authentication for secured Druid clusters
- Periodic checkpointing - Saves sync progress every 1000 records to enable safe resumption after interruption

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
  "datasources": "<YOUR_DRUID_DATASOURCES>",
  "username": "<YOUR_DRUID_USERNAME>",
  "password": "<YOUR_DRUID_PASSWORD>"
}
```

Configuration parameters:
- `host` - Hostname or IP address of your Druid router or broker node (required)
- `port` - Port number for the Druid router or broker, typically 8888 (required)
- `datasources` - Comma-separated list of Druid datasource names to sync (required)
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

The connector connects to Druid over HTTPS by default. The protocol can be configured by setting the `protocol` parameter to "http" or "https". Refer to `DruidPyDruidClient` in `connector.py` for the authentication implementation.

## Pagination

The connector uses PyDruid's native scan queries with time-based filtering for both full and incremental syncs. Each batch queries records with `__time` strictly greater than the current cursor, then advances the cursor to the last record's timestamp. Since results are ordered by `__time`, the last record in each batch is always the maximum timestamp.

For a full sync, the cursor starts at epoch (`1970-01-01T00:00:00+00:00`). For incremental syncs, it starts from the last synced timestamp stored in state. This approach leverages Druid's native time-based segment pruning, which is significantly faster than SQL `OFFSET` for large tables.

## Data handling

The connector processes Druid data through the following steps:

- Configuration validation - Validates all required fields (`host`, `port`, `datasources`) and optional fields before making any API calls. Refer to `validate_configuration()`.

- PyDruid client setup - Initializes PyDruid client with authentication settings and connection parameters. Refer to `update()`.

- Incremental filtering - Reads the per-datasource last sync timestamp from state. If none exists, defaults to epoch to trigger a full fetch. Refer to `fetch_datasource_data_pydruid()`.

- Native query execution - Uses PyDruid scan queries with time-based filtering, advancing the cursor after each batch until no more records are returned. Refer to `fetch_datasource_data_pydruid()`.

- Data upserting - Each record is upserted to the corresponding destination table. Fivetran automatically generates a `_fivetran_id` surrogate key as a hash of all row values, since Druid rows do not have a guaranteed unique key. Refer to `update()`.

- State checkpointing - Saves the maximum `__time` seen per datasource after each datasource completes. Also checkpoints every 1000 records during processing to enable safe resumption. Refer to `update()`.

## Error handling

The connector implements robust error handling to ensure reliable data synchronization:

- PyDruid client error handling - Handles PyDruid client exceptions and connection errors with appropriate retry logic
- Retry logic with exponential backoff - Automatically retries failed requests up to 3 times, waiting 2s, 4s, and 8s between attempts
- Timeout handling - Each request times out after 30 seconds, and timeout errors are retried up to the maximum retry count
- Configuration validation - Validates all configuration parameters before making any API calls and provides clear error messages for missing or invalid fields. See `validate_configuration()`
- State checkpointing - Checkpoints sync state after every 1000 records and after each datasource completes, enabling safe resumption without re-processing already-synced data

## Tables created

The connector creates one table per Druid datasource specified in the `datasources` configuration. Table names are derived from datasource names by replacing any character that is not a letter, digit, or underscore with an underscore.

Column types are automatically detected from PyDruid query results and may vary depending on your datasource schema. All Druid datasources include a `__time` column representing when each event occurred. Dimension and metric columns vary per datasource.

Since Druid rows do not have a guaranteed unique key, no primary key is defined in the schema. Fivetran automatically generates a `_fivetran_id` column as a hash of all row values to uniquely identify each record.

Example data for a `wikipedia` datasource:

| `_fivetran_id` | `__time` | `channel` | `page` | `delta` |
|----------------|----------|-----------|--------|---------|
| a1b2c3d4... | 2024-01-15T10:30:00.000Z | #en.wikipedia | Python | 42 |
| e5f6g7h8... | 2024-01-15T10:31:00.000Z | #fr.wikipedia | Django | -5 |

## Performance considerations

This PyDruid-based connector is optimized for performance:

- **Native Druid queries** - Uses PyDruid's native query types for optimal performance
- **Connection pooling** - Reduces connection overhead through persistent connections
- **Batch processing** - Processes data in configurable batch sizes (10,000 records per batch)
- **Segment pruning** - Leverages Druid's time-based segment pruning automatically
- **Parallel datasource processing** - Processes multiple datasources concurrently when possible

The connector uses optimized default settings for batch size (10,000 records), timeouts (30 seconds), and retry logic (3 attempts with exponential backoff).

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK with PyDruid. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.

For optimal performance with large Druid clusters, ensure your PyDruid version is compatible with your Druid cluster version and consider tuning the connection pool and batch size parameters based on your specific use case.