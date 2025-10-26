# Prometheus Connector Example

## Connector overview

This connector demonstrates how to sync metrics and time-series data from Prometheus, an open-source monitoring and alerting system. Prometheus is widely used for collecting and storing time-series data as metrics with millisecond precision timestamps. This connector extracts metric metadata and time-series data points, enabling users to analyze monitoring data in their destination warehouse.

The connector syncs two primary tables: metrics metadata (containing information about available metrics) and time-series data points (containing the actual metric values with timestamps and labels). It supports incremental syncs based on timestamps, multiple authentication methods, and configurable metric filtering to manage high-cardinality data.

## Requirements

- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

- Syncs metrics metadata including metric names, types, and help text
- Extracts time-series data points with labels and timestamps
- Supports incremental syncs based on last sync timestamp
- Configurable authentication methods including none, basic auth, and bearer token
- Optional metric filtering to control which metrics to sync
- Configurable lookback period for initial sync
- Automatic retry logic with exponential backoff for API failures
- Batched checkpointing to ensure sync resumability

## Configuration file

The connector requires the following configuration parameters:

```json
{
  "prometheus_url": "<YOUR_PROMETHEUS_SERVER_URL>",
  "auth_type": "<AUTHENTICATION_TYPE_NONE_BASIC_OR_BEARER>",
  "username": "<YOUR_USERNAME_FOR_BASIC_AUTH>",
  "password": "<YOUR_PASSWORD_FOR_BASIC_AUTH>",
  "bearer_token": "<YOUR_BEARER_TOKEN_FOR_BEARER_AUTH>",
  "lookback_hours": "<INITIAL_SYNC_LOOKBACK_HOURS_DEFAULT_24>",
  "metrics_filter": "<OPTIONAL_LIST_OF_METRIC_NAMES_TO_SYNC>"
}
```

Configuration parameters:

- `prometheus_url` - The base URL of your Prometheus server (e.g., http://localhost:9090)
- `auth_type` - Authentication method: none, basic, or bearer
- `username` - Username for basic authentication (optional, only if auth_type is basic)
- `password` - Password for basic authentication (optional, only if auth_type is basic)
- `bearer_token` - Bearer token for token-based authentication (optional, only if auth_type is bearer)
- `lookback_hours` - Number of hours to look back for initial sync (optional, defaults to 24)
- `metrics_filter` - List of specific metric names to sync (optional, if omitted all metrics are synced)

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file

This connector uses only the standard library and packages pre-installed in the Fivetran environment. No additional dependencies are required.

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication

This connector supports three authentication methods:

- None - No authentication (default for local Prometheus instances)
- Basic authentication - Username and password-based authentication
- Bearer token - Token-based authentication

To configure authentication:

1. Determine which authentication method your Prometheus server requires.

2. Set the `auth_type` field in `configuration.json` to none, basic, or bearer.

3. If using basic authentication:

   i. Obtain or create a username and password for your Prometheus server.

   ii. Add the `username` and `password` fields to `configuration.json`.

4. If using bearer token authentication:

   i. Obtain a valid bearer token from your authentication provider.

   ii. Add the `bearer_token` field to `configuration.json`.

5. For local testing with Docker Compose, use auth_type none as no authentication is configured by default.

The authentication logic is implemented in the `get_auth_headers()` function which builds the appropriate HTTP headers based on the configured authentication type.

## Data handling

The connector processes data in two stages:

Metrics metadata sync - The connector first fetches all available metric names from Prometheus using the `/api/v1/label/__name__/values` endpoint. For each metric, it retrieves metadata including the metric type (counter, gauge, histogram, summary) and help text from the `/api/v1/targets/metadata` endpoint. This data is upserted into the metrics table. Refer to the `sync_metrics_metadata()` function in connector.py.

Time-series data sync - The connector fetches actual time-series data using range queries via the `/api/v1/query_range` endpoint. Each metric is queried separately with a configurable step duration (default 15 seconds). The results include metric labels, timestamps, and values which are processed into individual data points. Each data point is assigned a unique series identifier based on the metric name and label combination. Refer to the `generate_series_id()` function in connector.py. The data is upserted into the time_series table.

For incremental syncs, the connector uses the `last_sync_timestamp` from state to query only new data since the last successful sync. For initial syncs, it uses a configurable lookback period (default 24 hours). Data is processed in batches with checkpointing every 1000 data points to ensure the sync can resume from the correct position if interrupted. Refer to the `sync_time_series_for_metrics()` function in connector.py.

## Error handling

The connector implements comprehensive error handling:

API request retry logic - All HTTP requests to Prometheus include automatic retry logic with exponential backoff. The connector retries up to 3 times for transient errors including timeouts and HTTP status codes 429, 500, 502, 503, and 504. The retry delay doubles with each attempt starting from 1 second. Non-retryable errors like authentication failures result in immediate failure. This is implemented in the `make_api_request()` function in connector.py.

Specific exception handling - The connector catches specific exception types including `requests.Timeout` and `requests.RequestException` rather than generic exceptions. All errors are logged at the severe level with detailed error messages before raising RuntimeError to fail the sync.

Response validation - All API responses are validated to ensure the status field is success before processing data. Invalid responses trigger errors with detailed information about what went wrong.

## Tables created

The connector creates two tables in the destination:

### metrics table

```json
{
  "table": "metrics",
  "primary_key": ["metric_name"],
  "columns": {
    "metric_name": "STRING",
    "metric_type": "STRING",
    "help_text": "STRING"
  }
}
```

This table contains metadata about each metric available in Prometheus. The `metric_name` is the unique identifier, `metric_type` indicates whether the metric is a counter, gauge, histogram, or summary, and `help_text` provides a description of what the metric measures.

### time_series table

```json
{
  "table": "time_series",
  "primary_key": ["series_id", "timestamp"],
  "columns": {
    "series_id": "STRING",
    "metric_name": "STRING",
    "labels": "JSON",
    "timestamp": "UTC_DATETIME",
    "value": "DOUBLE"
  }
}
```

This table contains the actual time-series data points. Each row represents a single measurement at a specific point in time. The `series_id` is a unique hash generated from the metric name and label combination. The `labels` column contains a JSON object with all label key-value pairs for that time series. The `timestamp` is when the measurement was taken, and `value` is the numeric metric value.

## Additional files

The connector includes additional files for local testing:

- **docker-compose.yml** - Docker Compose configuration to set up a local Prometheus instance with sample data generators for testing the connector
- **prometheus.yml** - Prometheus server configuration defining scrape jobs and targets
- **Dockerfile.mock** - Dockerfile for building the mock application container that generates test metrics
- **mock_exporter.py** - Python script that exposes various Prometheus metrics (counters, gauges, histograms, summaries) simulating a real application for testing purposes

To test this connector locally with Docker Compose:

1. Navigate to the connector directory.

2. Start the Prometheus stack with `docker-compose up -d`.

3. Wait for all services to start (approximately 30 seconds).

4. Verify Prometheus is running by opening http://localhost:9090 in your browser.

5. Update your `configuration.json` with the local Prometheus URL and set auth_type to none.

6. The mock application will automatically generate test metrics that Prometheus scrapes every 10 seconds.

7. To stop the Prometheus stack, run `docker-compose down`. To stop and remove all data, run `docker-compose down -v`.

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
