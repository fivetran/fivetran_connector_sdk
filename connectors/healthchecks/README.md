# Healthchecks.io Connector Example

## Connector overview

This connector syncs health check monitoring data from Healthchecks.io to your data warehouse using the Fivetran Connector SDK. Healthchecks.io is a cron job and background task monitoring service that tracks whether scheduled jobs are running on time. This connector enables you to analyze health check performance, track uptime trends, identify reliability patterns, and integrate monitoring data with your broader data analytics infrastructure.

The connector supports incremental syncing and retrieves data from four primary endpoints: health checks (monitors), pings (check-in events), flips (status changes), and integrations (notification channels). This allows you to build comprehensive dashboards for operational monitoring, SLA tracking, and incident analysis.

## Requirements

- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

- Incremental syncing with state management for efficient data replication
- Automatic retry logic with exponential backoff for transient API failures
- Comprehensive data model covering checks, pings, status changes, and integrations
- Flattened schema design for easy querying and analysis
- Support for all Healthchecks.io Management API v3 endpoints
- Graceful error handling with detailed logging for troubleshooting

## Configuration file

The connector requires the following configuration parameter:

```json
{
  "api_key": "<YOUR_HEALTHCHECKS_API_KEY>"
}
```

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file

This connector uses only the standard libraries and packages pre-installed in the Fivetran environment. No additional dependencies are required.

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication

This connector uses API key authentication to access the Healthchecks.io Management API v3. The API key is passed via the `X-Api-Key` HTTP header for all requests (refer to the `get_headers()` and `make_api_request()` functions).

To obtain your API key:

1. Log in to your Healthchecks.io account.
2. Navigate to **Settings > API Access**.
3. Copy your project-specific API key.
4. Add the API key to your `configuration.json` file.

Note: Healthchecks.io provides both read-write and read-only API keys. This connector requires a read-only key at minimum, though a read-write key will also work.

## Pagination

The Healthchecks.io API does not implement traditional pagination for the endpoints used by this connector. The `/api/v3/checks/` endpoint returns all checks in a single response. For ping and flip history, the API returns the most recent events based on account limits (100 pings for free accounts, 1000 pings for paid accounts). The connector fetches all available data in each sync without pagination logic.

## Data handling

The connector transforms nested JSON responses from the Healthchecks.io API into flattened relational tables suitable for data warehouse storage (refer to the `flatten_check_record()`, `flatten_ping_record()`, `flatten_flip_record()`, and `flatten_integration_record()` functions).

For parent-child relationships, the connector:
- Flattens single nested JSON objects into the parent table
- Creates separate breakout tables for arrays of objects (pings and flips)
- Maintains referential integrity using foreign keys (`check_uuid` in pings and flips tables)
- Generates composite primary keys for child tables (`ping_id` and `flip_id`)

The connector uses incremental syncing with timestamp-based state management to track the last successful sync. Each sync fetches all checks and their associated pings and flips, ensuring data consistency (refer to the `update()` function and state management logic).

## Error handling

The connector implements comprehensive error handling strategies (refer to the `make_api_request()` function):

- Automatic retry with exponential backoff for transient failures (HTTP 429, 500, 502, 503, 504)
- Maximum of 3 retry attempts with delays of 1s, 2s, and 4s
- Network timeout protection (30-second timeout for all requests)
- Graceful degradation for non-critical endpoints (pings, flips, and integrations failures are logged but don't halt the sync)
- Detailed error logging using Fivetran SDK logging levels (info, warning, severe)
- Fail-fast behavior for authentication errors and permanent failures (4xx errors)

## Tables created

The connector creates four tables in your destination:

### checks

```json
{
  "table": "checks",
  "primary_key": ["uuid"],
  "columns": {
    "uuid": "STRING",
    "name": "STRING",
    "slug": "STRING",
    "tags": "STRING",
    "desc": "STRING",
    "grace": "INT",
    "n_pings": "INT",
    "status": "STRING",
    "started": "BOOLEAN",
    "last_ping": "UTC_DATETIME",
    "next_ping": "UTC_DATETIME",
    "manual_resume": "BOOLEAN",
    "methods": "STRING",
    "subject": "STRING",
    "subject_fail": "STRING",
    "start_kw": "STRING",
    "success_kw": "STRING",
    "failure_kw": "STRING",
    "filter_subject": "BOOLEAN",
    "filter_body": "BOOLEAN",
    "badge_url": "STRING",
    "ping_url": "STRING",
    "update_url": "STRING",
    "pause_url": "STRING",
    "resume_url": "STRING",
    "channels": "STRING",
    "timeout": "INT"
  }
}
```

### pings

```json
{
  "table": "pings",
  "primary_key": ["ping_id"],
  "columns": {
    "ping_id": "STRING",
    "check_uuid": "STRING",
    "n": "INT",
    "type": "STRING",
    "date": "UTC_DATETIME",
    "scheme": "STRING",
    "remote_addr": "STRING",
    "method": "STRING",
    "ua": "STRING",
    "duration": "FLOAT"
  }
}
```

### flips

```json
{
  "table": "flips",
  "primary_key": ["flip_id"],
  "columns": {
    "flip_id": "STRING",
    "check_uuid": "STRING",
    "timestamp": "UTC_DATETIME",
    "up": "INT"
  }
}
```

### integrations

```json
{
  "table": "integrations",
  "primary_key": ["id"],
  "columns": {
    "id": "STRING",
    "name": "STRING",
    "kind": "STRING"
  }
}
```

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
