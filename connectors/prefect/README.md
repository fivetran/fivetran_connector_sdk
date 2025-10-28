# Prefect Cloud Connector Example

## Connector overview

This connector syncs workflow orchestration data from Prefect Cloud to your data warehouse using the Prefect Cloud REST API. It enables you to analyze workflow execution patterns, monitor pipeline health, and optimize data orchestration workflows. The connector supports incremental syncing of flows, deployments, flow runs, task runs, artifacts, and variables with automatic checkpointing for reliable data synchronization.

## Requirements

- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

- Incremental syncing based on updated timestamps for all tables
- Automatic pagination handling for large datasets
- Retry logic with exponential backoff for transient API failures
- Automatic checkpointing every 500 records to ensure data consistency
- Flattening of nested JSON structures into relational tables
- Support for six core Prefect resources: flows, deployments, flow runs, task runs, artifacts, and variables

## Configuration file

The connector requires the following configuration parameters:

```json
{
  "api_key": "<YOUR_PREFECT_API_KEY>",
  "account_id": "<YOUR_PREFECT_ACCOUNT_ID>",
  "workspace_id": "<YOUR_PREFECT_WORKSPACE_ID>"
}
```

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file

The connector does not require any additional Python packages beyond the Fivetran Connector SDK.

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication

This connector uses Bearer token authentication to access the Prefect Cloud API. The API key is passed in the Authorization header for all requests.

To obtain your API credentials:

1. Log in to your Prefect Cloud account at https://app.prefect.cloud/.
2. Navigate to your account settings and select **API Keys**.
3. Generate a new API key with appropriate permissions.
4. Make a note of the API key. You will need to add it to the `configuration.json` file.
5. Find your account ID and workspace ID in the Prefect Cloud URL or account settings.
6. Add all three values to your `configuration.json` file.

## Pagination

The connector implements pagination using the Prefect API filter endpoints with limit and offset parameters. Each API request fetches up to 100 records per page. The connector continues fetching pages until an empty response is received, indicating all available data has been retrieved. Pagination logic is implemented in the `fetch_paginated_data()` function.

## Data handling

The connector fetches data from Prefect Cloud API filter endpoints and processes it as follows:

- JSON responses are flattened using the `flatten_record()` function to convert nested dictionaries into single-level key-value pairs
- Nested objects are flattened with underscore-separated keys
- Arrays are serialized as JSON strings
- Each record is upserted to the destination table with its flattened structure
- The connector tracks the maximum updated timestamp for each table to enable incremental syncing
- State is maintained separately for each table to allow independent sync progress

## Error handling

The connector implements comprehensive error handling:

- Retry logic with exponential backoff for transient failures including network timeouts, connection errors, and HTTP 429/5xx status codes
- Maximum of 3 retry attempts with delays of 1, 2, and 4 seconds
- Specific exception handling for timeout and connection errors
- Failed requests after all retries raise RuntimeError with detailed error messages
- All errors are logged with severity levels for monitoring and debugging
- Non-retryable errors such as authentication failures result in immediate failure

Refer to the `make_api_request()` function for detailed error handling implementation.

## Tables created

The connector creates the following tables in your destination:

### flows

Stores workflow definitions and metadata.

```json
{
  "table": "flows",
  "primary_key": ["id"]
}
```

Key columns include: `id`, `created`, `updated`, `name`, `tags`, `labels`

### deployments

Stores deployment configurations with scheduling information.

```json
{
  "table": "deployments",
  "primary_key": ["id"]
}
```

Key columns include: `id`, `created`, `updated`, `name`, `flow_id`, `schedule`, `parameters`, `is_schedule_active`, `paused`

### flow_runs

Stores execution records of workflow runs.

```json
{
  "table": "flow_runs",
  "primary_key": ["id"]
}
```

Key columns include: `id`, `created`, `updated`, `name`, `flow_id`, `deployment_id`, `state_type`, `state_name`, `start_time`, `end_time`, `total_run_time`

### task_runs

Stores execution records of individual tasks within flows.

```json
{
  "table": "task_runs",
  "primary_key": ["id"]
}
```

Key columns include: `id`, `created`, `updated`, `name`, `flow_run_id`, `task_key`, `state_type`, `state_name`, `start_time`, `end_time`, `total_run_time`

### artifacts

Stores output data and results from workflow executions.

```json
{
  "table": "artifacts",
  "primary_key": ["id"]
}
```

Key columns include: `id`, `created`, `updated`, `key`, `type`, `description`, `data`, `flow_run_id`, `task_run_id`

### variables

Stores configuration variables used across workflows.

```json
{
  "table": "variables",
  "primary_key": ["id"]
}
```

Key columns include: `id`, `created`, `updated`, `name`, `value`, `tags`

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
