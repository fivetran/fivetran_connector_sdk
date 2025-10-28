# Astronomer / Airflow Cloud API Connector Example

## Connector overview
This connector demonstrates how to fetch DAGs, DAG Runs, and Task Instances metadata from [Astronomer’s Airflow API](https://www.astronomer.io/docs/astro/airflow-api/) and sync it to your destination using the **Fivetran Connector SDK**.

The connector supports incremental synchronization by tracking `logical_date` timestamps for DAG Runs, handles pagination, and includes robust retry logic for API rate limits and transient errors.

This pattern is ideal for:
- Tracking Airflow DAG execution and task performance over time.
- Centralizing Airflow job metadata for analytics and reporting.
- Monitoring DAG and task-level statuses across deployments.

---

## Requirements
- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
    - Windows: 10 or later (64-bit only)
    - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
    - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)
- An **Astronomer Airflow deployment** with API access enabled.
- A valid **API token** generated from your Astronomer workspace.

---

## Getting started
Refer to the [Fivetran Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

---

## Features
- Connects to the **Astronomer-hosted Airflow REST API (v2)**.
- Retrieves and syncs:
    - DAG metadata (`/api/v2/dags`)
    - DAG Runs (`/api/v2/dags/{dag_id}/dagRuns`)
    - Task Instances (`/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances`)
- Supports incremental synchronization using `logical_date` cursors.
- Handles pagination automatically for large result sets.
- Retries rate-limited or transient API errors with exponential backoff.
- Logs all synchronization activity and state checkpoints.

---

## Configuration file
The connector requires the following configuration parameters:

```json
{
  "base_url": "<YOUR_ASTRONOMER_BASE_URL>",
  "api_token": "<YOUR_ASTRONOMER_API_TOKEN>",
  "verify_ssl": "<YOUR_SSL_VERIFICATION_OPTION>"
}
```

### Configuration parameters

| Parameter | Description |
|------------|-------------|
| `base_url` | The base URL for your Astronomer Airflow deployment (e.g., `https://your-workspace.astronomer.run`). |
| `api_token` | Your Astronomer API Bearer token for authentication. |
| `verify_ssl` | Optional. Set to `false` if your Astronomer deployment uses self-signed SSL certificates. Default is `true`. |

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

---


## Authentication
This connector uses **Bearer Token Authentication** to securely connect to your Astronomer-hosted Airflow API.

Each request includes the following header:
```
Authorization: Bearer <YOUR_ASTRONOMER_API_TOKEN>
```

### To generate an API token:
1. Log in to your [Astronomer Cloud Workspace](https://cloud.astronomer.io/).
2. Navigate to **Workspace Settings → API Tokens**.
3. Create a new API token with read-only permissions.
4. Copy the generated token into the connector configuration as `api_token`.

---

## Pagination
The connector handles pagination using `limit` and `offset` parameters for each API call:

- **DAGs** (`/api/v2/dags`): Paginated with `limit=100`.
- **DAG Runs** (`/api/v2/dags/{dag_id}/dagRuns`): Paginated with `limit=100`.
- **Task Instances** (`/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances`): Paginated with `limit=100`.

It continues fetching pages until all records are retrieved, handling large deployments efficiently.

---

## Data handling
The connector performs the following data extraction and transformation steps:

### 1. **DAGs**
- Fetches metadata for all DAGs in the Airflow environment.
- Fields include `dag_id`, `is_active`, `is_paused`, owners, tags, and scheduling configuration.
- Upserts records into the `airflow_dags` table.

### 2. **DAG Runs**
- Retrieves all runs for each DAG.
- Tracks incremental syncs using the `logical_date` timestamp.
- Filters out previously synced DAG Runs using a stored checkpoint cursor.
- Upserts records into the `airflow_dag_runs` table.

### 3. **Task Instances**
- Fetches task-level execution details for each DAG Run.
- Includes fields such as `task_id`, `try_number`, `state`, `duration`, and operator metadata.
- Upserts records into the `airflow_task_instances` table.

### 4. **Incremental Sync and Checkpointing**
- The connector stores the most recent `logical_date` value per DAG in the sync state.
- This ensures subsequent runs only fetch new or updated DAG Runs.
- State is persisted using:
  ```python
  op.checkpoint(state)
  ```
- Allows safe restarts and avoids duplicate data.

---

## Error handling
The connector includes comprehensive error handling:
- Retries 429, 502, 503, and 504 responses with **exponential backoff** (up to 5 attempts).
- Validates required configuration parameters before execution.
- Logs detailed error messages for debugging.
- Gracefully skips over failed DAGs or tasks while continuing to sync others.

Example log output:
```
INFO: Starting Astronomer Airflow sync
WARNING: Retrying https://your-workspace.astronomer.run/api/v2/dags after 429, sleeping 4s
INFO: Synced 42 DAGs, 388 DAG Runs, and 1,276 Task Instances
INFO: Sync completed successfully
```

---

## Tables created

### **airflow_dags**
Primary key: `dag_id`

| Column | Type | Description |
|---------|------|-------------|
| `dag_id` | STRING | Unique DAG identifier. |
| `is_paused` | BOOLEAN | Whether the DAG is currently paused. |
| `is_active` | BOOLEAN | Whether the DAG is active and deployed. |
| `owners` | STRING | Comma-separated list of DAG owners. |
| `tags` | STRING | Comma-separated list of DAG tags. |
| `timezone` | STRING | DAG timezone configuration. |
| `max_active_runs` | INT | Maximum number of concurrent DAG runs allowed. |
| `default_view` | STRING | Default view type (e.g., `graph`, `tree`). |
| `next_dagrun_create_after` | STRING | Timestamp of next DAG run creation. |
| `_synced_at` | UTC_DATETIME | Timestamp of the connector sync. |

---

### **airflow_dag_runs**
Primary key: (`dag_id`, `dag_run_id`)

| Column | Type | Description |
|---------|------|-------------|
| `dag_id` | STRING | DAG identifier. |
| `dag_run_id` | STRING | Unique DAG run identifier. |
| `run_type` | STRING | Type of DAG run (manual, scheduled, etc.). |
| `state` | STRING | Current state of the DAG run. |
| `logical_date` | UTC_DATETIME | Logical execution date. |
| `start_date` | UTC_DATETIME | Start timestamp. |
| `end_date` | UTC_DATETIME | End timestamp. |
| `external_trigger` | BOOLEAN | Whether the DAG run was externally triggered. |
| `data_interval_start` | STRING | Data interval start time. |
| `data_interval_end` | STRING | Data interval end time. |
| `_synced_at` | UTC_DATETIME | Timestamp of the connector sync. |

---

### **airflow_task_instances**
Primary key: (`dag_id`, `dag_run_id`, `task_id`, `try_number`)

| Column | Type | Description |
|---------|------|-------------|
| `dag_id` | STRING | Associated DAG identifier. |
| `dag_run_id` | STRING | Associated DAG run identifier. |
| `task_id` | STRING | Task identifier within the DAG. |
| `try_number` | INT | The task attempt number. |
| `state` | STRING | Task execution state (e.g., `success`, `failed`). |
| `start_date` | UTC_DATETIME | Task start time. |
| `end_date` | UTC_DATETIME | Task end time. |
| `duration` | FLOAT | Task duration in seconds. |
| `operator` | STRING | The operator class used by the task. |
| `hostname` | STRING | Worker hostname where the task executed. |
| `_synced_at` | UTC_DATETIME | Timestamp of the connector sync. |

---

## Additional considerations
- The connector is compatible with both **Astronomer Cloud** and **self-hosted Airflow** APIs.
- To optimize incremental syncs, the connector limits initial backfills to the last 7 days by default.
- Extendable to include **Airflow Variables**, **Pools**, or **XComs** by adding additional API calls.
- Designed for **educational and demonstration purposes** using the **Fivetran Connector SDK**.

For inquiries, please reach out to **Fivetran Support**.
