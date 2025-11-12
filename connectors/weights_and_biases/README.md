# Weights & Biases (W&B) Connector Example

## Connector overview
This connector demonstrates how to fetch experiment and metric data from [Weights & Biases (W&B)](https://wandb.ai/) and upsert it into your destination using the **Fivetran Connector SDK**.  
It synchronizes **run metadata** and **metric time-series data** from W&B projects, supports incremental synchronization via timestamps, and includes comprehensive retry and error handling logic for large-scale ML experiment tracking.

---

## Requirements
- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
    - Windows: 10 or later (64-bit only)
    - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
    - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)
- A valid **Weights & Biases API key**
- Access to your W&B **entity** and **project** names

---

## Getting started
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

---

## Features
- Connects to Weights & Biases using REST API
- Fetches run metadata including configuration, tags, and summary metrics
- Fetches detailed metric history for each run
- Implements incremental synchronization using timestamp-based watermarking
- Handles pagination for large datasets
- Built-in retry logic with exponential backoff for rate limits and server errors
- Robust logging for monitoring and debugging

---

## Configuration file
The connector requires the following configuration parameters:

```json
{
  "api_key": "<YOUR_WANDB_API_KEY>",
  "entity": "<YOUR_WANDB_ENTITY>",
  "project": "<YOUR_WANDB_PROJECT>"
}
```

### Configuration parameters

| Parameter | Description |
|------------|-------------|
| `api_key` | Required. Your W&B API key for authentication. |
| `entity` | Required. The W&B user or organization name. |
| `project` | Required. The project name in which your runs are logged. |
| `page_size` | Optional. The number of records to retrieve per request (default: 100). |

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

---


## Authentication
The connector uses **Bearer Token authentication** to connect securely to the W&B API.

To obtain your API key:
1. Go to your [W&B account settings](https://wandb.ai/settings).
2. Click **API Keys**.
3. Copy your personal API key.
4. Add it to the `configuration.json` file as the `api_key`.

Each request includes the header:
```
Authorization: Bearer <YOUR_WANDB_API_KEY>
```

---

## Pagination
The connector paginates through the W&B `/runs` and `/history` endpoints using standard W&B API pagination.

- The `/projects/{entity}/{project}/runs` endpoint supports the `perPage` parameter for batch retrieval of runs.
- If a `nextUrl` field is present in the response, the connector automatically fetches subsequent pages until all runs are processed.
- Each run’s metrics are fetched individually using the `/runs/{entity}/{project}/{run_id}/history` endpoint.

---

## Data handling
The connector performs the following operations:

1. **Runs Table**
    - Fetches experiment metadata such as:
        - Run ID
        - User
        - State
        - Creation and update timestamps
        - Tags, config, and summary metrics
    - Converts nested objects (`tags`, `config`, `summaryMetrics`) into JSON strings for storage.
    - Performs incremental filtering using the `updatedAt` field to avoid re-fetching unchanged runs.

2. **Metrics Table**
    - For each run, retrieves metric history including:
        - Step number
        - Metric name
        - Metric value
        - Timestamp
    - Each metric record is upserted to maintain a complete time-series view.

3. **Incremental Sync**
    - Tracks the latest synchronization timestamp (`runs_hwm_utc`) to fetch only new or updated runs during subsequent syncs.
    - Uses checkpointing to store this state.

4. **Checkpointing**
    - Saves synchronization progress after each run:
      ```python
      op.checkpoint({"runs_hwm_utc": current_hwm})
      ```

5. **Upserts**
    - Inserts or updates data in Fivetran using:
      ```python
      op.upsert("runs", run_record)
      op.upsert("metrics", metric_record)
      ```

---

## Error handling
The connector includes robust error handling with exponential backoff and retry mechanisms:

- **HTTP 429 / 5xx errors:** Retries up to 5 times with exponential delay.
- **Network issues:** Retries transient network errors automatically.
- **Configuration validation:** Ensures required parameters are present before running.
- **Logging:**
    - `log.info()` for normal operations.
    - `log.warning()` for recoverable errors (e.g., rate limits).
    - `log.severe()` for fatal errors.

Example log output:
```
INFO: Starting Weights & Biases connector sync
WARNING: Rate-limit/server error 429 for https://api.wandb.ai/projects/myentity/myproject/runs. Retrying in 2s...
INFO: Synced 48 runs from W&B
```

---

## Tables created
The connector creates two tables in your Fivetran destination:

### **runs**
Primary key: `run_id`

| Column | Type | Description |
|---------|------|-------------|
| `run_id` | STRING | Unique run identifier. |
| `name` | STRING | Display name of the run. |
| `state` | STRING | Run status (e.g., finished, running, failed). |
| `user` | STRING | Username of the experiment owner. |
| `created_at` | UTC_DATETIME | Run creation timestamp. |
| `updated_at` | UTC_DATETIME | Last modification timestamp. |
| `tags` | JSON | Tags associated with the run. |
| `config_json` | JSON | Configuration parameters for the run. |
| `summary_json` | JSON | Summary metrics logged by the run. |

---

### **metrics**
Primary key: (`run_id`, `step`, `metric_name`)

| Column | Type | Description |
|---------|------|-------------|
| `run_id` | STRING | Associated run ID. |
| `step` | INTEGER | Step number in the run. |
| `metric_name` | STRING | Name of the metric. |
| `metric_value` | DOUBLE | Recorded metric value. |
| `timestamp` | UTC_DATETIME | Metric timestamp. |

---

## Additional considerations
- API responses may vary based on your account type and workspace access.
- Incremental syncs help minimize API calls for large projects.
- If your project contains thousands of runs, increase `page_size` to optimize sync performance.
- This connector is designed for **educational and demonstration purposes** using the **Fivetran Connector SDK**.

For questions or feedback, please contact **Fivetran Support**.
