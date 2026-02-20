# Dagster Connector Example

## Connector overview

This connector fetches **run**, **asset**, **schedule**, and **sensor** data from the [Dagster GraphQL API](https://docs.dagster.io/concepts/dagster-glossary/graphql) and syncs it into your Fivetran destination.  
It allows monitoring of Dagster pipeline executions, asset materializations, and schedule/sensor configurations for analytics and workflow observability.

The connector supports **incremental synchronization** for runs and automatically handles **type normalization** for list-based fields (such as tags and paths).  
It is designed to work with **Dagster Cloud** or **self-hosted Dagster** deployments.

---

## Requirements

- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating System:
    - Windows 10 or later
    - macOS 13 (Ventura) or later
    - Linux: Ubuntu 20.04+, Debian 10+, or Amazon Linux 2+ (arm64 or x86_64)
- Active **Dagster** instance or **Dagster Cloud API** endpoint
- API access with a valid **Dagster Cloud API Token** (if applicable)

---

## Getting started

Refer to the [Fivetran Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

---

## Features

- Fetches data from Dagster’s **GraphQL API** endpoints:
    - `runsOrError` for pipeline run history
    - `assetNodes` for asset metadata
    - `schedulesOrError` for active schedules
    - `sensorsOrError` for monitoring sensors
- Supports **incremental syncs** using `run_id` and `update_time`
- Automatically converts lists (tags, paths, IDs) into database-compatible JSON strings
- Handles API pagination for run results
- Retries on transient API or network errors
- Implements **checkpointing** for reliable resume
- Supports Dagster Cloud and on-prem deployments

---

## Configuration file

The connector requires configuration with your Dagster API details and authentication:

```json
{
  "base_url": "<YOUR_DAGSTER_API_URL>",
  "api_token": "<YOUR_DAGSTER_API_TOKEN>"
}
```

### Required fields:
| Key | Description |
|-----|--------------|
| `base_url` | Base URL of your Dagster GraphQL API (e.g., `https://dagster.cloud/api`). |
| `api_token` | API token for authentication (for Dagster Cloud). |

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

---

## Authentication

The connector authenticates with Dagster using an **API token** passed in the request header:
```
Dagster-Cloud-Api-Token: <YOUR_API_TOKEN>
```

For **self-hosted Dagster** instances, you can omit the API token if authentication is not required.  
For **Dagster Cloud**, the token must be generated via the Cloud console.

---

## Data handling

The connector processes data in the following way:

1. **Runs (`runsOrError`)**
    - Fetches all pipeline/job runs.
    - Extracts key metadata including run ID, job name, mode, timestamps, and tags.
    - Converts timestamp fields to ISO 8601 format.
    - Converts tag lists (`[{key,value}]`) into dictionary objects for Fivetran JSON compatibility.
    - Supports incremental sync by tracking the last processed `run_id`.

2. **Assets (`assetNodes`)**
    - Retrieves metadata for all asset nodes.
    - Converts asset key paths (lists) into dot-separated strings (e.g., `["my","asset"]` → `my.asset`).
    - Syncs asset materialization details such as last run ID and timestamp.

3. **Schedules (`schedulesOrError`)**
    - Fetches schedule configurations including cron schedule, pipeline name, and state.
    - Tracks the number of currently running instances.

4. **Sensors (`sensorsOrError`)**
    - Extracts information about sensors, their types, and associated pipelines.
    - Converts lists of run IDs into JSON strings.

5. **Checkpointing**
    - After each sync, updates the incremental cursor and checkpoints the current state using:
      ```python
      op.checkpoint(state)
      ```
    - Ensures that subsequent syncs continue from the last successful run.

---

## Error handling

The connector implements comprehensive error handling for:
- **Rate limits (429):** Retries automatically after the delay specified in the `Retry-After` header.
- **Schema mismatches:** Logs warnings and skips incompatible responses.
- **Network or HTTP errors:** Retries failed requests with exponential backoff.
- **GraphQL errors:** Captured and logged with full query context.
- **Invalid response payloads:** Automatically skipped with error logging.

All errors are recorded using the Fivetran SDK logging system for easy troubleshooting.

Example logs:
```
429 rate limited – retrying after 5s
Skipping assets due to schema mismatch: unexpected key
Dagster sync complete.
```

---

## Tables created

The connector creates four destination tables:

| Table name | Primary key | Description |
|-------------|-------------|--------------|
| `dagster_runs` | `run_id` | Metadata for pipeline runs including timestamps and status. |
| `dagster_assets` | `id` | Asset node metadata including description and materialization info. |
| `dagster_schedules` | `id` | Schedule definitions with cron expressions and state. |
| `dagster_sensors` | `id` | Sensor configurations and active run tracking. |

### Table schemas

#### `dagster_runs`
| Column | Type | Description |
|---------|------|-------------|
| `run_id` | STRING | Unique run identifier. |
| `pipeline_name` | STRING | Name of the pipeline/job. |
| `status` | STRING | Current status of the run. |
| `mode` | STRING | Execution mode for the run. |
| `start_time` | UTC_DATETIME | Run start time. |
| `end_time` | UTC_DATETIME | Run end time. |
| `update_time` | UTC_DATETIME | Last update timestamp. |
| `tags` | JSON | Run tags converted from key-value lists to JSON. |

#### `dagster_assets`
| Column | Type | Description |
|---------|------|-------------|
| `id` | STRING | Unique asset identifier. |
| `key_path` | STRING | Dot-path name of the asset (e.g., `my.asset`). |
| `description` | STRING | Asset description. |
| `last_materialization_run_id` | STRING | ID of the last materialization run. |
| `last_materialization_time` | UTC_DATETIME | Timestamp of the last materialization. |

#### `dagster_schedules`
| Column | Type | Description |
|---------|------|-------------|
| `id` | STRING | Schedule ID. |
| `name` | STRING | Schedule name. |
| `cron_schedule` | STRING | Cron expression for schedule frequency. |
| `pipeline_name` | STRING | Associated pipeline name. |
| `status` | STRING | Current schedule status. |
| `running_count` | INTEGER | Number of running instances. |

#### `dagster_sensors`
| Column | Type | Description |
|---------|------|-------------|
| `id` | STRING | Sensor ID. |
| `name` | STRING | Sensor name. |
| `sensor_type` | STRING | Type of the sensor. |
| `pipeline_name` | STRING | Pipeline monitored by the sensor. |
| `status` | STRING | Sensor status. |
| `active_run_ids` | STRING | JSON string of active run IDs. |

---

## Additional considerations

- Incremental sync is supported for `dagster_runs` using the `run_id` cursor.
- List-based fields are converted to JSON strings or key-value mappings for Fivetran compatibility.
- The connector can be extended to include **partitions**, **jobs**, or **repository-level metadata**.
- Compatible with both **Dagster Cloud** and **self-hosted Dagster** instances.
- This example is provided for **educational and demonstration purposes** using the **Fivetran Connector SDK**.

For support or feature requests, please contact **Fivetran Support**.
