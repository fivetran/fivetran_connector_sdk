# Prefect API Connector Example

This connector demonstrates how to use the [Prefect REST API](https://docs.prefect.io/latest/api-ref/rest/) with the Fivetran Connector SDK to retrieve and sync orchestration metadata — including **flows**, **flow runs**, and **deployments** — into your Fivetran destination.

It supports incremental syncs via the `updated` timestamp, automatic pagination with `limit` and `offset`, and JSON normalization for nested Prefect metadata.  
The connector works seamlessly with both **Prefect Cloud 2.x** and **self-hosted Prefect servers**.

---

## Requirements
- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
    - Windows: 10 or later (64-bit only)
    - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
    - Linux: Ubuntu 20.04+, Debian 10+, or Amazon Linux 2+ (arm64 or x86_64)

---

## Getting started
Refer to the [Fivetran Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

---

## Features
- Retrieves data from Prefect REST endpoints:
    - `/flows/filter`
    - `/flow_runs/filter`
    - `/deployments/filter`
- Incremental updates based on `updated` timestamps.
- Paginates automatically with `limit` and `offset`.
- Flattens and serializes JSON fields for database compatibility.
- Persists sync state with checkpoints for fault-tolerant incremental updates.

---

## Configuration file

The connector requires a Prefect API URL and API key for authentication.

```json
{
  "api_url": "<YOUR_PREFECT_API_URL>",
  "api_key": "<YOUR_PREFECT_API_KEY>"
}
```

**Configuration parameters**
| Parameter | Description |
|------------|-------------|
| `api_url` | Prefect REST API base URL (Cloud or self-hosted). |
| `api_key` | Prefect API key used for authentication. |
| `batch_size` | Optional, number of records to fetch per request (default: 100). |

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

---


## Authentication

Prefect APIs use **Bearer Token authentication**. The connector authenticates by including your API key in the request header:

```
Authorization: Bearer <YOUR_PREFECT_API_KEY>
```

To create a Prefect API key:
1. Log into [Prefect Cloud](https://app.prefect.cloud/).
2. Go to **Account → Profile → API Keys**.
3. Create and copy the key.
4. Paste it into your `configuration.json` file.

---

## Pagination

The connector paginates results using Prefect’s built-in parameters:
- `limit`: number of records per page.
- `offset`: pagination offset.

Pagination automatically continues until the API returns fewer than `limit` results.

---

## Data handling

The connector extracts and syncs three main Prefect entities:

### **Flows**
- Endpoint: `/flows/filter`
- Incremental syncs based on `updated`
- Schema:
  | Field | Type | Description |
  |--------|------|-------------|
  | `id` | STRING | Unique flow ID |
  | `name` | STRING | Flow name |
  | `tags` | JSON | List of tags |
  | `updated` | UTC_DATETIME | Last updated timestamp |
  | `_fivetran_synced` | UTC_DATETIME | Sync timestamp |

### **Flow Runs**
- Endpoint: `/flow_runs/filter`
- Incremental syncs based on `updated`
- Schema:
  | Field | Type | Description |
  |--------|------|-------------|
  | `id` | STRING | Unique run ID |
  | `name` | STRING | Run name |
  | `state_type` | STRING | State (Running, Completed, Failed, etc.) |
  | `flow_id` | STRING | Associated flow ID |
  | `deployment_id` | STRING | Associated deployment ID |
  | `start_time` | UTC_DATETIME | Start time |
  | `end_time` | UTC_DATETIME | End time |
  | `parameters` | JSON | Run parameters |
  | `_fivetran_synced` | UTC_DATETIME | Sync timestamp |

### **Deployments**
- Endpoint: `/deployments/filter`
- Full refresh each sync
- Schema:
  | Field | Type | Description |
  |--------|------|-------------|
  | `id` | STRING | Deployment ID |
  | `name` | STRING | Deployment name |
  | `flow_id` | STRING | Associated flow ID |
  | `schedule` | JSON | Deployment schedule configuration |
  | `updated` | UTC_DATETIME | Last updated timestamp |
  | `_fivetran_synced` | UTC_DATETIME | Sync timestamp |

---

## Error handling

The connector includes retry and recovery mechanisms for common issues:
- Retries transient HTTP errors (e.g., 429, 5xx).
- Logs skipped endpoints (404).
- Converts unexpected response types to safe defaults.
- Handles malformed or nested JSON gracefully.

Example log output:
```
INFO: Starting Prefect sync
WARNING: https://api.prefect.cloud/api/accounts/.../flows/filter not found; skipping.
INFO: Synced 150 flow runs, 30 flows, and 10 deployments
INFO: Sync completed successfully
```

---

## Tables created

The connector creates three destination tables:
1. `flows` — Flow metadata.
2. `flow_runs` — Run executions.
3. `deployments` — Deployment configurations.

---

## Additional considerations

- Works with both **Prefect Cloud 2.x** and **self-hosted Prefect API servers**.
- Incremental state ensures efficient and resumable syncs.
- Logs provide granular insights into pagination, retries, and updates.
- Designed for **educational and demonstration purposes** using the **Fivetran Connector SDK**.

For more information, see [Prefect REST API Documentation](https://docs.prefect.io/latest/api-ref/rest/).

---
