# Temporal Cloud Connector Example

## Connector overview

This connector fetches **workflow execution data** from **Temporal Cloud** using the **Temporal Python SDK** and syncs it to your Fivetran destination.  
It retrieves details about workflow executions, including workflow identifiers, status, timestamps, and custom attributes.

The connector maintains one table, `temporal_workflows`, which contains workflow execution data from Temporal namespaces.  
It supports **incremental synchronization** using the `close_time_unix` cursor and includes **checkpointing** for state management.

---

## Requirements

- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
   - Windows: 10 or later (64-bit only)
   - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
   - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)
- Temporal Cloud account with **API key** or **mTLS certificates**
- Access to Temporal Cloud endpoint (e.g., `<namespace_id>.<account_id>.tmprl.cloud:7233`)

---

## Getting started

Refer to the [Fivetran Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) for installation and testing steps.  

---

## Features

- Connects securely to Temporal Cloud via **API Key** or **mTLS** authentication.
- Fetches **workflow executions** from the Temporal Visibility API.
- Supports incremental syncs using the `close_time_unix` field.
- Converts Temporal datetime objects into UTC ISO 8601 strings.
- Automatically manages **checkpointing** for resumable syncs.
- Implements retry logic with backoff for transient connection errors.
- Asynchronous data fetching for efficient performance.

---

## Configuration file

The connector requires the following configuration parameters:

```json
{
  "endpoint": "<YOUR_TEMPORAL_CLOUD_ENDPOINT>",
  "namespace": "<YOUR_TEMPORAL_CLOUD_NAMESPACE>",
  "api_key": "<YOUR_TEMPORAL_CLOUD_API_KEY>",
  "tls": "<YOUR_TLS_OPTION>",
  "page_size": "<YOUR_PAGE_SIZE>"
}
```

### Configuration parameters

| Key | Required | Description |
|------|-----------|-------------|
| `endpoint` | Yes | Temporal Cloud endpoint (e.g., `<namespace_id>.<account_id>.tmprl.cloud:7233`). |
| `namespace` | Yes | Namespace identifier for your Temporal workflows. |
| `api_key` | Conditional | Required if using API key authentication. |
| `tls` | Optional | Set to `true` to enable TLS (default: `true`). |
| `page_size` | No | Number of workflows to fetch per page (default: 1000). |

> NOTE: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

---

## Requirements file

The connector requires the following Python dependencies:

```
temporalio
```

> **Note:** The `fivetran_connector_sdk` package is pre-installed in the Fivetran environment. Include `temporalio` only for local testing.

---

## Authentication

The connector supports two authentication methods:

1. **API Key (recommended)**
    - Pass your Temporal API key directly in the `configuration.json` file.
    - Example:
      ```json
      "api_key": "<YOUR_TEMPORAL_API_KEY>"
      ```

2. **mTLS (mutual TLS)**
    - Use client and server certificates for secure, certificate-based authentication.
    - Provide Base64 or PEM-encoded certificates and keys.

---

## Data handling

The connector processes Temporal workflow execution data as follows:

1. **Connection Setup**
    - Establishes a connection to Temporal Cloud using either API key or mTLS authentication via the Temporal Python SDK.

2. **Incremental Sync**
    - Retrieves workflow executions with `StartTime >= last_cursor` or `CloseTime >= last_cursor`.
    - Tracks progress using the `close_time_unix` or `start_time_unix` timestamp as the incremental cursor.

3. **Upsert Operations**
    - Inserts or updates each workflow record:
      ```python
      op.upsert(table="temporal_workflows", data=row)
      ```

4. **Checkpointing**
    - After every sync, saves state with the latest timestamp per namespace:
      ```python
      op.checkpoint(state)
      ```

5. **Structured Attributes**
    - Parses search attributes and memo fields into plain JSON structures for easy querying.
    - Encodes any binary data as Base64 strings.

6. **Data Conversion**
    - Converts all timestamps (start/close times) to UTC ISO 8601 strings.
    - Stores original UNIX epoch timestamps in integer form for analytics.

---

## Error handling

The connector implements comprehensive error handling for:
- Network timeouts and retries with exponential backoff.
- Authentication errors for invalid API keys or TLS certificates.
- Rate limits using async I/O with controlled page size.
- Serialization errors in memo or attribute payloads (logged as warnings).
- Partial failures — logs the error but continues syncing other records.

All exceptions and retry attempts are logged using the Fivetran logging system.

Example log entries:
```
Connecting to Temporal Cloud (namespace: demo.production)
Rate limit encountered – retrying after 2 seconds...
Temporal Cloud sync complete. Rows: 1000, cursor(ns=demo.production)=1730000000
```

---

## Tables created

The connector creates one destination table:

### `temporal_workflows`
Primary key: (`workflow_id`, `run_id`)

| Column | Type | Description |
|--------|------|-------------|
| `namespace` | STRING | Temporal namespace identifier. |
| `workflow_id` | STRING | Unique workflow execution ID. |
| `run_id` | STRING | Unique run identifier for a workflow. |
| `workflow_type` | STRING | Workflow type name. |
| `task_queue` | STRING | Task queue where the workflow executes. |
| `start_time` | UTC_DATETIME | Workflow start time in UTC. |
| `close_time` | UTC_DATETIME | Workflow close time in UTC. |
| `start_time_unix` | INTEGER | Workflow start time (Unix timestamp). |
| `close_time_unix` | INTEGER | Workflow close time (Unix timestamp). |
| `status` | STRING | Workflow execution status (e.g., RUNNING, COMPLETED, FAILED). |
| `history_length` | INTEGER | Number of workflow events in history. |
| `search_attributes` | JSON | Key-value pairs of custom workflow search attributes. |
| `memo` | JSON | Memo field with additional metadata. |
| `raw` | JSON | Full raw workflow execution record. |

---

## Additional considerations

- The connector uses asynchronous I/O to maximize throughput in large Temporal namespaces.
- Incremental syncs depend on accurate `close_time` or `start_time` timestamps — ensure Temporal Visibility API access is enabled.
- This example can be extended to fetch **activities**, **signals**, or **queries** by parsing workflow history events.
- Designed for **educational and demonstration purposes** using the **Fivetran Connector SDK**.

For questions or feedback, contact **Fivetran Support**.
