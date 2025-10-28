# Dune API Connector Example

## Connector overview

This connector retrieves query results from the [Dune API](https://dune.com/docs/api/) and syncs them into your Fivetran destination.  
It allows you to execute one or multiple Dune queries, monitor their execution status, and fetch the final results into a unified destination table.

The connector supports:
- Running multiple queries sequentially.
- Tracking execution status (`RUNNING`, `COMPLETED`, `FAILED`).
- Rate limit handling (HTTP 429).
- Incremental sync management using checkpointing.

---

## Requirements

- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
    - Windows: 10 or later (64-bit only)
    - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
    - Linux: Ubuntu 20.04+, Debian 10+, or Amazon Linux 2+ (arm64 or x86_64)
- A valid **Dune API Key**.
- Pre-created **query IDs** in your Dune account.

---

## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) for setup instructions.

---

## Features

- Executes one or many queries using Dune’s REST API.
- Polls query execution status until completion or failure.
- Fetches results from completed queries.
- Handles **rate limiting (HTTP 429)** with exponential backoff.
- Stores all query results in a single table for streamlined analysis.
- Supports checkpointing for state tracking and recovery.
- Logs progress, errors, and performance metrics for visibility.

---

## Configuration file

The connector requires the following configuration parameters:

```json
{
  "DUNE_API_KEY": "<YOUR_DUNE_API_KEY>",
  "DUNE_QUERY_IDS": "<YOUR_DUNE_QUERY_IDS>"
}
```

### Configuration parameters

| Parameter | Description |
|------------|-------------|
| `DUNE_API_KEY` | Your Dune API key for authentication. |
| `DUNE_QUERY_IDS` | Comma-separated list of Dune query IDs to execute and sync. |

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

---

## Authentication

The connector uses **API Key authentication** for secure access to the Dune API.

Each request includes the following header:
```
X-DUNE-API-KEY: <YOUR_DUNE_API_KEY>
```

### How to obtain credentials:

1. Log in to your [Dune account](https://dune.com/).
2. Navigate to **Settings → API Keys**.
3. Create a new API key (or reuse an existing one).
4. Add the API key to your `configuration.json`.
5. Copy your Dune **query IDs** from the URL of your Dune queries.

---

## Pagination

The Dune API manages pagination internally for query results. The connector automatically retrieves all rows returned by a completed query execution.

- Each query run creates a unique `execution_id`.
- The connector repeatedly checks the execution status until it reaches a terminal state (`COMPLETED` or `FAILED`).
- Results are fetched from `/execution/{execution_id}/results` once completed.
- Each query execution is logged, and progress is checkpointed for recovery.

---

## Data handling

The connector executes the following flow for each configured query:

1. **Execution**
    - Triggers a new query run via:
      ```python
      POST /query/{query_id}/execute
      ```
    - Stores the generated `execution_id`.

2. **Polling**
    - Periodically checks query status via:
      ```python
      GET /execution/{execution_id}/status
      ```
    - Waits until state is `COMPLETED` or `FAILED`.

3. **Fetching results**
    - Retrieves final query results via:
      ```python
      GET /execution/{execution_id}/results
      ```

4. **Upserting results**
    - Each row is upserted into the `dune_query_result` table:
      ```python
      op.upsert("dune_query_result", row_data)
      ```
    - Includes metadata such as `query_id`, `execution_id`, `row_id`, and sync timestamp.

5. **Checkpointing**
    - After all queries are processed, the connector checkpoints the current sync state:
      ```python
      op.checkpoint(state={"synced_at": current_time})
      ```

---

## Error handling

The connector includes robust error handling for:
- **Rate limiting (HTTP 429):** Waits for `Retry-After` delay before retrying.
- **API failures (4xx / 5xx):** Retries with exponential backoff.
- **Network issues:** Retries failed connections automatically.
- **Invalid Query IDs:** Logs warnings and skips invalid entries.
- **Query failures:** Captures `FAILED` query states and continues with the next query.

Example log output:
```
INFO: Executing Dune query 123456 …
INFO: Query 123456 → QUERY_STATE_RUNNING
INFO: Query 123456 → QUERY_STATE_COMPLETED
INFO: Fetched 500 rows for query 123456
INFO: Dune sync completed
```

---

## Tables created

The connector creates a single destination table to store all Dune query results:

### **dune_query_result**
Primary key: `query_id`, `execution_id`, `row_id`

| Column | Type | Description |
|---------|------|-------------|
| `query_id` | LONG | The Dune query ID executed. |
| `execution_id` | STRING | The unique execution identifier for the query run. |
| `row_id` | LONG | Row index for each result. |
| `data` | JSON | The JSON-formatted row data returned by the query. |
| `synced_at` | UTC_DATETIME | The timestamp when the row was synced. |

---

## Additional considerations

- You can configure multiple query IDs in the `DUNE_QUERY_IDS` field for batch execution.
- The connector is stateless for result storage but uses checkpointing for incremental updates.
- Rate limits depend on your Dune API plan.
- Large query results may take several polling cycles to complete.
- Designed for **educational and demonstration purposes** using the **Fivetran Connector SDK**.

For support or inquiries, please contact **Fivetran Support**.
