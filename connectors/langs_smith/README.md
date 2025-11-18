# LangSmith Connector Example

## Connector overview
The **LangSmith** connector fetches data from the [LangSmith REST API](https://docs.smith.langchain.com/) to sync datasets, examples, and run records into your Fivetran destination.  
It enables monitoring and analysis of model runs, dataset performance, and experiment tracking across LangChain applications.

The connector retrieves and syncs data for:
- **Datasets** — Metadata for datasets defined in LangSmith.
- **Examples** — Input/output pairs and metadata within each dataset.
- **Runs** — Execution logs of model runs including inputs, outputs, status, and timing.

It supports **incremental synchronization** using timestamps and implements retry and checkpoint mechanisms to ensure data reliability.

---

## Requirements
- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
    - Windows: 10 or later (64-bit only)
    - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
    - Linux: Ubuntu 20.04+, Debian 10+, or Amazon Linux 2+ (arm64 or x86_64)
- Active **LangSmith account** and API access enabled
- API Key and Tenant ID credentials for LangSmith

---

## Getting started
Refer to the [Fivetran Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) for setup and debugging instructions.

---

## Features
- Fetches **datasets**, **examples**, and **runs** from the LangSmith REST API.
- Supports incremental synchronization based on run start timestamps.
- Automatically handles rate limits (HTTP 429) and transient errors.
- Uses retry logic for failed requests.
- Checkpoints progress for resumable syncs.
- Configurable sync for datasets, examples, and runs.
- Supports JSON serialization for nested fields.

---

## Configuration file
The connector requires the following configuration parameters in the `configuration.json` file:

```json
{
  "LANGSMITH_API_KEY": "<YOUR_API_KEY>",
  "LANGSMITH_TENANT_ID": "<YOUR_TENANT_ID>",
  "PAGE_SIZE": "<YOUR_PAGE_SIZE>",
  "SYNC_EXAMPLES": "<YOUR_SYNC_EXAMPLES_OPTION>",
  "SYNC_RUNS": "<YOUR_SYNC_RUNS_OPTION>"
}
```

### Required fields:
| Key | Description |
|-----|--------------|
| `LANGSMITH_API_KEY` | API key for LangSmith authentication. |
| `LANGSMITH_TENANT_ID` | Tenant ID for LangSmith organization. |

### Optional fields:
| Key | Description |
|-----|--------------|
| `LANGSMITH_BASE_URL` | Override default base API URL (default: `https://api.smith.langchain.com`). |
| `LANGSMITH_ORG_ID` | Organization ID (optional for some API scopes). |
| `PAGE_SIZE` | Number of records to fetch per API call (default: 200). |
| `SYNC_EXAMPLES` | Enable or disable syncing examples (default: true). |
| `SYNC_RUNS` | Enable or disable syncing runs (default: true). |

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

---

## Authentication
The connector uses **API Key authentication** with LangSmith’s REST API.  
Each request must include the following headers:

```
x-api-key: <YOUR_API_KEY>
x-tenant-id: <YOUR_TENANT_ID>
accept: application/json
```

To obtain your credentials:
1. Log in to your [LangSmith account](https://smith.langchain.com/).
2. Go to **Account Settings → API Keys**.
3. Create or copy your existing API Key and Tenant ID.
4. Add them to your `configuration.json` file.

---

## Pagination
The connector automatically handles pagination for all endpoints:
- Fetches data using a configurable `PAGE_SIZE` parameter.
- Continues paginating until all records are retrieved.
- Uses `limit` query parameters for batch retrieval (default: 200).

Pagination is implemented for both `/runs` and `/examples` endpoints, ensuring large datasets and experiment logs are fully synchronized.

---

## Data handling
The connector processes LangSmith data in three categories:

1. **Datasets**
    - Fetches dataset metadata from `/datasets`.
    - Upserts data into the `langsmith_dataset` table.
    - Includes attributes such as `name`, `description`, and timestamps.

2. **Examples**
    - Fetches examples associated with each dataset via `/examples`.
    - Syncs input/output data for experiments and model responses.
    - Converts timestamps to ISO 8601 format.
    - Upserts into `langsmith_example`.

3. **Runs**
    - Retrieves execution logs from `/runs`.
    - Uses incremental sync based on `start_time`.
    - Includes inputs, outputs, tags, errors, and project metadata.
    - Upserts into `langsmith_run`.

4. **Checkpointing**
    - After syncing all data, the connector checkpoints using:
      ```python
      op.checkpoint(state)
      ```
    - Stores the last synced timestamp in state to resume future syncs without duplication.

---

## Error handling
The connector implements robust error handling for:
- **Rate limits (HTTP 429):** Retries after the `Retry-After` delay.
- **Client errors (4xx):** Logs warnings and skips invalid requests.
- **Server errors (5xx):** Retries failed requests up to three times.
- **Timeouts and network issues:** Retries with backoff.
- **Empty responses:** Gracefully continues processing.

All errors are logged using the Fivetran logging system for monitoring and debugging.

Example log output:
```
Rate limited 429. Sleeping 5s
Syncing LangSmith runs...
LangSmith sync complete.
```

---

## Tables created

This connector creates three tables in your destination:

| Table name | Primary key | Description |
|-------------|-------------|--------------|
| `langsmith_dataset` | `dataset_id` | Metadata for LangSmith datasets. |
| `langsmith_example` | `example_id` | Example-level data (inputs, outputs, metadata). |
| `langsmith_run` | `run_id` | Execution logs of LangChain runs, including errors and statuses. |

### Table Schemas

#### `langsmith_dataset`
| Column | Type | Description |
|---------|------|-------------|
| `dataset_id` | STRING | Unique identifier for the dataset. |
| `name` | STRING | Dataset name. |
| `description` | STRING | Dataset description. |
| `created_at` | UTC_DATETIME | Creation timestamp. |
| `updated_at` | UTC_DATETIME | Last modified timestamp. |
| `raw` | JSON | Full raw API response for auditing. |

#### `langsmith_example`
| Column | Type | Description |
|---------|------|-------------|
| `example_id` | STRING | Unique identifier for the example. |
| `dataset_name` | STRING | Name of the parent dataset. |
| `created_at` | UTC_DATETIME | Creation timestamp. |
| `updated_at` | UTC_DATETIME | Last modified timestamp. |
| `inputs` | JSON | Input data for the example. |
| `outputs` | JSON | Output data from the example. |
| `metadata` | JSON | Additional metadata. |
| `raw` | JSON | Full raw API response. |

#### `langsmith_run`
| Column | Type | Description |
|---------|------|-------------|
| `run_id` | STRING | Unique identifier for the run. |
| `project_name` | STRING | Associated LangSmith project name. |
| `run_type` | STRING | Run type (chain, llm, retriever, etc.). |
| `start_time` | UTC_DATETIME | Start time of the run. |
| `end_time` | UTC_DATETIME | End time of the run. |
| `name` | STRING | Run name. |
| `status` | STRING | Run execution status (success, failed, etc.). |
| `inputs` | JSON | Input parameters for the run. |
| `outputs` | JSON | Output data produced by the run. |
| `error` | STRING | Error message if the run failed. |
| `tags` | JSON | Tags associated with the run. |
| `raw` | JSON | Full raw API response. |

---

## Additional considerations
- Incremental synchronization depends on the `start_time` timestamp for runs.
- Rate-limiting and backoff ensure compliance with LangSmith API limits.
- The connector can be extended to include additional endpoints such as **feedback** or **traces**.
- Designed for **educational and demonstration purposes** using the **Fivetran Connector SDK**.

For questions or support, please contact **Fivetran Support**.
