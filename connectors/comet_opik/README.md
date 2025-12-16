# Comet Opik Connector Example

## Connector overview

This connector retrieves observability data from the [Comet Opik API](https://www.comet.com/opik) and syncs it into your Fivetran destination.  
It pulls metadata about projects, datasets, and dataset items from Comet Opik’s private REST API to enable monitoring, analytics, and reporting on machine learning data assets.

The connector supports:
- Secure authentication via API key and workspace headers.
- Automatic pagination and rate limit handling.
- Incremental syncs using checkpoints for efficient data ingestion.

---

## Requirements

- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating System:
  - Windows 10 or later
  - macOS 13 (Ventura) or later
  - Linux: Ubuntu 20.04+, Debian 10+, or Amazon Linux 2+ (arm64 or x86_64)
- Active Comet Opik account with API access.
- A valid Comet Opik API key and workspace name.

---

## Getting started

Refer to the [Fivetran Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) for installation and setup instructions.

---

## Features

- Connects securely to Comet Opik via API key and workspace headers.
- Fetches:
    - Projects with metadata such as creator, timestamps, and visibility.
    - Datasets within each workspace.
    - Dataset items (inputs, outputs, metadata) for each dataset.
- Handles rate limiting (HTTP 429) and retries with exponential backoff.
- Implements checkpointing for reliable and resumable syncs.
- Converts timestamps to ISO 8601 UTC format.
- Logs detailed information for monitoring and troubleshooting.

---

## Configuration file

The connector requires the following configuration parameters:

```json
{
  "COMET_OPIK_API_KEY": "<YOUR_COMET_OPIK_API_KEY>",
  "COMET_OPIK_WORKSPACE": "<YOUR_COMET_OPIK_WORKSPACE>"
}
```

## Configuration parameters
- COMET_OPIK_API_KEY: Your Comet Opik API key for authorization.
- COMET_OPIK_WORKSPACE: The workspace name used to scope your API access.

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

---

## Authentication

The connector uses header-based authentication to connect securely to the Comet Opik API.

Example headers:
```
Authorization: <YOUR_COMET_OPIK_API_KEY>
Comet-Workspace: <YOUR_COMET_OPIK_WORKSPACE>
Accept: application/json
User-Agent: fivetran-connector-sdk-opik/1.1
```

## How to obtain credentials:
1. Log in to your Comet Opik account.
2. Navigate to **API Settings** → **API Keys**.
3. Create or copy your existing API key.
4. Identify your workspace name from your account dashboard.
5. Add both values to your `configuration.json`.

---

## Pagination

The connector automatically handles pagination for endpoints that return paginated results (e.g., `/projects`, `/datasets`, `/datasets/{id}/items`).  
If an endpoint does not support pagination parameters, the connector gracefully falls back to a single batch fetch.

Pagination behavior:
- Uses the `content` or `items` fields in the JSON response.
- Continues until all results are retrieved.
- Logs progress for each page.

---

## Data handling

The connector performs the following operations during synchronization:

- Projects
    - Fetched via `/projects`.
    - Extracts fields such as `id`, `name`, `visibility`, timestamps, and creator.
    - Upserts rows into the `opik_project` table.

- Datasets
    - Fetched via `/datasets`.
    - Captures dataset-level metadata such as `id`, `name`, visibility, and creation timestamps.
    - Upserts into the `opik_dataset` table.

- Dataset Items
    - For each dataset, retrieves dataset items via `/datasets/{dataset_id}/items`.
    - Extracts `inputs`, `outputs`, and `metadata`.
    - Stores results in the `opik_dataset_item` table.

- Checkpointing
    - Saves progress using:
      ```python
      op.checkpoint(state)
      ```
    - Ensures state persistence and recovery from the last successful sync.

---

## Error Handling

The connector includes robust error handling and retry mechanisms:
- Rate Limits (HTTP 429): Automatically waits and retries after the `Retry-After` delay.
- HTTP Errors: Retries failed requests up to three times with exponential backoff.
- Network Issues: Logs network exceptions and retries safely.
- Schema Changes: Logs warnings when response fields change or are missing.
- Timeouts: Each request has a default timeout of 60 seconds.

All errors are logged via the Fivetran logging system for visibility and diagnostics.

Example log output:
```
Rate limited, sleeping 5s
Syncing Comet Opik datasets …
Comet Opik sync complete
```

---

## Tables Created

The connector creates three destination tables:

## 1. opik_project
Primary key: `project_id`

| Column | Type | Description |
|---------|------|-------------|
| `project_id` | STRING | Unique project identifier. |
| `name` | STRING | Project name. |
| `visibility` | STRING | Access level of the project. |
| `created_at` | UTC_DATETIME | Project creation timestamp. |
| `created_by` | STRING | Creator of the project. |
| `last_updated_at` | UTC_DATETIME | Last updated timestamp. |
| `last_updated_by` | STRING | Last modifier of the project. |
| `raw` | JSON | Raw API response for the project. |

---

## 2. opik_dataset
Primary key: `dataset_id`

| Column | Type | Description |
|---------|------|-------------|
| `dataset_id` | STRING | Unique dataset identifier. |
| `name` | STRING | Dataset name. |
| `visibility` | STRING | Dataset access level. |
| `created_at` | UTC_DATETIME | Creation timestamp. |
| `created_by` | STRING | Dataset creator. |
| `last_updated_at` | UTC_DATETIME | Last update timestamp. |
| `last_updated_by` | STRING | Last modifier. |
| `raw` | JSON | Full raw JSON payload. |

---

## 3. opik_dataset_item
Primary key: `item_id`

| Column | Type | Description |
|---------|------|-------------|
| `item_id` | STRING | Unique dataset item ID. |
| `dataset_id` | STRING | Parent dataset identifier. |
| `inputs` | JSON | Input data associated with the dataset item. |
| `outputs` | JSON | Output data of the dataset item. |
| `metadata` | JSON | Additional metadata for the dataset item. |
| `created_at` | UTC_DATETIME | Creation timestamp. |
| `raw` | JSON | Raw API response. |

---

## Additional Considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
