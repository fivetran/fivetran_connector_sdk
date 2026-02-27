# Coda API Connector Example

## Connector overview
This connector demonstrates how to extract and sync data from Coda Docs using the Fivetran Connector SDK. It connects to the Coda REST API (v1), authenticates via a Bearer token, and retrieves tables and rows from a specified Coda document.

It supports:
- Incremental syncs using the `updatedAt` timestamp.
- Token-based checkpointing with `nextSyncToken`.
- Pagination for large datasets using `pageToken`.
- Two sample tables: `order` and `customer_feedback`.

You can extend this connector to:
- Sync additional Coda tables.
- Modify the data transformation logic to match custom schemas.
- Consolidate multiple tables into a single destination table.

Refer to the [Coda API documentation](https://coda.io/developers/apis/v1) for more information on supported endpoints.

---

## Requirements
- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
    - Windows: 10 or later (64-bit only)
    - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
    - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

---

## Getting started
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

---

## Features
- Connects to Coda API v1 using Bearer token authentication.
- Syncs two example tables: `order` and `customer_feedback`.
- Uses `updatedAt` to perform incremental syncs.
- Supports token-based pagination with `nextSyncToken` and `pageToken`.
- Automatically checkpoints progress during long syncs.
- Handles rate limiting (HTTP 429) with exponential backoff.
- Retries failed requests up to three times on transient errors.

---

## Configuration file

```json
{
  "api_token": "<YOUR_CODA_API_TOKEN>",
  "doc_id": "<YOUR_CODA_DOC_ID>"
}
```

### Configuration Parameters

| Key | Required | Description |
|------|-----------|-------------|
| `api_token` | Yes | Your Coda API token, used for authentication. |
| `doc_id` | Yes | The unique identifier of your Coda document. |

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

---

## Authentication
Authentication is handled via a Bearer Token passed in the request headers.

Example:
```
Authorization: Bearer <YOUR_CODA_API_TOKEN>
```

You can generate your Coda API token by visiting [Coda Account Settings → API Tokens](https://coda.io/account).

---

## Pagination
Pagination is handled using Coda’s `pageToken` and `nextSyncToken` mechanisms.

- `pageToken` fetches the next batch of records from a table.
- `nextSyncToken` provides a token for incremental updates after the initial sync.
- The connector continues paging until no further `nextPageToken` is returned.

Example:
```
GET /docs/{docId}/tables/{tableId}/rows?pageToken=<next_page_token>
```

---

## Data handling
The connector processes data as follows:

- Incremental Syncs
    - The connector compares each record’s `updatedAt` against the last stored timestamp.
    - Only rows modified since the last sync are retrieved.

- Pagination
    - Each page of results is retrieved and processed in batches of 1,000 rows.

- Upserts
    - Each record is upserted using:
      ```python
      op.upsert(table=table_name, data=values)
      ```

- Checkpointing
    - State is periodically saved using:
      ```python
      op.checkpoint(state)
      ```
    - This ensures that if a sync is interrupted, it can resume from the last processed token.

- Field Normalization
    - Field names are converted to `snake_case` for consistency.
    - Example: `"First Name"` → `"first_name"`

---

## Error handling
The connector includes robust error handling:

- Rate limiting (429): Retries automatically with exponential backoff.
- Client errors (4xx): Raises an exception for invalid API tokens or bad requests.
- Server errors (5xx): Retries up to three times before failing.
- Network errors: Retries with incremental backoff delays.
- Logging: All warnings and errors are logged via `fivetran_connector_sdk.Logging`.

Example retry log:
```
Rate limit hit. Retrying in 6s...
Server error 503, retrying...
```

---

## Tables created
This connector creates two destination tables by default:

### `order`
| Field | Type | Description |
|--------|------|-------------|
| `id` | STRING | Row ID from the Coda table. |
| `region` | STRING | Sales region. |
| `rep` | STRING | Representative name. |
| `item` | STRING | Product item. |
| `units` | DOUBLE | Number of units sold. |
| `unit_cost` | DOUBLE | Unit cost of the product. |
| `total` | DOUBLE | Total revenue. |

---

### `customer_feedback`
| Field | Type | Description |
|--------|------|-------------|
| `id` | STRING | Row ID from the Coda table. |
| `customer_id` | STRING | Unique identifier for the customer. |
| `first_name` | STRING | Customer’s first name. |
| `last_name` | STRING | Customer’s last name. |
| `email_address` | STRING | Email address of the customer. |
| `number_of_complaints` | INT | Total complaints made by the customer. |

---

## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
