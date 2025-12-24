# Linear API Connector Example

## Connector overview
This connector demonstrates how to use the Linear GraphQL API with the Fivetran Connector SDK to extract and sync entities such as issues, projects, teams, users, and comments into your destination.

It highlights one of the core advanced features — Isolated Endpoint Sync, which allows each Linear entity to sync independently. This ensures that if one entity fails (for example, `issues`), other entities (`projects`, `teams`, `users`, etc.) continue syncing successfully, improving connector reliability.

This connector supports:
- Incremental syncs based on `updatedAt`.
- Soft delete detection using `archivedAt`.
- GraphQL cursor-based pagination.
- Isolated endpoint failure handling and recovery.
- Full checkpointing for resuming syncs.

The connector communicates with the [Linear API](https://developers.linear.app/docs/graphql/working-with-the-graphql-api) and requires a valid Linear API key.

---

## Requirements
- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
    - Windows: 10 or later (64-bit only)
    - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
    - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

---

## Getting started
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) for installation instructions, configuration setup, and local testing.

---

## Features
- Connects to the Linear GraphQL API securely using API keys.
- Fetches data from multiple entities (`issues`, `projects`, `teams`, `users`, `comments`).
- Performs incremental replication using `updatedAt`.
- Captures soft deletes using the `archivedAt` field.
- Implements cursor-based pagination (`hasNextPage`, `endCursor`).
- Executes Isolated Endpoint Sync — a major feature that runs each entity independently, capturing and logging individual failures without stopping other syncs.
- Tracks and persists checkpoints (`op.checkpoint()`) for resumable syncs.
- Converts datetimes into UTC ISO 8601 format for consistent data.

---

## Configuration file
The connector requires the following configuration parameters:

```json
{
  "linear_api_key": "<YOUR_LINEAR_API_KEY>"
}
```

### Configuration parameters

| Key | Required | Description |
|-----|-----------|-------------|
| `linear_api_key` | Yes | Your Linear API key used for GraphQL authentication. |

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

---

## Authentication
This connector uses Bearer Token Authentication for all Linear GraphQL requests.

Example request header:

```
Authorization: <YOUR_LINEAR_API_KEY>
Content-Type: application/json
```

### How to obtain your API key
1. Log in to your [Linear account](https://linear.app/).
2. Go to **Settings → API Keys**.
3. Generate a personal API key and copy it.
4. Add it to your `configuration.json` file under `linear_api_key`.

---

## Pagination
Pagination is handled using the GraphQL `pageInfo` object.  
The connector continues fetching data until all pages are retrieved.

- `hasNextPage`: indicates if additional pages exist.
- `endCursor`: provides the cursor token for the next page.

Example:
```graphql
query IssuesList($cursor: String, $count: Int) {
  issues(first: $count, after: $cursor, orderBy: updatedAt) {
    nodes {
      id
      createdAt
      updatedAt
      archivedAt
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
```

---

## Data handling
The connector processes and syncs Linear data using the following workflow:

1. Incremental syncs
    - Each entity (e.g., `issues`, `projects`) is queried using the `updatedAt` filter:
      ```graphql
      filter: { updatedAt: { gt: "<last_updated>" } }
      ```
    - Fetches only new or updated records since the last checkpoint.

2. Upserts
    - New or modified records are written via:
      ```python
      op.upsert(entity, record)
      ```

3. Deletes
    - Records with non-null `archivedAt` are soft-deleted via:
      ```python
      op.delete(entity, {"id": record["id"]})
      ```

4. Checkpointing
    - The connector tracks `last_updated` for each entity independently:
      ```python
      yield op.checkpoint({"issues": {"last_updated": "2025-10-26T00:00:00Z"}})
      ```

---

## Isolated endpoint sync
A major feature of this connector is Isolated Endpoint Sync.  
Each entity (issues, projects, teams, users, comments) runs independently in isolation.

### How it works:
- If one entity fails during the sync, it logs the failure without stopping the rest.
- The connector continues syncing other entities normally.
- After all syncs finish, it raises a summarized exception listing failed endpoints.

This approach ensures fault tolerance and resilience — particularly useful when dealing with large-scale or partially degraded API responses.

Example log:
```
Starting isolated sync for issues (last_updated=2025-10-25T10:00:00Z)
issues sync failed: 500 Internal Server Error
Continuing with projects, teams, and users...
IsolatedEndpointSync detected failed endpoints -> issues: 500 Internal Server Error
```

---

## Error handling
The connector implements robust error handling for API and sync operations:
- HTTP errors (4xx/5xx) – Logged and raised immediately.
- GraphQL errors – Logged with full error details and halted for that entity only.
- Network Failures – Automatically retried with incremental delays.
- Partial Failures – Isolated by entity; other syncs continue running.
- Rate Limits – The Linear API’s standard 400 requests/minute limit is respected.

---

## Tables created
The connector creates a table for each synced entity.

| Table name | Primary key | Description |
|------------|--------------|-------------|
| `ISSUES`   | `id` | Tracks issues with created, updated, and archived timestamps. |
| `PROJECTS` | `id` | Stores project metadata and lifecycle data. |
| `TEAMS`    | `id` | Contains team metadata and relationships. |
| `USERS`    | `id` | Captures user accounts and updates. |
| `COMMENTS` | `id` | Stores comment text and timestamps. |

Each table includes:
- `id`
- `createdAt`
- `updatedAt`
- `archivedAt`

---

## Additional considerations
- The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, questions, and assistance, please reach out to our Support team.- Incremental syncs depend on the `updatedAt` field — ensure it exists for all entities.
- Soft deletes use the `archivedAt` timestamp; archived records are removed from destinations.
- The connector is designed for educational and demonstration purposes using the Fivetran Connector SDK.
