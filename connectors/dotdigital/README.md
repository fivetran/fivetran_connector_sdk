# **Dotdigital Connector Example**

## **Connector Overview**

This connector demonstrates how to build a Fivetran Connector SDK integration for the **Dotdigital marketing automation platform**.  
It connects to Dotdigital’s REST APIs (v2 and v3) using **Basic Authentication**, retrieves **contacts** and **campaigns**, and syncs them into your Fivetran destination.

It illustrates region autodiscovery, pagination, and rate-limiting best practices.

---

## **Requirements**

* [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
* Operating System:
  * Windows 10 or later
  * macOS 13 (Ventura) or later
  * Linux (Ubuntu 20.04+, Debian 10+, or Amazon Linux 2+)

---

## **Getting Started**

Refer to the [Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to learn how to install dependencies, configure the connector, and run it locally or in the Fivetran environment.

---

## **Features**

- Connects to Dotdigital’s REST APIs (v2 and v3).
- Supports **region autodiscovery** for API endpoints.
- Implements **pagination** using:
  - Marker tokens for v3 API (seek pagination).
  - Skip/limit for v2 API (offset pagination).
- Implements **rate-limit handling** via HTTP 429 headers.
- Performs **incremental syncs** using checkpoints.
- Extracts and syncs **contacts** (v3) and **campaigns** (v2).
- Persists state for resumable data extraction.

---

## **Configuration File**

The connector requires Dotdigital API credentials and optional configuration parameters for pagination and region detection.

```json
{
  "DOTDIGITAL_API_USER": "<YOUR_DOT_DIGITAL_API_USER_NAME>",
  "DOTDIGITAL_API_PASSWORD": "<YOUR_DOT_DIGITAL_API_PASSWORD>",
  "DOTDIGITAL_REGION_ID": "<YOUR_REGION_ID>",
  "AUTO_DISCOVER_REGION": "<YOUR_AUTO_DISCOVER_REGION_OPTION>",
  "CONTACTS_START_TIMESTAMP": "<YOUR_START_TIME>",
  "CONTACTS_PAGE_SIZE": "<YOUR_PAGE_SIZE>",
  "V2_PAGE_SIZE": "<YOUR_V2_PAGE_SIZE>",
  "CAMPAIGNS_ENABLED": "<YOUR_CAMPAIGNS_ENABLED_OPTION>"
}
```

### Configuration Parameters

| Key | Required | Description |
|------|-----------|-------------|
| `DOTDIGITAL_API_USER` | Yes | Your Dotdigital API username. |
| `DOTDIGITAL_API_PASSWORD` | Yes | Your Dotdigital API password. |
| `DOTDIGITAL_REGION_ID` | No | API region ID (e.g., `r1`, `r2`, `r3`). Defaults to `r1`. |
| `AUTO_DISCOVER_REGION` | No | Set to `true` to automatically detect your account’s region. |
| `CONTACTS_PAGE_SIZE` | No | Number of contacts per page (default: 500). |
| `V2_PAGE_SIZE` | No | Page size for campaign API calls (default: 1000). |
| `CAMPAIGNS_ENABLED` | No | Set to `false` to disable campaign syncs. |

Note: Ensure that the configuration.json file is not checked into version control to protect sensitive information.

---

## **Authentication**

The connector uses **Basic Authentication** for API requests.

The `DOTDIGITAL_API_USER` and `DOTDIGITAL_API_PASSWORD` are combined and Base64 encoded as follows:

```
Authorization: Basic base64(<API_USER>:<API_PASSWORD>)
```

### Region Autodiscovery

- The connector first connects to `https://r1-api.dotdigital.com/v2/account-info`.
- It then extracts the correct regional API endpoint from the response.
- Supported regions include `r1`, `r2`, `r3`, and `r4`.
- If autodiscovery fails, the configured `DOTDIGITAL_REGION_ID` is used.

---

## **Pagination**

The connector handles pagination differently for v2 and v3 APIs:

- **v3 (Contacts API)**:  
  Uses seek-based pagination with a **marker token**.  
  Example:
  ```
  GET /contacts/v3?limit=500&marker=<next_marker>
  ```

- **v2 (Campaigns API)**:  
  Uses skip/limit pagination.  
  Example:
  ```
  GET /v2/campaigns?select=1000&skip=0
  ```

Pagination continues until no more records are returned.

---

## **Data Handling**

The connector processes two main data entities:

1. **Contacts (v3 API)**
  - Extracts contacts using seek pagination.
  - Filters contacts updated after the last sync checkpoint (`~modified >= last_sync`).
  - Maps nested fields such as identifiers, lists, and preferences to JSON columns.
  - Performs upserts using `op.upsert("dotdigital_contact", data=row)`.

2. **Campaigns (v2 API)**
  - Extracts campaign metadata including `id`, `name`, `subject`, and timestamps.
  - Uses skip-based pagination.
  - Syncs campaigns via `op.upsert("dotdigital_campaign", data=row)`.

3. **Checkpointing**
  - Contacts: Saves last `updated` timestamp as `contacts_cursor`.
  - Campaigns: Saves current UTC timestamp as `campaigns_synced_at`.

---

## **Error Handling**

The connector includes robust error handling for HTTP requests and rate limiting:

- **429 Too Many Requests:** Retries automatically using the `X-RateLimit-Reset` header to calculate backoff time.
- **Network or Timeout Errors:** Retries with exponential backoff.
- **Authentication Failures (401/403):** Stops sync and logs the issue.
- **Region Detection Errors:** Logs warning and falls back to the configured region.
- **Partial Data Failures:** Logs errors without interrupting the sync process.

All logs are handled via the `fivetran_connector_sdk.Logging` interface.

---

## **Tables Created**

The connector creates the following tables in your destination:

| Table name | Primary key | Description |
|-------------|--------------|-------------|
| `dotdigital_contact` | `contact_id` | Stores contact records including email, mobile number, and preferences. |
| `dotdigital_campaign` | `campaign_id` | Stores marketing campaign metadata. |

Each table includes structured fields for timestamps and nested JSON columns.

---

## **Additional Considerations**

- Ensure your API credentials have read access to contacts and campaigns endpoints.
- Dotdigital rate limits API usage to 180 requests per minute; the connector respects this limit automatically.
- Set `AUTO_DISCOVER_REGION` to `true` for dynamic region resolution.
- Incremental syncs are idempotent and can be safely re-run.
- The connector is designed for **educational and reference purposes** using the Fivetran Connector SDK.

For support or further guidance, contact **Fivetran Support**.
