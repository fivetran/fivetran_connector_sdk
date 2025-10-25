# DocuSign eSignature Connector Example

## Connector overview

This connector extracts data from the DocuSign eSignature API. It is designed to sync key objects related to the electronic signature process, including envelopes, recipients, documents (including their binary content), audit events, and templates. The extracted data can be used in a data warehouse to enable analytics for Sales, Legal, Operations, and other teams tracking contract lifecycles, signature status, and compliance.

## Requirements

  - Supported Python versions
  - Operating system:
      - Windows: 10 or later (64-bit only)
      - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86\_64])
      - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86\_64)

## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

  - **Incremental Sync:** Supports incremental updates for `envelopes` and `templates`. Envelopes are fetched based on the `last_sync_time` state, and templates are filtered based on their `lastModified` timestamp.
  - **Comprehensive Data Extraction:** Fetches envelopes and their related child objects, including:
      - `recipients`
      - `enhanced_recipients` (with more status detail)
      - `audit_events`
      - `envelope_notifications`
      - `documents` (metadata)
      - `custom_fields`
  - **Document Content:** Downloads the binary content of each document and stores it as a Base64-encoded string in the `document_contents` table. (Refer to `fetch_document_content` and lines 498-513).
  - **Calculated Fields:** Automatically calculates `contract_cycle_time_hours` for completed envelopes. (Refer to lines 405-419).

## Configuration file

The configuration for this connector is defined in `configuration.json`. It is validated at the beginning of the sync by the `validate_configuration` function.

```json
{
  "access_token": "YOUR_DOCUSIGN_OAUTH_ACCESS_TOKEN",
  "base_url": "YOUR_DOCUSIGN_BASE_URL",
  "account_id": "YOUR_DOCUSIGN_ACCOUNT_ID"
}
```

  - `access_token`: The OAuth2 access token for authenticating with the DocuSign API.
  - `base_url`: The base URL for the DocuSign API (e.g., `https://demo.docusign.net/restapi` for demo or `https://na3.docusign.net/restapi` for production).
  - `account_id`: The Account ID for your DocuSign account.

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file

This connector uses the `requests` library, which is pre-installed in the Fivetran environment. No additional libraries are required, so the `requirements.txt` file can be empty.

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication

This connector authenticates using an OAuth2 Bearer Token. The token is provided in the `configuration.json` and used in the `get_docusign_headers` function to create the required `Authorization` header for all API requests.

To obtain credentials:

1.  Configure an OAuth integration within your DocuSign account (e.g., using JWT Grant or Authorization Code Grant).
2.  Generate a valid `access_token`.
3.  Find your `account_id` and correct `base_url` (e.g., `demo.docusign.net` or `na3.docusign.net`) from your DocuSign Admin panel.
4.  Add these three values to the `configuration.json` file.

## Pagination

Pagination is implemented for the two main data streams: `envelopes` and `templates`.

  - Refer to `fetch_envelopes` (lines 142-169) and `fetch_templates` (lines 280-311).
  - Both functions use an offset-based pagination strategy. They send requests in a `while True` loop with a `count` of 100 and increment the `start_position` parameter by the number of records received in the previous call. The loop terminates when the API returns fewer records than the requested count, indicating the last page has been reached.

## Data handling

Data is processed within the `update` function and its various `fetch_*` helper functions.

  - **Schema:** The connector schema, including all tables and their primary keys, is defined in the `schema` function (lines 51-93).
  - **Type Conversion:** All data values are explicitly converted to strings (e.g., `str(envelope.get("status", ""))`) before being passed to `op.upsert` to ensure compatibility with the destination warehouse.
  - **Data Flattening:** For the `audit_events` table (lines 450-456), the nested `eventFields` array from the API response is flattened into top-level columns in the destination table.
  - **Binary Data:** The `fetch_document_content` function downloads the binary content of documents. In the `update` function (lines 498-513), this content is then Base64-encoded (`base64.b64encode`) and stored as a string in the `document_contents` table.
  - **State Management:** The connector uses `state.get("last_sync_time", ...)` to fetch data incrementally. At the end of a successful sync, it checkpoints the new state using `op.checkpoint(new_state)` (lines 526-527).

## Error handling

  - **Configuration Validation:** The `validate_configuration` function (lines 35-48) runs at the start of every sync to ensure all required configuration keys (`access_token`, `base_url`, `account_id`) are present. It raises a `ValueError` if any are missing.
  - **API Request Errors:** The `make_api_request` function (lines 103-113) uses `response.raise_for_status()` to automatically raise an exception for HTTP 4xx (client) or 5xx (server) errors.
  - **Resilient Child Object Fetching:** All functions that fetch data for a *specific* envelope (e.g., `fetch_audit_events`, `fetch_document_content`, `fetch_recipients_for_envelope`) are wrapped in `try...except` blocks. If an API call for one envelope's sub-resource fails, the error is logged using `logger.warning` and an empty list or `None` is returned. This prevents a single failed document or recipient from stopping the entire sync.
  - **Global Error Handling:** The entire `update` function is wrapped in a `try...except Exception as e` block (lines 529-531). Any unhandled exception will be caught and raised as a `RuntimeError`, which signals to Fivetran that the sync has failed.

## Tables created

This connector creates the following tables in the destination, as defined in the `schema` function:

  - `envelopes`
  - `recipients`
  - `enhanced_recipients`
  - `audit_events`
  - `envelope_notifications`
  - `documents`
  - `document_contents`
  - `templates`
  - `custom_fields`

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.