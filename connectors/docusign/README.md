# DocuSign eSignature Connector Example

## Connector overview

This connector extracts data from the DocuSign eSignature API. It is designed to sync key objects related to the electronic signature process, including envelopes, recipients, documents (including their binary content), audit events, and templates. The extracted data can be used in a destination to enable analytics for Sales, Legal, Operations, and other teams tracking contract lifecycles, signature status, and compliance.

## Requirements

  - [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)   
  - Operating system:
      - Windows: 10 or later (64-bit only)
      - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86\_64])
      - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86\_64)

## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

  - Extracts core DocuSign resources such as envelopes and templates their related child objects.
  - Incremental sync based on timestamp tracking.
  - Pagination and performance: uses offset-based pagination with sensible batch sizes to efficiently iterate large result sets.
  - Resiliency and partial-failure handling: per-envelope sub-resource failures are logged and skipped so a single broken item does not stop the entire sync.


## Configuration file

The configuration for this connector is defined in `configuration.json`. It is validated at the beginning of the sync by the `validate_configuration` function.

```json
{
  "access_token": "YOUR_DOCUSIGN_OAUTH_ACCESS_TOKEN",
  "base_url": "YOUR_DOCUSIGN_BASE_URL",
  "account_id": "YOUR_DOCUSIGN_ACCOUNT_ID"
}
```
Configuration parameters:
  - `access_token` (required): The OAuth2 access token for authenticating with the DocuSign API.
  - `base_url` (required): The base URL for the DocuSign API (e.g., `https://demo.docusign.net/restapi` for demo or `https://na3.docusign.net/restapi` for production).
  - `account_id` (required): The Account ID for your DocuSign account.

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file

This connector uses the `requests` library, which is pre-installed in the Fivetran environment. No additional libraries are required, so the `requirements.txt` file can be empty.

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication

This connector authenticates using an OAuth2 bearer token. The token is provided in `configuration.json` and used in the `get_docusign_headers` function to create the required `Authorization` header for all API requests.

To obtain credentials:
1.  Configure an OAuth integration within your DocuSign account (e.g., using JWT Grant or Authorization Code Grant).
2.  Generate a valid `access_token`.
3.  Find your `account_id` and correct `base_url` (e.g., `demo.docusign.net` or `na3.docusign.net`) from your DocuSign Admin panel.
4.  Add these three values to the `configuration.json` file.

## Pagination

Pagination is implemented for the two main API endpoints: `envelopes` and `templates`.

 Both functions use an offset-based pagination strategy. They send requests in a `while True` loop with a `count` of 100 and increment the `start_position` parameter by the number of records received in the previous call. The loop terminates when the API returns fewer records than the requested count, indicating the last page has been reached.

## Data handling

Data is processed within the `update` function and its various `fetch_*` helper functions.

- Schema: The connector schema, including all tables and their primary keys, is defined in the `schema` function.
- Type conversion: All data values are explicitly converted to strings (e.g., `str(envelope.get("status", ""))`) before being passed to `op.upsert` to ensure compatibility with the destination warehouse.
- Data flattening: For the `audit_events` table , the nested `eventFields` array from the API response is flattened into top-level columns in the destination table.
- Binary data: The `fetch_document_content` function downloads the binary content of documents. In the `update` function , this content is then Base64-encoded (`base64.b64encode`) and stored as a string in the `document_contents` table.
- State management: The connector uses `state.get("last_sync_time", ...)` to fetch data incrementally. At the end of a successful sync, it checkpoints the new state using `op.checkpoint(new_state)` .

## Error handling

- Configuration validation: The `validate_configuration` function  runs at the start of every sync to ensure all required configuration keys (`access_token`, `base_url`, `account_id`) are present. It raises a `ValueError` if any are missing.
- API request errors: The `make_api_request` function  uses `response.raise_for_status()` to automatically raise an exception for HTTP 4xx (client) or 5xx (server) errors.
- Resilient child object fetching: All functions that fetch data for a *specific* envelope (e.g., `fetch_audit_events`, `fetch_document_content`, `fetch_recipients_for_envelope`) are wrapped in `try...except` blocks. If an API call for one envelope's sub-resource fails, the error is logged using `logger.warning` and an empty list or `None` is returned. This prevents a single failed document or recipient from stopping the entire sync.
- Global error handling: The entire `update` function is wrapped in a `try...except Exception as e` block. Any unhandled exception will be caught and raised as a `RuntimeError`, which signals to Fivetran that the sync has failed.

## Tables created

This connector creates the following tables in the destination, as defined in the `schema` function:

  - `ENVELOPES`
  - `RECIPIENTS`
  - `ENHANCED_RECIPIENTS`
  - `AUDIT_EVENTS`
  - `ENVELOPE_NOTIFICATIONS`
  - `DOCUMENTS`
  - `DOCUMENT_CONTENTS`
  - `TEMPLATES`
  - `CUSTOM_FIELDS`

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.