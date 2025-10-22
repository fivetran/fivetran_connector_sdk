# Dotdigital Connector Example

## Connector overview
The Dotdigital connector demonstrates how to use the Fivetran Connector SDK to extract marketing data from the [Dotdigital API](https://developer.dotdigital.com/). The connector retrieves both **contacts** and **campaigns** data from Dotdigital and loads them into a Fivetran destination for downstream analysis.

It maintains two tables in the destination:
- `dotdigital_contacts` – stores subscriber details, consent status, and custom data fields.  
- `dotdigital_campaigns` – stores email campaign information such as name, subject line, send date, and HTML content.

The connector supports **incremental syncing** using `date_created` or `date_modified` timestamps to ensure only new or updated records are extracted on each sync.

This example helps developers understand how to build and maintain stateful connectors that interact with REST APIs using pagination and authentication.

## Requirements
- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)   
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started. The guide explains how to configure the connector, set up dependencies, and test it locally using the `fivetran debug` command.

## Features
- Retrieves data from Dotdigital’s **v2 REST API**.  
- Supports **incremental syncing** via `date_created` and `date_modified` timestamps.  
- Automatically paginates through large datasets using `page` and `count` parameters.  
- Dynamically maps custom contact fields into JSON objects.  
- Includes retry handling for rate limits and transient API errors.  
- Normalizes contacts and campaigns into joinable relational tables.  

## Configuration file
The connector uses a configuration file, `configuration.json`, to define API access credentials and settings.

```
{
  "api_base_url": "https://r1-api.dotdigital.com/v2",
  "api_user": "YOUR_API_USER",
  "api_password": "YOUR_API_PASSWORD",
  "cursor_field": "date_modified",
  "page_size": 1000
}
```

| Key | Required | Description |
|-----|-----------|-------------|
| `api_base_url` | Yes | The base URL for your Dotdigital API region (e.g. `r1`, `r2`, `r3`). |
| `api_user` | Yes | Your Dotdigital API username (created in **User Settings → Access**). |
| `api_password` | Yes | Your Dotdigital API password. |
| `cursor_field` | No | Field used for incremental syncs (`date_created` or `date_modified`). |
| `page_size` | No | Number of records fetched per page (default 1000). |

Note: Do not check the `configuration.json` file into version control since it contains credentials.

## Requirements file
The `requirements.txt` file lists Python dependencies required for the connector.

```
fivetran-connector-sdk
requests
```

Note: Both `fivetran_connector_sdk` and `requests` are pre-installed in the Fivetran environment. You only need this file for local testing.

## Authentication
The connector authenticates to the Dotdigital API using **Basic Authentication**.  
Each API call includes the following header:

```
Authorization: Basic <base64(API_USER:API_PASSWORD)>
```

This authentication method requires generating an API user in Dotdigital under **User Settings → Access**.  
No OAuth configuration is needed for this example.

## Pagination
Pagination is implemented using the `page` and `count` query parameters supported by the Dotdigital API.

For example:
```
GET /v2/contacts?page=2&count=1000
```

The connector continues fetching until the API returns an empty page, ensuring all records are synchronized.

Pagination is handled in the API fetch logic within the `update(configuration, state)` function.

## Data handling
The connector performs two independent extraction workflows for contacts and campaigns.

1. Fetches data from `/contacts` and `/campaigns` endpoints.  
2. Converts timestamps to ISO 8601 UTC format.  
3. Flattens nested objects (e.g., custom contact fields).  
4. Emits each record as an `Upsert` operation to Fivetran.  
5. Saves the last `date_modified` checkpoint for incremental continuation.  

Schema inference automatically adjusts for new fields returned by the API, minimizing maintenance overhead.

## Error handling
Refer to the API request logic in the connector implementation.

The connector includes the following error-handling strategies:
- `401 Unauthorized`: Raised for invalid credentials; connector stops and logs the issue.  
- `429 Too Many Requests`: Retries automatically using exponential backoff based on the API’s rate-limit headers.  
- `500/503` Server Errors: Retries automatically before failing the sync.  
- Schema drift: Automatically accommodates new fields without requiring schema changes.  

## Tables created
The connector produces two main destination tables.

### dotdigital_contacts
Primary key: `id`

| Column | Description |
|--------|-------------|
| `id` | Unique contact ID. |
| `email` | Contact email address. |
| `first_name` | Contact first name. |
| `last_name` | Contact last name. |
| `opt_in_type` | Type of opt-in used for subscription. |
| `email_type` | Preferred email format (HTML or Text). |
| `status` | Contact subscription status. |
| `date_created` | Timestamp when the contact was created. |
| `date_modified` | Timestamp when the contact record was last modified. |
| `data_fields` | JSON field containing custom contact properties. |

### dotdigital_campaigns
Primary key: `id`

| Column | Description |
|--------|-------------|
| `id` | Unique campaign ID. |
| `name` | Name of the campaign. |
| `subject` | Subject line of the email. |
| `from_name` | “From” name shown to recipients. |
| `status` | Campaign status (Draft, Sent, Paused, etc.). |
| `date_created` | Timestamp when the campaign was created. |
| `date_modified` | Timestamp when the campaign record was last modified. |
| `date_scheduled` | Timestamp when the campaign was scheduled for send. |
| `split_test_parent_id` | ID of parent campaign if part of a split test. |
| `html_content` | Full HTML body of the campaign email. |

## Additional files
- `dotdigital_connector.py` – Main connector script containing schema and update logic.  
- `helpers.py` – Handles pagination and API request utilities.  
- `state_manager.py` – Manages incremental sync checkpoints between runs.  

## Additional considerations
The examples provided are intended to help you effectively use Fivetran’s Connector SDK. While the connector has been tested against Dotdigital’s v2 REST API, Fivetran cannot be held responsible for any unexpected issues that may arise.  
For support or guidance, contact the Fivetran Support team.