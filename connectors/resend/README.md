# Resend Connector Example

## Connector overview
This connector demonstrates how to fetch email data from [Resend](https://resend.com/) and upsert it into your destination using the Fivetran Connector SDK. The custom connector synchronizes email records from your Resend account and implements pagination handling to efficiently process large datasets with incremental synchronization based on email IDs.

## Requirements
- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features
- Synchronizes emails from Resend's API `/emails` endpoint
- Supports incremental syncing based on email IDs
- Implements pagination handling for large datasets
- Checkpoints progress at regular intervals to ensure reliable sync resumption
- Comprehensive error handling with exponential backoff retry logic
- Flattens nested JSON structures for optimal table schemas

## Configuration file
The configuration key required for your connector is as follows:

```json
{
  "api_token": "<YOUR_RESEND_API_TOKEN>"
}
```

### Configuration parameters

- `api_token` (required) - Your Resend API token for authentication

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file
The connector uses the `requests` library for HTTP communication, which is pre-installed in the Fivetran environment.

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication
The connector uses Bearer token authentication with Resend API keys. To obtain your API key:

1. Log in to your [Resend account](https://resend.com/).
2. Go to **API Keys** in your dashboard.
3. Click **Create API Key**.
4. Enter a descriptive name for the API key.
5. Select the appropriate permissions (ensure the key has read access to emails).
6. Make a note of the generated API key. You will use it as the `api_token` in your connector's `configuration.json` file.

Note: Resend API keys are shown only once upon creation. Store them securely.

## Pagination
The connector implements Resend's pagination system using the `starting_after` parameter. It processes emails in pages and uses the `has_more` flag to determine if additional pages exist. The connector uses the last email ID from each page to fetch subsequent pages (refer to the `sync_emails` function in lines 177-250).

## Data handling
The connector processes email data from the `/emails` endpoint which contains email metadata including sender, recipients, subject, timestamps, and delivery status. All nested JSON structures are flattened using the `flatten_dict` function (refer to lines 38-65) to create optimal table schemas. Arrays are converted to JSON strings for storage.

## Error handling
The connector implements comprehensive error handling strategies (refer to the `fetch_emails_from_api` function in lines 68-118):
- HTTP timeout handling with 30-second timeouts
- Rate limiting detection (HTTP 429) with exponential backoff retry logic
- Server error handling (HTTP 5xx) with exponential backoff
- Maximum of 5 retry attempts with 1-second base delay
- Specific exception handling for timeout, HTTP errors, and request failures
- Graceful error logging without exposing sensitive information

## Tables created
The connector creates a single table named `email` with the following schema:

| Column Name | Type | Description |
|------------|------|-------------|
| id | STRING | Email ID (Primary Key) |
| _from | STRING | Sender email address |
| _to | STRING | Recipient email addresses (JSON array) |
| subject | STRING | Email subject line |
| created_at | STRING | Email creation timestamp |
| last_event | STRING | Last email event status (delivered, bounced, complained, etc.) |
| cc | STRING | CC recipients (JSON array) |
| bcc | STRING | BCC recipients (JSON array) |
| reply_to | STRING | Reply-to addresses (JSON array) |
| scheduled_at | STRING | Scheduled send time (if applicable) |

The table uses `id` as the primary key. The connector automatically infers additional column types from the API response data.

## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
