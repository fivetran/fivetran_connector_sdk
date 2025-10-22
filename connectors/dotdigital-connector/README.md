# Dotdigital Connector

## Connector overview

This connector extracts **marketing contact and campaign data** from the [Dotdigital API](https://developer.dotdigital.com/).  
It synchronizes your audience and email campaign metadata into your Fivetran destination for downstream analytics.

It maintains two core tables:

- `dotdigital_contacts`: Stores subscriber details, attributes, and consent information
- `dotdigital_campaigns`: Stores campaign metadata such as subject line, status, creation date, and metrics

Incremental syncs ensure new or updated records are fetched without duplicating existing data.

---

## Requirements

* [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
* Operating System:
    * Windows 10 or later
    * macOS 13 (Ventura) or later
    * Linux: Ubuntu 20.04+, Debian 10+, or Amazon Linux 2+ (arm64 or x86_64)

---

## Getting started

Refer to the [Fivetran Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to initialize and test the connector.

---

## Features

* Fetches data from Dotdigital’s **v2 REST API**
* Supports incremental sync using `date_created` and `date_modified` fields
* Automatically paginates through results (`page` and `count` parameters)
* Normalizes complex nested attributes (lists, custom fields)
* Maps campaign and contact relationships for joinable analytics

---

## Configuration file

Example `configuration.json`:

```json
{
  "api_base_url": "https://r1-api.dotdigital.com/v2",
  "api_user": "YOUR_API_USER",
  "api_password": "YOUR_API_PASSWORD",
  "cursor_field": "date_modified",
  "page_size": 1000
}
```

**Notes**
- You must generate an API user and password in Dotdigital (User Settings → Access).
- The connector supports `cursor_field` = `date_created` or `date_modified`.
- Use the correct API region prefix (e.g. `r1`, `r2`, `r3`) from your Dotdigital account.

---

## Requirements file

The connector has minimal dependencies.  
Create `requirements.txt` with:

```text
fivetran-connector-sdk
requests
```

---

## Authentication

Dotdigital uses **Basic Authentication** for API access.

Each request includes:
```
Authorization: Basic <base64(API_USER:API_PASSWORD)>
```

---

## Data handling

Each sync follows this process:

1. Fetch contact data from `/contacts`
2. Fetch campaign data from `/campaigns`
3. Transform and normalize both datasets
4. Emit rows as `Upsert` operations
5. Save the most recent `date_modified` checkpoint for incremental loads

---

## Error handling

The connector handles:
- **401 Unauthorized** – invalid credentials
- **429 Too Many Requests** – rate-limited; automatically retries
- **500/503** – transient server errors; retried with exponential backoff
- Schema drift (new Dotdigital fields) handled gracefully by dynamic field inference

---

## Tables Created

### dotdigital_contacts
**Primary key:** `id`

| Field | Description |
|-------|--------------|
| `id` | Unique contact ID |
| `email` | Contact email address |
| `first_name` | Contact first name |
| `last_name` | Contact last name |
| `opt_in_type` | How the contact opted in |
| `email_type` | Preferred email format (HTML/Text) |
| `status` | Contact status (Subscribed, Unsubscribed, Pending, etc.) |
| `date_created` | Contact creation timestamp |
| `date_modified` | Last modified timestamp |
| `data_fields` | JSON map of custom contact fields |

---

### dotdigital_campaigns
**Primary key:** `id`

| Field | Description |
|-------|--------------|
| `id` | Unique campaign ID |
| `name` | Campaign name |
| `subject` | Campaign subject line |
| `from_name` | From name displayed to recipients |
| `status` | Campaign status (Draft, Sent, etc.) |
| `date_created` | Creation timestamp |
| `date_modified` | Last modified timestamp |
| `date_scheduled` | Scheduled send time |
| `split_test_parent_id` | ID of parent campaign if split test |
| `html_content` | Raw HTML of the email (if available) |

---

## Incremental Syncs

The connector stores a persistent cursor (usually `date_modified`) to fetch only new or updated contacts and campaigns.  
Fivetran automatically handles checkpoints between syncs to avoid duplicates.

---

## Additional considerations

* Dotdigital API has **rate limits** (default: 180 requests per minute); the connector includes backoff logic.
* Use the correct region endpoint (`r1-api`, `r2-api`, etc.) based on your Dotdigital account.
* Incremental syncs are idempotent.
* This connector is designed for educational and demonstration purposes using the Fivetran Connector SDK.

---