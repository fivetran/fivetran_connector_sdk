# Keycloak Connector Example

## Connector overview

This connector syncs identity and access management data from Keycloak Admin API to enable security analytics, compliance reporting, and user behavior analysis. The connector extracts users, groups, roles, clients, authentication events, and admin events from a Keycloak realm and delivers them to your destination through Fivetran.

Keycloak is an open-source identity and access management solution that provides authentication, authorization, and user management capabilities. This connector enables organizations to centralize their IAM data for security monitoring, compliance auditing, user lifecycle analytics, and multi-tenant SaaS operations.

## Requirements

- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

- Syncs users with attributes, required actions, and realm roles
- Extracts groups with member relationships
- Retrieves realm roles and OAuth clients
- Captures authentication events for security monitoring
- Tracks admin events for compliance auditing
- Implements incremental sync using timestamp-based checkpointing
- Handles pagination for large datasets with checkpointing every 100 records
- Automatic token refresh for long-running syncs
- Creates breakout tables for one-to-many and many-to-many relationships
- Flattens nested JSON objects for efficient querying

## Configuration file

The connector requires the following configuration parameters:

```json
{
  "keycloak_url": "<YOUR_KEYCLOAK_SERVER_URL>",
  "realm": "<YOUR_KEYCLOAK_REALM>",
  "client_id": "<YOUR_CLIENT_ID>",
  "client_secret": "<YOUR_CLIENT_SECRET>",
  "sync_events": true,
  "start_date": "<START_DATE_YYYY_MM_DD>"
}
```

Configuration parameters:
- `keycloak_url` - Base URL of your Keycloak server (e.g., https://keycloak.example.com or http://localhost:8080 for local development)
- `realm` - Keycloak realm name to sync data from (e.g., master, production, or your custom realm)
- `client_id` - OAuth2 client ID for service account authentication
- `client_secret` - OAuth2 client secret for service account authentication
- `sync_events` - Boolean flag to enable/disable event syncing (default: true)
- `start_date` - Start date for incremental event sync in YYYY-MM-DD format (e.g., 2024-01-01)

Note: Ensure that the [configuration.json](configuration.json) file is not checked into version control to protect sensitive information.

## Requirements file

This connector does not require any additional Python packages beyond the Fivetran Connector SDK. The [requirements.txt](requirements.txt) file is empty as all necessary dependencies are pre-installed in the Fivetran environment.

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication

This connector uses OAuth2 Client Credentials grant type to authenticate with the Keycloak Admin API. A dedicated service account with specific view permissions is required.

To set up authentication:

1. Log in to your Keycloak Admin Console (e.g., http://localhost:8080/admin).

2. Navigate to **Clients** and click **Create client**.

3. Configure the client:

   i. Set **Client ID** to a descriptive name (e.g., fivetran-connector).

   ii. Set **Client Protocol** to openid-connect.

   iii. Click **Next**.

4. Enable authentication options:

   i. Enable **Client authentication**.

   ii. Enable **Service accounts roles**.

   iii. Disable **Standard flow** and **Direct access grants**.

   iv. Click **Save**.

5. Retrieve credentials:

   i. Go to the **Credentials** tab.

   ii. Copy the **Client Secret** and add it to your [configuration.json](configuration.json) file.

6. Assign required roles:

   i. Go to the **Service account roles** tab.

   ii. Click **Assign role** and filter by clients.

   iii. Select **realm-management** client.

   iv. Assign the following roles: view-users, view-events, view-realm, view-clients, view-identity-providers.

   v. Click **Assign**.

The connector will use these credentials to obtain access tokens via the `/realms/{realm}/protocol/openid-connect/token` endpoint and automatically refresh tokens as they expire during sync operations.

## Pagination

The connector implements pagination using Keycloak's `first` and `max` query parameters. Each API endpoint is queried in batches of 100 records to prevent memory overflow and enable efficient incremental syncing.

The pagination logic processes each page immediately upon retrieval, upserting records to the destination and checkpointing state after every 100 records. This ensures that if the sync is interrupted, it can resume from the last successful checkpoint without reprocessing data.

For APIs that do not support pagination (such as roles and some event endpoints), the connector fetches all available data in a single request and processes it in memory-efficient batches.

## Data handling

The connector processes Keycloak data as follows:

- **User records** - Flattens nested `access` object fields into parent table columns, creates breakout tables for `attributes`, `realmRoles`, and `requiredActions` arrays
- **Group records** - Stores group metadata in parent table, creates breakout table for group member relationships
- **Event records** - Converts millisecond timestamps to ISO 8601 format, generates synthetic IDs from timestamp, type, and user ID
- **Admin event records** - Flattens `authDetails` nested object into parent table columns

All data types are automatically inferred by Fivetran based on the actual values encountered during sync. Only primary keys are explicitly defined in the schema to enable proper upsert behavior.

## Error handling

The connector implements comprehensive error handling with the following strategies:

- **Retry logic with exponential backoff** - Retries transient errors (HTTP 429, 500, 502, 503, 504) up to 3 times with delays of 1s, 2s, and 4s
- **Token expiration handling** - Monitors token expiration time and automatically refreshes tokens between table syncs
- **Specific exception handling** - Catches `requests.exceptions.RequestException` for network errors and provides detailed logging
- **Graceful degradation** - If optional resources like events or admin-events are unavailable, the connector logs warnings and continues with other tables
- **Authentication failures** - Immediately fails on HTTP 401 (invalid token) and HTTP 403 (insufficient permissions) with actionable error messages

All errors are logged using the Fivetran SDK logging framework with appropriate severity levels (warning for retryable errors, severe for fatal errors).

## Tables created

The connector creates the following tables in your destination:

### user

Main table containing Keycloak user data with flattened access fields.

Primary key: `id`

Key columns: id, username, email, first_name, last_name, enabled, created_timestamp, access_manage_group_membership, access_view, access_map_roles, access_impersonate, access_manage

### user_attribute

Breakout table for user attributes (key-value pairs).

Primary key: `user_id`, `attribute_key`

Key columns: user_id, attribute_key, attribute_value

### user_realm_role

Breakout table for user realm role assignments.

Primary key: `user_id`, `role_name`

Key columns: user_id, role_name

### user_required_action

Breakout table for user required actions.

Primary key: `user_id`, `required_action`

Key columns: user_id, required_action

### group

Table containing Keycloak group data.

Primary key: `id`

Key columns: id, name, path

### group_member

Breakout table for group membership relationships.

Primary key: `group_id`, `user_id`

Key columns: group_id, user_id

### role

Table containing realm roles.

Primary key: `id`

Key columns: id, name, description, composite, client_role

### client

Table containing OAuth/OIDC client applications.

Primary key: `id`

Key columns: id, client_id, name, description, enabled, public_client, protocol, base_url

### event

Table containing authentication events.

Primary key: `id`

Key columns: id, time, type, user_id, session_id, ip_address, client_id

### admin_event

Table containing administrative events for audit trails.

Primary key: `id`

Key columns: id, time, operation_type, resource_type, resource_path, auth_realm_id, auth_client_id, auth_user_id

## Additional files

The connector includes the following additional files:

- [docker-compose.yml](docker-compose.yml) - Docker Compose configuration for running Keycloak locally for development and testing
- [populate_keycloak_data.py](populate_keycloak_data.py) - Script to populate Keycloak with test data including 250 users, 15 groups, 5 roles, and 10 clients to test pagination
- [configuration_template.json](configuration_template.json) - Template configuration file with placeholder values for documentation

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
