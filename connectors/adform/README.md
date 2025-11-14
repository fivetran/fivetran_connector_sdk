# Adform Connector Example

This connector provides Adform advertising campaign data synchronization into your data warehouse for campaign management and advertising insights. It leverages Adform's Buyer API to extract campaign metadata and configuration data for digital advertising analysis.

## Connector overview

The Adform Connector is a Fivetran Connector SDK implementation that extracts campaign data from Adform's Buyer API endpoints. It provides detailed insights into campaign configuration, status, and metadata for digital advertising management.

Data source: Adform Buyer API (v1) for campaign data

Use cases: Campaign management, advertising configuration analysis, campaign status tracking, and digital marketing intelligence.

## Requirements

- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

- Campaign data: Campaign metadata, budgets, status, and configuration tracking
- Campaign management: Campaign status tracking, budget management, and configuration analysis
- Dynamic time ranges: Intelligent data fetching based on sync type (initial vs incremental)
- Incremental syncs: Efficient data updates with checkpoint-based state management
- Error handling: Robust error handling with comprehensive logging and validation

## Configuration file

```json
{
  "api_key"(required): "<YOUR_ADFORM_API_KEY>",
  "client_id"(required): "<YOUR_CLIENT_ID>",
  "initial_sync_days" (optional): "<YOUR_INITIAL_SYNC_DAYS>",
  "max_records_per_page" (optional): "<YOUR_MAX_RECORDS_PER_PAGE>",
  "request_timeout_seconds" (optional): "<YOUR_REQUEST_TIMEOUT_SECONDS>",
  "retry_attempts" (optional): "<YOUR_RETRY_ATTEMPTS>",
  "enable_incremental_sync": "<YOUR_ENABLE_INCREMENTAL_SYNC>"
}
```

**Required Configuration Keys:**
- `api_key`: Adform API key for Buyer API access
- `client_id`: Adform client ID for data extraction

**Optional Configuration Keys:**
- `initial_sync_days`: Days of historical data for initial sync (1-365, default: 90)
- `max_records_per_page`: Maximum records per API page (1-1000, default: 100)
- `request_timeout_seconds`: API request timeout in seconds (default: 30)
- `retry_attempts`: Number of retry attempts for failed requests (default: 3)
- `enable_incremental_sync`: Enable incremental sync (true/false, default: true)

## Requirements file

This connector does not require any additional packages beyond those provided by the Fivetran environment.

Note: The fivetran_connector_sdk:latest and requests:latest packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your requirements.txt.

## Authentication

The connector uses Adform API keys for authentication with the Buyer API. Authentication is provided through:

1. API key: Direct Adform API key in configuration
2. Client access: Client ID for data scope definition

Required API Permissions:
- Buyer API access for querying campaign data
- Client-level access for the specified client ID
- Read permissions for campaigns for Adform account administrator to ensure proper access levels.

Steps to obtain credentials:
1. Go to your Adform account dashboard.
2. Navigate to **Settings > API Keys**.
3. Create a new API key with appropriate permissions.
4. (For Production): We recommend adding credentials as GitHub secrets (see [Deployment](#deployment) section).
5. (For Development): You can add credentials to `configuration.json` (remember not to commit this file).

## Pagination

The connector handles data retrieval from Adform Buyer API with efficient pagination:

- Campaigns API: Supports pagination for large campaign sets
- Data volume management: Uses appropriate time ranges to manage data volume
- Incremental sync: Implements incremental sync to reduce data transfer

The connector implements efficient data handling by:
- Processing complete API responses in memory
- Using appropriate time ranges to manage data volume
- Implementing incremental syncs to reduce data transfer
- Applying filters to focus on relevant data subsets

Refer to function `get_campaigns` in `connector.py` for the data retrieval implementation.

## Data handling

The connector processes Adform data through several stages:

1. Data extraction: Direct Buyer API calls to Adform endpoints.
2. Data transformation: Conversion of Adform API responses to structured table format.
3. Schema mapping: Consistent data types and column naming across all tables.
4. State management: Checkpoint-based incremental sync support.
5. Error handling: Comprehensive error handling with logging and validation.

Data processing features:
- Type conversion: Adform API responses converted to appropriate data types
- Default values: Missing dimensions populated with appropriate defaults
- Timestamp handling: ISO format timestamp conversion and period-based grouping
- Filtering: Campaign and time-based filtering based on configuration
- Dynamic time ranges: Intelligent time range selection for initial vs incremental syncs

Data flow:
Adform Buyer API → Requests Client → Dynamic Time Range Processing → Data Processing Functions → Fivetran Operations → Data Warehouse Tables

Refer to functions `get_time_range`, `execute_api_request`, and `get_campaigns` in `connector.py` for detailed data handling logic.

## Error handling

The connector implements comprehensive error handling strategies to ensure robust operation:

Configuration validation errors:
- ValueError: Missing required configuration values (`api_key`, `client_id`)
- ValueError: Empty API key or client ID values

API request errors:
- RequestException: Handles network timeouts, connection errors, and HTTP failures
- RuntimeError: Manages API request failures and query execution errors

Sync operation errors:
- RuntimeError: Handles general sync failures with detailed error messages
- Logging: Uses Fivetran's logging system with info and severe levels for comprehensive reporting

Error handling implementation:
- Early validation: Configuration parameters validated before API calls
- Exception propagation: Errors are logged and re-raised for Fivetran to handle
- State preservation: Checkpoint system maintains sync state across failures

Refer to functions `validate_configuration`, `execute_api_request`, and the main `update` function in `connector.py` for error handling implementation.

## Tables created

The connector creates the following table for Adform campaign data analysis. _Column types are automatically inferred by Fivetran_ based on the actual data structure and content.

### CAMPAIGN
Primary table for campaign information and configuration data with comprehensive campaign metadata.

Primary key: `id`

Sample columns (automatically inferred by Fivetran):
- `id` - Unique Adform campaign identifier
- `client_id` - Adform client identifier
- `name` - Campaign name
- `status` - Campaign status
- `start_date` - Campaign start date
- `end_date` - Campaign end date
- `budget` - Campaign budget amount
- `currency` - Budget currency code
- `campaign_type` - Type of campaign
- `objective` - Campaign objective
- `created_at` - Campaign creation timestamp
- `updated_at` - Campaign last update timestamp
- `sync_timestamp` - Data extraction timestamp

## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
