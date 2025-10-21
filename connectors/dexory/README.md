# Dexory Connector Example

## Connector overview

This connector demonstrates how to sync inventory scan data from the Dexory Inventory API using the Fivetran Connector SDK. Dexory provides real-time inventory visibility and tracking solutions for warehouses and distribution centers. The connector fetches location data and associated expected inventory objects from a single Dexory site, enabling organizations to integrate their inventory management data with their data warehouse for analytics and reporting.

The connector supports pagination for large datasets, implements robust retry logic with exponential backoff, and handles nested data structures by flattening them into separate tables for optimal data warehouse storage.

## Requirements

- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)   
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

- Connects to Dexory Inventory API to fetch real-time inventory scan data
- Supports pagination for handling large datasets efficiently
- Implements robust retry logic with configurable exponential backoff
- Handles nested data structures by creating separate parent-child tables
- Provides configurable page size and maximum page limits
- Includes comprehensive error handling for various API failure scenarios
- Supports incremental sync with state checkpointing

## Configuration file

The connector requires the following configuration parameters in the configuration.json file:

```json
{
    "site_name": "<YOUR_SITE_NAME>",
    "site_api_key": "<YOUR_SITE_API_KEY>",
    "base_url": "https://api.service.dexoryview.com"
}
```

**Required Parameters:**
- `site_name`: The name of your Dexory site
- `site_api_key`: Your Dexory API key for authentication

**Optional Parameters:**
- `base_url`: The Dexory API base URL (default: https://api.service.dexoryview.com)
- `max_pages`: Maximum number of pages to process (default: 1000)
- `page_size`: Number of records per page (default: 1000)
- `base_retry_delay_seconds`: Base delay in seconds for retry logic (default: 2)

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file

This connector uses standard Python libraries and does not require any additional packages beyond those provided by the Fivetran environment.

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication

The connector uses API key authentication with the Dexory Inventory API. Authentication is handled through the `site_api_key` parameter in the configuration file.

## Pagination

The connector implements page-based pagination to handle large datasets efficiently. Pagination is handled in the `update` function and uses the following approach:

- Fetches data in configurable page sizes (default: 1000 records per page)
- Continues fetching pages until no more data is available or maximum page limit is reached
- Uses the `next` URL from API response links for subsequent page requests
- Implements safeguards to prevent infinite loops with maximum page limits
- Logs progress every 10 pages for monitoring

The pagination logic automatically handles the `links.next` field from the Dexory API response to determine when to stop fetching additional pages.

## Data handling

The connector processes Dexory inventory data through several transformation steps:

**Data Retrieval:** Fetches location data from the `/customer-api/v1/locations` endpoint with automatic pagination support.

**Data Transformation:** The `convert_lists_to_strings` function converts complex nested data structures to JSON strings for Fivetran compatibility, while preserving the `expected_inventory_object` field for separate table processing.

**Parent-Child Table Structure:** 
- **Parent Table (`location`)**: Contains location metadata including name, type, aisle, and scan date
- **Child Table (`expected_inventory_object`)**: Contains individual inventory objects with foreign key references to the parent location

**Data Flattening:** Nested objects are flattened and foreign key relationships are established by adding location identifiers to each inventory object record.

**State Management:** The connector maintains sync state using checkpointing to enable resumable syncs and track the last successful sync timestamp.

## Error handling

The connector implements comprehensive error handling strategies across multiple functions:

**API Request Retries:** The `fetch_locations_with_retry` function implements exponential backoff retry logic for:
- Connection timeouts and network errors
- Rate limiting (HTTP 429 responses)
- Server errors (5xx status codes)
- General request exceptions

**Configuration Validation:** The `validate_configuration` function validates the presence of all required configuration parameters before processing begins.

**Data Processing Errors:** Graceful handling of missing or malformed data with appropriate logging and continuation of processing.

**Retry Logic:** Configurable retry attempts with exponential backoff using the formula: `base_retry_delay * (2^attempt)` where:
- Attempt 1: 2 seconds
- Attempt 2: 4 seconds  
- Attempt 3: 8 seconds

**Logging:** Comprehensive logging at different levels (info, warning, severe) for monitoring and troubleshooting.

## Tables created

The connector creates two related tables in your data warehouse:

### 1. **LOCATIONS**
- **Purpose**: Contains location metadata and scan information
- **Primary Key**: `["name", "location_type", "aisle", "scan_date"]`
- **Content**: Location details, scan timestamps, and metadata
- **Data Source**: `/customer-api/v1/locations` endpoint

### 2. **EXPECTED_INVENTORY_OBJECTS**
- **Purpose**: Contains individual inventory objects associated with each location
- **Primary Key**: `_fivetran_id` (auto-generated hash of all fields)
- **Content**: Inventory object details with foreign key references to parent location
- **Data Source**: Nested `expected_inventory_objects` field from locations API response
- **Foreign Keys**: `name`, `location_type`, `aisle`, `scan_date` (linking to locations table)

Both tables use the `op.upsert()` operation to insert or update data, ensuring data consistency across sync runs. The parent-child relationship allows for efficient querying of inventory data while maintaining referential integrity.

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
