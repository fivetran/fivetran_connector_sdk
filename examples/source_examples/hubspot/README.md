# HubSpot Events Connector Example

This connector demonstrates how to integrate HubSpot's Events API with Fivetran using the Connector SDK. It provides an example of retrieving event analytics data from HubSpot, specifically focusing on page visit events, and synchronizing them to your destination with proper pagination handling and state management.

## Requirements

* [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)   
* Operating System:  
  * Windows 10 or later  
  * macOS 13 (Ventura) or later
* HubSpot account with API access
* HubSpot API token with appropriate permissions
* Access to HubSpot Events API

## Getting Started

Refer to the [Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

* Demonstrates HubSpot Events API integration
* Implements efficient pagination handling
* Supports multiple event types
* Processes nested JSON data
* Includes state checkpointing for resumable syncs
* Handles large datasets with configurable page sizes
* Provides comprehensive error handling
* Supports historical data sync with configurable start date

## Configuration File

The connector requires the following configuration parameters:

```json
{
    "api_token": "YOUR_HUBSPOT_API_TOKEN",
    "initial_sync_start_date": "2025-01-01T00:00:00.000Z"
}
```

* `api_token`: Your HubSpot API token for authentication
* `initial_sync_start_date`: The start date for the initial historical data sync (ISO 8601 format)

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements File

The connector requires the following Python packages:

```
requests
```

Note: The `fivetran_connector_sdk:latest` package is pre-installed in the Fivetran environment.

## Authentication

The connector uses HubSpot API token authentication:
1. API token provided in configuration
2. Token included in request headers as Bearer token
3. Automatic token validation on each request

## Data Handling

The connector syncs the following table:

### e_visited_page Table
| Column    | Type    | Description                    |
|-----------|---------|--------------------------------|
| id        | STRING  | Primary key                    |
| eventType | STRING  | Type of event                  |
| occurred  | STRING  | Event timestamp                |
| ...       | VARIOUS | Additional event properties    |

The connector implements the following data handling features:
* Configurable batch size (default: 1000 records per request)
* Automatic flattening of nested JSON structures
* List value serialization to JSON strings
* Incremental sync using timestamp-based pagination
* State management for resumable syncs

## Error Handling

The connector implements the following error handling:
* Validates API responses with raise_for_status()
* Includes comprehensive logging
* Handles pagination edge cases
* Validates configuration parameters
* Manages API rate limits
* Implements proper error propagation

## Additional Considerations

This example is intended for learning purposes and demonstrates HubSpot Events API integration. For production use, you should:

1. Implement appropriate error retry mechanisms
2. Add rate limiting controls
3. Consider implementing multiple event type handling
4. Add monitoring for sync performance
5. Implement proper logging strategy
6. Consider implementing custom event filtering
7. Add proper handling for API outages
8. Consider implementing data validation
9. Add proper cleanup procedures
10. Consider implementing custom data transformations
11. Implement proper error notification system
12. Consider implementing parallel processing for large datasets

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team. 