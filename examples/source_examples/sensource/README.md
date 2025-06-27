# Sensource API Connector

This connector demonstrates how to fetch traffic and occupancy data from the Sensource API using OAuth2 authentication and incremental syncing with date-based cursors.

## Connector overview

The Sensource connector fetches data from the Sensource API, which provides traffic and occupancy metrics for physical spaces. The connector supports:

- **Static reference data**: Locations, sites, zones, spaces, and sensors
- **Traffic metrics**: Entry and exit counts by zone
- **Occupancy metrics**: Maximum, minimum, and average occupancy by space
- **Incremental syncing**: Date-based cursor tracking for efficient data replication
- **OAuth2 authentication**: Secure API access using client credentials

The connector processes data in 30-day chunks to handle large historical datasets efficiently and uses checkpointing to ensure reliable resumption after interruptions.

## Requirements

* [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)   
* Operating system:
  * Windows: 10 or later (64-bit only)
  * macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  * Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started

Refer to the [Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

For more information about the Sensource API, refer to the [Sensource API Documentation](https://vea.sensourceinc.com/api-docs/).

## Features

* **OAuth2 Authentication**: Secure API access using client credentials flow
* **Static Data Sync**: Reference data for locations, sites, zones, spaces, and sensors
* **Traffic Metrics**: Entry and exit counts with hourly granularity
* **Occupancy Metrics**: Maximum, minimum, and average occupancy with hourly granularity
* **Incremental Syncing**: Date-based cursor tracking for efficient data replication
* **Retry Logic**: Automatic retry with exponential backoff for 5xx errors
* **Configurable Metrics**: Customizable traffic and occupancy metrics via configuration
* **Flexible Start Date**: Support for both specific start dates and year-based initialization

## Configuration file

The configuration file contains the following keys for Sensource API access:

```json
{
  "client_id": "YOUR_CLIENT_ID",
  "client_secret": "YOUR_CLIENT_SECRET",
  "traffic_metrics": "ins,outs",
  "occupancy_metrics": "occupancy(max),occupancy(min),occupancy(avg)",
  "static_endpoints": "location,site,zone,space,sensor",
  "start_year": "2022"
}
```

**Configuration Parameters:**
- `client_id` (required): OAuth2 client ID from Sensource
- `client_secret` (required): OAuth2 client secret from Sensource
- `traffic_metrics` (optional): Comma-separated list of traffic metrics (default: "ins,outs")
- `occupancy_metrics` (optional): Comma-separated list of occupancy metrics (default: "occupancy(max),occupancy(min),occupancy(avg)")
- `static_endpoints` (optional): Comma-separated list of static endpoints to sync (default: "location,site,zone,space")
- `start_year` (optional): Year to start historical data collection (default: 2022)

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file

The `requirements.txt` file specifies the Python libraries required by the connector. For the Sensource connector, no additional dependencies are needed as it only uses the pre-installed `fivetran_connector_sdk` and `requests` packages.

## Authentication

The connector uses OAuth2 client credentials flow for authentication with the Sensource API. To obtain credentials:

1. Contact Sensource to get your OAuth2 client ID and client secret
2. Add these credentials to your `configuration.json` file
3. The connector will automatically obtain and refresh access tokens as needed

For detailed authentication examples and API reference, see the [Sensource API Documentation](https://vea.sensourceinc.com/api-docs/).

**Authentication Flow** - Refer to `get_access_token()` function (lines 18-58)

## Pagination

The Sensource API returns all data for a given date range in a single response, so no traditional pagination is required. However, the connector implements date-based chunking to handle large historical datasets efficiently.

**Date Range Processing** - Refer to `generate_date_ranges()` function (lines 120-150) and the main update loop (lines 200-250)

## Data handling

The connector processes data in the following way:

1. **Static Data**: Fetches reference data (locations, sites, zones, spaces, sensors) without state tracking
2. **Traffic Data**: Fetches hourly traffic metrics by zone with configurable metrics
3. **Occupancy Data**: Fetches hourly occupancy metrics by space with configurable metrics
4. **State Management**: Tracks the last synced date to enable incremental updates
5. **Data Transformation**: Converts API responses to standardized format for Fivetran

**Data Processing** - Refer to `fetch_data()` function (lines 80-110) and `fetch_static_data()` function (lines 60-78)

## Error handling

The connector implements comprehensive error handling:

- **Authentication Errors**: Validates OAuth2 token acquisition and provides clear error messages
- **API Request Errors**: Implements retry logic with exponential backoff for 5xx errors
- **Configuration Validation**: Ensures all required configuration parameters are present
- **Data Processing Errors**: Graceful handling of malformed API responses

**Error Handling** - Refer to `make_request_with_retry()` function (lines 300-330) and configuration validation in `update()` function (lines 160-170)

## Tables Created

The connector creates the following tables:

| Table | Primary Key | Description |
|-------|-------------|-------------|
| `traffic` | `zone_id`, `record_date_hour_1` | Hourly traffic metrics by zone |
| `occupancy` | `space_id`, `record_date_hour_1` | Hourly occupancy metrics by space |
| `location` | `location_id` | Location reference data |
| `site` | `site_id` | Site reference data |
| `zone` | `zone_id` | Zone reference data |
| `space` | `space_id` | Space reference data |
| `sensor` | `sensor_id` | Sensor reference data |

**Schema Definition** - Refer to `schema()` function (lines 270-300)

## Additional files

This connector uses the standard file structure with no additional modular files. All functionality is contained within the main `connector.py` file:

* **connector.py** – Main connector implementation with all functions and logic
* **configuration.json** – Configuration file with API credentials and settings
* **requirements.txt** – Dependencies specification (no additional packages required)
* **README.md** – This documentation file

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team. 