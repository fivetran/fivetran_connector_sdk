# ACT Web API Connector

## Overview

This connector fetches data from ACT Web API endpoints and loads it into Fivetran. It handles JWT authentication, pagination, rate limiting, and flattens nested JSON structures into tabular format. The connector is designed to work with ACT Web API and is ideal for users who need to sync CRM data from ACT! into their Fivetran destination.

## Features

- **Multi-Endpoint Support**: Syncs data from 6 ACT Web API endpoints (contacts, companies, opportunities, activities, activity_types, products)
- **JWT Authentication**: Secure authentication with Basic Auth to JWT token exchange and automatic token refresh
- **Incremental Sync**: Table-specific cursors with consistent sync timing across all tables
- **Data Flattening**: Converts nested JSON structures into flat tabular format with automatic ID field handling
- **Rate Limiting**: Configurable rate limiting with Retry-After header support
- **Batch Tracking**: Robust batch progress tracking with resume capability on failures
- **Fail-Safe Protection**: Empty result tracking with automatic failure detection
- **Modular Configuration**: Flexible URL configuration with protocol, hostname, port, and API path components
- **Comprehensive Error Handling**: Specific HTTP status code handling with retry strategies
- **Production Ready**: Unlimited batch processing with intelligent empty result handling

## Requirements

- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Fivetran Connector SDK environment

## Getting Started

Refer to the [Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Configuration

### Configuration File

The connector requires a `configuration.json` file with the following structure:

```json
{
  "protocol": "http",
  "hostname": "YOUR_ACT_SERVER_ADDRESS_LIKE='192.168.7.27'",
  "port": "YOUR_PORT_LIKE='8080'",
  "api_path": "YOUR_API_PATH_LIKE='/act.web.api'",
  "username": "YOUR_USERNAME",
  "password": "YOUR_PASSWORD",
  "database_name": "YOUR_DATABASE_NAME",
  "batch_size": "10000",
  "rate_limit_pause": "0.25",
  "default_start_date": "1900-01-01T00:00:00Z"
}
```

### Configuration Options

| Setting | Type | Required | Description |
|---------|------|----------|-------------|
| `protocol` | string | Yes | Protocol for ACT Web API (http or https) |
| `hostname` | string | Yes | Hostname or IP address for ACT Web API |
| `port` | string | No | Port number (1-65535). Defaults to 80 for HTTP, 443 for HTTPS. Leave empty for default ports |
| `api_path` | string | Yes | API path for ACT Web API (e.g., /act.web.api) |
| `username` | string | Yes | ACT API username |
| `password` | string | Yes | ACT API password |
| `database_name` | string | Yes | ACT Database Name |
| `batch_size` | string | No | Batch size for API requests (default: 10000) |
| `rate_limit_pause` | string | No | Rate limit pause in seconds (default: 0.25) |
| `default_start_date` | string | No | Default start date for incremental sync in ISO 8601 format (default: 1900-01-01T00:00:00Z) |

**Note**: Ensure that the configuration file is not checked into version control to protect sensitive information.

### URL Configuration

The connector builds API URLs from separate configuration components:

- **Protocol**: HTTP or HTTPS protocol specification
- **Hostname**: Server hostname or IP address
- **Port**: Optional port number (1-65535). Defaults to 80 for HTTP, 443 for HTTPS
- **API Path**: API endpoint path (e.g., /act.web.api)
- **URL Construction**: Automatically constructs complete URLs with proper port handling

**Examples:**

```json
// HTTP with custom port
{
  "protocol": "http",
  "hostname": "192.168.7.27",
  "port": "8080",
  "api_path": "/act.web.api"
}
// Result: http://192.168.7.27:8080/act.web.api
```

```json
// HTTP with no port (defaults to 80)
{
  "protocol": "http",
  "hostname": "192.168.7.27",
  "api_path": "/act.web.api"
}
// Result: http://192.168.7.27/act.web.api
```

```json
// HTTPS with custom port
{
  "protocol": "https",
  "hostname": "api.act.com",
  "port": "8443",
  "api_path": "/act.web.api"
}
// Result: https://api.act.com:8443/act.web.api
```

```json
// HTTPS with no port (defaults to 443)
{
  "protocol": "https",
  "hostname": "api.act.com",
  "api_path": "/act.web.api"
}
// Result: https://api.act.com/act.web.api
```

## Authentication

The connector uses a robust authentication process with automatic token refresh:

1. **Basic Auth Exchange**: Authenticates with username/password using Basic Auth
2. **JWT Token**: Exchanges Basic Auth credentials for a JWT token
3. **API Calls**: Uses JWT token for all subsequent API requests
4. **Automatic Refresh**: Detects token expiration (401 errors) and automatically refreshes
5. **Database Context**: Includes `Act-Database-Name` header for database context

### JWT Token Refresh

The connector automatically handles JWT token expiration:

- **Detection**: Monitors for 401 Unauthorized responses
- **Automatic Refresh**: Re-authenticates using stored credentials
- **Seamless Retry**: Retries failed requests with new token
- **Error Handling**: Fails gracefully if refresh is not possible

To obtain credentials, contact your ACT! administrator.

## Data Sources

The connector syncs data from these ACT Web API endpoints:

| Endpoint | Table | Description | Sync Type |
|----------|-------|-------------|-----------|
| `/api/contacts` | `contacts` | Contact records | Incremental (edited field) |
| `/api/companies` | `companies` | Company records | Incremental (edited field) |
| `/api/opportunities` | `opportunities` | Opportunity records | Incremental (edited field) |
| `/api/activities` | `activities` | Activity records | Incremental (edited field) |
| `/api/activity-types` | `activity_types` | Activity type records | Full sync (no filtering) |
| `/api/products` | `products` | Product records | Incremental (editdate field) |

## Schema

The connector creates 6 tables with the following structure:

| Table | Primary Key | Description |
|-------|-------------|-------------|
| `contacts` | `id` | Contact records with flattened JSON structure |
| `companies` | `id` | Company records with flattened JSON structure |
| `opportunities` | `id` | Opportunity records with flattened JSON structure |
| `activities` | `id` | Activity records with flattened JSON structure |
| `activity_types` | `id` | Activity type records with flattened JSON structure |
| `products` | `id` | Product records with flattened JSON structure |

**Note**: Only primary keys are defined in the schema. Fivetran automatically infers other column types from the data.

## Data Processing

### Incremental Sync

The connector supports incremental sync using table-specific cursors:

- **Date Fields**: Uses `edited` field for most tables, `editdate` for products
- **Cursor Management**: Each table maintains its own cursor timestamp
- **Fallback Logic**: Uses `default_start_date` when no previous cursor exists
- **State Persistence**: Cursors are saved after each batch for resumable syncs

### Data Flattening

Nested JSON structures are flattened into tabular format:

- **Nested Objects**: Converted to dot-notation fields (e.g., `address.street` → `address_street`)
- **Arrays**: Converted to JSON strings for tabular storage
- **Date Conversion**: Date strings are converted to ISO 8601 format
- **ID Generation**: Ensures each record has a unique `id` field

### OData Filtering

The connector uses OData filters for efficient data retrieval:

```sql
-- Most tables use edited field
$filter=(edited ge {start_time} and edited le {end_time})
$orderby=edited asc

-- Products table uses editdate field  
$filter=(editdate ge {start_time} and editdate le {end_time})
$orderby=editdate asc

-- Activity types (no filtering - full sync)
-- No OData parameters
```

## Batch Tracking and Resume

The connector implements robust batch tracking to handle sync failures gracefully:

### Batch Tracking Mechanism
- **Progress Tracking**: Tracks current batch position (`skip`) and batch count for each table
- **State Persistence**: Saves batch progress in connector state after each successful batch
- **Resume Capability**: Automatically resumes from the last successful batch on restart
- **Cursor Management**: Only updates table cursors when tables are fully processed
- **Error Recovery**: Clears batch tracking on errors to start fresh on next run

### State Structure
```json
{
  "table_cursors": {
    "companies": "2024-01-01T00:00:00Z",
    "contacts": "2024-01-02T00:00:00Z"
  },
  "table_batch_tracking": {
    "opportunities": {
      "skip": 50,
      "batch_count": 5
    }
  }
}
```

### Benefits
- **No Data Loss**: Failed syncs don't lose progress
- **No Duplicates**: Resumes from exact position without reprocessing
- **Efficient Recovery**: Only processes remaining data on restart
- **Reliable Cursors**: Cursors only advance when tables are complete

## Fail-Safe Protection

The connector implements a fail-safe mechanism to prevent infinite loops and detect systematic issues:

### Fail-Safe Mechanism
- **Empty Result Tracking**: Counts total empty results across the entire sync process
- **Immediate Stop**: Stops processing each table on first empty result (normal behavior)
- **Failure Detection**: Fails entire sync if more than 3 empty results occur across all tables
- **Counter Reset**: Resets empty counter when data is found, allowing normal processing

### Behavior Examples
| Scenario | Empty Results | Action | Result |
|----------|---------------|--------|---------|
| **Normal empty** | 1-2 total | Stop table, continue others | ✅ Normal |
| **Multiple empty** | 3 total | **FAIL & STOP** | ❌ RuntimeError |
| **Data found** | Any count | Reset counter, process normally | ✅ Continue |

### Benefits
- **Prevents Infinite Loops**: Stops on first empty result per table
- **Detects Issues**: Catches systematic problems with API or configuration
- **Clear Error Reporting**: Logs exactly what went wrong and why
- **Graceful Degradation**: Continues processing other tables unless fail-safe triggers

## Error Handling

The connector implements comprehensive error handling with specific HTTP status code management and infinite loop prevention:

### HTTP Status Code Handling

| Status Code | Error Type | Action | Retry |
|-------------|------------|--------|-------|
| **200** | Success | Process data | No |
| **400** | Bad Request | Log error, fail | No |
| **401** | Unauthorized | Refresh JWT token | Yes |
| **403** | Forbidden | Log error, fail | No |
| **404** | Not Found | Log error, fail | No |
| **429** | Rate Limited | Wait and retry | Yes |
| **500-504** | Server Error | Exponential backoff | Yes |

### Error Handling Features

- **Retry Logic**: 3 retry attempts with exponential backoff
- **JWT Token Refresh**: Automatic token refresh on 401 errors
- **Rate Limiting**: Respects Retry-After headers and configurable pause
- **Client Errors**: Immediate failure for 400, 403, 404 (no retry)
- **Server Errors**: Retry with exponential backoff for 5xx errors
- **Network Errors**: Handles timeouts and connection errors
- **Data Validation**: Handles malformed responses and empty data
- **Empty Result Handling**: Stops immediately on first empty result per table (normal behavior)
- **Fail-Safe Protection**: Tracks total empty results across sync - fails if more than 3 empty results occur
- **Intelligent Processing**: Resets empty counter when data is found, prevents infinite loops
- **Batch Tracking**: Tracks batch progress to resume from where sync left off on failures
- **Cursor Management**: Only updates table cursors when tables are fully processed
- **Comprehensive Logging**: Detailed error messages and debugging info

## Testing

### Test Suite

Run the comprehensive test suite:

```bash
python test_connector.py
```

### Test Coverage

The test suite includes:

- Schema validation
- Configuration loading and validation
- Connector creation
- Authentication function testing
- Dictionary flattening validation
- API endpoint accessibility
- Response data handling
- Table cursors persistence
- Connector debug mode

### Fivetran CLI Testing

```bash
fivetran debug --configuration configuration.json
```

## Dependencies

The `requirements.txt` file specifies the Python libraries required:

```
python_dateutil==2.8.2
pytz==2023.3.post1
```

**Note**: The `fivetran_connector_sdk` and `requests` packages are pre-installed in the Fivetran environment.

## File Structure

```
ACT/
├── connector.py              # Main connector implementation
├── configuration.json        # Connector configuration
├── requirements.txt          # Python dependencies
├── test_connector.py         # Test suite
├── README.md                 # Documentation
├── deploy.py                 # Deployment script
├── notes.txt                 # API documentation and examples
└── fields.json              # Original API specification
```

## State Management

The connector uses Fivetran's state management for incremental sync:

```json
{
  "table_cursors": {
    "contacts": "2024-03-20T10:00:00Z",
    "companies": "2024-03-20T09:00:00Z",
    "opportunities": "2024-03-20T11:00:00Z",
    "activities": "2024-03-20T08:00:00Z",
    "activity_types": "2024-03-20T12:00:00Z",
    "products": "2024-03-20T07:00:00Z"
  }
}
```

## Logging

The connector provides detailed logging at multiple levels:

- **INFO**: High-level progress updates, sync status
- **FINE**: Detailed debugging information, API requests/responses
- **WARNING**: Potential issues, rate limits, data validation
- **SEVERE**: Errors, failures, critical issues

## Performance Considerations

- **Batch Size**: Configurable batch size (default: 100 records)
- **Rate Limiting**: Configurable pause between requests (default: 0.25s)
- **Pagination**: Efficient offset-based pagination
- **Memory Usage**: Processes data in batches to manage memory
- **Network**: Retry logic handles transient network issues

## Troubleshooting

### Common Issues and Solutions

| Issue | Symptoms | Solution |
|-------|----------|----------|
| **401 Unauthorized** | JWT token expired | Automatic refresh enabled |
| **429 Rate Limited** | Too many requests | Increase `rate_limit_pause` |
| **400 Bad Request** | Missing parameters | Check API endpoint and parameters |
| **403 Forbidden** | Access denied | Verify API permissions |
| **404 Not Found** | Endpoint not found | Check API base URL |
| **500-504 Server Errors** | API server issues | Automatic retry with backoff |
| **Connection Timeout** | Network issues | Check connectivity and timeout settings |
| **Too Many Empty Results** | Fail-safe triggered | Check API connectivity and authentication |
| **Batch Resume Issues** | Sync doesn't resume | Check batch tracking in state |
| **URL Configuration** | Connection errors | Verify protocol, hostname, port, and API path |

### Debug Steps

1. **Enable Fine Logging**: See detailed API interactions and error messages
2. **Check Configuration**: Verify all required parameters are set (protocol, hostname, port, api_path)
3. **Test API Endpoints**: Use test suite to validate connectivity
4. **Review State**: Check state.json for cursor persistence and batch tracking
5. **Monitor Rate Limits**: Watch for 429 errors and adjust pause settings
6. **Check Fail-Safe**: Review logs for empty result tracking and fail-safe triggers
7. **Verify Batch Tracking**: Ensure batch progress is being saved and resumed correctly
8. **Test URL Building**: Verify complete API URL construction with port handling
9. **Validate Credentials**: Ensure username/password are correct

### Error Message Reference

- **"JWT token expired"**: Automatic refresh will be attempted
- **"Rate limit exceeded"**: Increase `rate_limit_pause` in configuration
- **"Bad Request"**: Check API parameters and endpoint URL
- **"Too many empty results"**: Fail-safe triggered - check API connectivity and authentication
- **"Failed to authenticate"**: Verify username, password, and database name
- **"Port must be between 1 and 65535"**: Check port configuration
- **"Access denied"**: Verify API permissions and credentials
- **"Server error"**: Temporary API issues, will retry automatically

## Support

For technical support and questions:

- [Fivetran Connector SDK Documentation](https://fivetran.com/docs/connector-sdk)
- [SDK Examples Repository](https://github.com/fivetran/fivetran_connector_sdk/tree/main/examples)
- [Best Practices Guide](https://fivetran.com/docs/connector-sdk/best-practices)

## License

This connector is provided as an example for Fivetran Connector SDK usage. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples.
