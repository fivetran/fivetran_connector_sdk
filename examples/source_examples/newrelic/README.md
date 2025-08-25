# New Relic Feature APIs Connector

*This connector provides comprehensive New Relic monitoring data synchronization into your data warehouse for advanced performance analytics, infrastructure monitoring, and application insights. It leverages New Relic's NerdGraph API to extract APM, Infrastructure, Browser, Mobile, and Synthetic monitoring data for comprehensive observability analysis.*

## Connector overview

The New Relic Feature APIs Connector is a Fivetran Connector SDK implementation that extracts comprehensive monitoring and performance data from New Relic's various feature APIs. It provides detailed insights into application performance, infrastructure health, user experience, and system reliability across multiple New Relic services.

**Key Capabilities:**
- **APM Data Extraction**: Transaction performance, error rates, throughput, and Apdex scores
- **Infrastructure Monitoring**: Host status, domain analysis, and infrastructure health tracking
- **Browser Performance**: Page load times, browser analytics, and device performance insights
- **Mobile App Monitoring**: Crash analysis, platform performance, and device utilization
- **Synthetic Monitoring**: Uptime monitoring, response time analysis, and failure detection
- **Cross-Service Analytics**: Correlated performance analysis across all monitoring services
- **Incremental Syncs**: Efficient data updates with state management

**Data Source**: New Relic NerdGraph API (GraphQL queries for APM, Infrastructure, Browser, Mobile, and Synthetic monitoring data)

**Use Cases**: Application performance monitoring, infrastructure health tracking, user experience optimization, mobile app quality assurance, synthetic monitoring, and comprehensive observability analysis.

## Requirements

* [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)   
* Operating system:
  * Windows: 10 or later (64-bit only)
  * macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  * Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started

Refer to the [Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

* **Comprehensive APM Data**: Transaction performance metrics, error rates, throughput analysis, and Apdex scores for application monitoring
* **Infrastructure Health Tracking**: Host status monitoring, domain analysis, infrastructure entity tracking, and health reporting
* **Browser Performance Analytics**: Page load time analysis, browser compatibility insights, device performance comparison, and user experience metrics
* **Mobile App Quality Assurance**: Crash analysis, platform performance tracking, device utilization insights, and app stability monitoring
* **Synthetic Monitoring**: Uptime monitoring, response time analysis, failure detection, and location-based performance insights
* **Cross-Service Correlation**: Integrated analysis across all monitoring services for comprehensive observability
* **Dynamic Time Ranges**: Intelligent data fetching based on sync type (initial vs incremental)
* **Incremental Syncs**: Efficient data updates with checkpoint-based state management
* **Configurable Filtering**: Service and account-level filtering for focused monitoring analysis
* **Error Handling**: Robust error handling with comprehensive logging and validation

## Configuration file

The connector requires the following configuration keys defined in `configuration.json`:

```json
{
  "api_key": "NRAK-ABC123DEF456",
  "account_id": "123456789",
  "region": "US"
}
```

**Required Configuration Keys:**
- `api_key`: New Relic API key for NerdGraph API access (must start with NRAK-)
- `account_id`: New Relic account ID for data extraction

**Optional Configuration Keys:**
- `region`: New Relic region (US or EU, defaults to US)

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file

The `requirements.txt` file specifies the Python libraries required by the connector for New Relic API integration and data processing.

**Example content of `requirements.txt`:**

```
fivetran-connector-sdk>=2.0.0
requests>=2.28.0
```

Note: The `fivetran_connector_sdk:latest` package (v2.0.0+) is pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare it in your `requirements.txt`. This connector uses the updated SDK version that does not require `yield` statements for operations.

## Authentication

The connector uses New Relic API keys for authentication with the NerdGraph API. Authentication is provided through:

1. **API Keys**: Direct New Relic API key in configuration (NRAK- format)
2. **Account Access**: Account ID for data scope definition

**Required API Permissions:**
- NerdGraph API access for querying monitoring data
- Account-level access for the specified account ID

**Steps to obtain credentials:**
1. Go to your New Relic account
2. Navigate to API Keys section
3. Create a new API key with appropriate permissions
4. Add credentials to `configuration.json`

## Pagination

The connector handles data retrieval from New Relic NerdGraph API with efficient pagination:

- **NRQL Queries**: Limited to 1000 results per query with time-based filtering
- **Entity Search**: Supports cursor-based pagination for large result sets
- **Data Volume Management**: Uses appropriate time ranges to manage data volume
- **Incremental Syncs**: Implements incremental syncs to reduce data transfer

The connector implements efficient data handling by:
- Processing complete API responses in memory
- Using appropriate time ranges to manage data volume
- Implementing incremental syncs to reduce data transfer
- Applying filters to focus on relevant data subsets

Refer to functions `get_apm_data`, `get_infrastructure_data`, `get_browser_data`, `get_mobile_data`, and `get_synthetic_data` in `connector.py` for the data retrieval implementation.

## Dynamic Time Range Management

The connector intelligently manages data fetching based on sync type:

### **Initial Sync**
- **Time Range**: Last 90 days of data
- **Use Case**: First-time setup or full re-sync
- **Data Volume**: Comprehensive historical data for baseline analysis

### **Incremental Sync**
- **Time Range**: Data since last successful sync timestamp
- **Use Case**: Regular scheduled syncs
- **Data Volume**: Only new/changed data since last sync
- **Benefits**: Faster sync times, reduced API calls, efficient resource usage

### **Implementation Details**
- **State Management**: Uses Fivetran's checkpoint system to track last sync time
- **Automatic Detection**: Automatically determines sync type based on state
- **Flexible Queries**: All NRQL queries dynamically adjust time ranges
- **Logging**: Clear indication of sync type and time ranges in logs

## Data handling

The connector processes New Relic monitoring data through several stages:

1. **Data Extraction**: Direct GraphQL queries to NerdGraph endpoints
2. **Data Transformation**: Conversion of New Relic API responses to structured table format
3. **Schema Mapping**: Consistent data types and column naming across all tables
4. **State Management**: Checkpoint-based incremental sync support
5. **Error Handling**: Comprehensive error handling with logging and validation

**Data Processing Features:**
- **Type Conversion**: New Relic API responses converted to appropriate data types
- **Default Values**: Missing dimensions populated with appropriate defaults
- **Timestamp Handling**: ISO format timestamp conversion and period-based grouping
- **Data Aggregation**: Multiple metric aggregations (averages, counts, percentages)
- **Filtering**: Service and account-level filtering based on configuration
- **Dynamic Time Ranges**: Intelligent time range selection for initial vs incremental syncs

**Data Flow:**
New Relic NerdGraph API → Requests Client → Dynamic Time Range Processing → Data Processing Functions → Fivetran Operations → Data Warehouse Tables

Refer to functions `get_time_range`, `execute_nerdgraph_query`, `get_apm_data`, `get_infrastructure_data`, `get_browser_data`, `get_mobile_data`, and `get_synthetic_data` in `connector.py` for detailed data handling logic.

## Error handling

The connector implements comprehensive error handling strategies to ensure robust operation:

**Configuration Validation Errors:**
- **ValueError**: Missing required configuration values (`api_key`, `account_id`, `region`)
- **ValueError**: Invalid API key format (must start with `NRAK-`)
- **ValueError**: Invalid region specification (must be `US` or `EU`)

**API Request Errors:**
- **RequestException**: Handles network timeouts, connection errors, and HTTP failures
- **RuntimeError**: Manages API request failures and query execution errors

**Sync Operation Errors:**
- **RuntimeError**: Handles general sync failures with detailed error messages
- **Logging**: Uses Fivetran's logging system with info and severe levels for comprehensive reporting

**Error Handling Implementation:**
- **Early Validation**: Configuration parameters validated before API calls
- **Exception Propagation**: Errors are logged and re-raised for Fivetran to handle
- **State Preservation**: Checkpoint system maintains sync state across failures

Refer to functions `validate_configuration`, `execute_nerdgraph_query`, and the main `update` function in `connector.py` for error handling implementation.

## Tables created

The connector creates the following tables for comprehensive New Relic monitoring analysis:

### **apm_data**
Primary table for application performance monitoring data with transaction-level metrics and performance indicators.

**Key Columns:**
- `account_id` (STRING): New Relic account identifier
- `timestamp` (STRING): Data collection timestamp
- `transaction_name` (STRING): Name of the monitored transaction
- `duration` (FLOAT): Transaction response time in milliseconds
- `error_rate` (FLOAT): Error rate percentage (0-1)
- `throughput` (FLOAT): Transactions per second
- `apdex_score` (FLOAT): Apdex performance score (0-1)

### **infrastructure_data**
Infrastructure monitoring data for host status, domain analysis, and infrastructure health tracking.

**Key Columns:**
- `account_id` (STRING): New Relic account identifier
- `timestamp` (STRING): Data collection timestamp
- `host_name` (STRING): Infrastructure host name
- `domain` (STRING): Host domain classification
- `type` (STRING): Host type classification
- `reporting` (BOOLEAN): Whether the host is currently reporting
- `tags` (STRING): JSON-formatted host tags and metadata

### **browser_data**
Browser performance monitoring data for user experience analysis and page performance insights.

**Key Columns:**
- `account_id` (STRING): New Relic account identifier
- `timestamp` (STRING): Data collection timestamp
- `page_url` (STRING): Monitored page URL
- `browser_name` (STRING): Browser name and version
- `browser_version` (STRING): Browser version information
- `device_type` (STRING): Device type classification
- `load_time` (FLOAT): Page load time in milliseconds
- `dom_content_loaded` (FLOAT): DOM content loaded time in milliseconds

### **mobile_data**
Mobile application monitoring data for crash analysis, platform performance, and device utilization insights.

**Key Columns:**
- `account_id` (STRING): New Relic account identifier
- `timestamp` (STRING): Data collection timestamp
- `app_name` (STRING): Mobile application name
- `platform` (STRING): Mobile platform (iOS/Android)
- `version` (STRING): Application version
- `device_model` (STRING): Device model information
- `os_version` (STRING): Operating system version
- `crash_count` (INTEGER): Number of crashes detected

### **synthetic_data**
Synthetic monitoring data for uptime monitoring, response time analysis, and failure detection.

**Key Columns:**
- `account_id` (STRING): New Relic account identifier
- `timestamp` (STRING): Data collection timestamp
- `monitor_name` (STRING): Synthetic monitor name
- `monitor_type` (STRING): Type of synthetic monitor
- `status` (STRING): Monitor execution status
- `response_time` (FLOAT): Monitor response time in milliseconds
- `location` (STRING): Monitor execution location
- `error_message` (STRING): Error message if monitor failed

## Additional files

The connector includes several additional files to support functionality and deployment:

* **`dashboard_queries.sql`** – Comprehensive SQL queries for building New Relic monitoring dashboards and reports. Includes executive summaries, performance trends, infrastructure analysis, and alerting queries.

* **`deploy_connector.sh`** – Deployment script for automating connector deployment to Fivetran environment.

* **`requirements.txt`** – Python dependency specification for New Relic API integration and connector requirements.

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.

**Important New Relic Limitations:**
- NRQL queries limited to 1000 results per query
- API rate limits apply to large accounts
- Data retention varies by New Relic plan
- Some features require specific New Relic licenses
- Historical data availability depends on account settings

**Performance Considerations:**
- Large accounts may experience longer sync times
- Multiple monitoring services increase API calls
- Historical data syncs can be resource-intensive
- Use incremental syncs for production environments

**Security Best Practices:**
- Use API keys with minimal required permissions
- Regularly rotate API keys
- Monitor API usage and rate limits
- Implement proper error handling and logging

## References

- [New Relic API Documentation](https://docs.newrelic.com/docs/apis/intro-apis/introduction-new-relic-apis)
- [NerdGraph API](https://docs.newrelic.com/docs/apis/nerdgraph/get-started/introduction-new-relic-nerdgraph/)
- [Fivetran Connector SDK Documentation](https://fivetran.com/docs/connector-sdk)
- [Fivetran Connector SDK Best Practices](https://fivetran.com/docs/connectors/connector-sdk/best-practices)
- [Fivetran Connector SDK v2.0.0+ Migration Guide](https://fivetran.com/docs/connector-sdk/tutorials/removing-yield-usage)
