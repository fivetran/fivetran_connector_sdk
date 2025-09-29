# New Relic Feature APIs Connector

This connector provides comprehensive New Relic monitoring data synchronization into your data warehouse for advanced performance analytics, infrastructure monitoring, and application insights. It leverages New Relic's NerdGraph API to extract APM, Infrastructure, Browser, Mobile, and Synthetic monitoring data for comprehensive observability analysis.

## Connector overview

The New Relic Feature APIs Connector is a Fivetran Connector SDK implementation that extracts comprehensive monitoring and performance data from New Relic's various feature APIs. It provides detailed insights into application performance, infrastructure health, user experience, and system reliability across multiple New Relic services.

Data source: New Relic NerdGraph API (GraphQL queries for APM, Infrastructure, Browser, Mobile, and Synthetic monitoring data)

Use cases: Application performance monitoring, infrastructure health tracking, user experience optimization, mobile app quality assurance, synthetic monitoring, and comprehensive observability analysis.

## Requirements

- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)   
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

* Comprehensive APM data: Transaction performance metrics, error rates, throughput analysis, and Apdex scores for application monitoring
* Infrastructure health tracking: Host status monitoring, domain analysis, infrastructure entity tracking, and health reporting
* Browser performance analytics: Page load time analysis, browser compatibility insights, device performance comparison, and user experience metrics
* Mobile app quality assurance: Crash analysis, platform performance tracking, device utilization insights, and app stability monitoring
* Synthetic monitoring: Uptime monitoring, response time analysis, failure detection, and location-based performance insights
* Cross-Service correlation: Integrated analysis across all monitoring services for comprehensive observability
* Dynamic time ranges: Intelligent data fetching based on sync type (initial vs incremental)
* Incremental syncs: Efficient data updates with checkpoint-based state management
* Configurable filtering: Service and account-level filtering for focused monitoring analysis
* Error handling: Robust error handling with comprehensive logging and validation
* CI/CD integration: Automated deployment via GitHub Actions with parameterized configuration
* Environment management: Secure credential management using GitHub Environments
* Zero-Downtime deployment: Seamless updates without service interruption

## Configuration file

PRODUCTION DEPLOYMENT GUIDANCE: For production deployments, we recommend using **GitHub secrets** configured in the GitHub Actions workflow. Local configuration files can be used for development and testing, but we encourage using GitHub secrets for production environments.

### Basic configuration

For production deployments, we recommend configuring these as **GitHub secrets** (see [Deployment](#deployment) section). For local development and testing, you can define them in `configuration.json`:

```json
{
  "api_key": "NRAK-ABC123DEF456",
  "account_id": "123456789",
  "region": "US",
  "sync_frequency_minutes": "15",
  "initial_sync_days": "90",
  "max_records_per_query": "1000",
  "enable_apm_data": "true",
  "enable_infrastructure_data": "true",
  "enable_browser_data": "true",
  "enable_mobile_data": "true",
  "enable_synthetic_data": "true",
  "timeout_seconds": "30",
  "retry_attempts": "3",
  "retry_delay_seconds": "5",
  "data_quality_threshold": "0.95",
  "alert_on_errors": "true",
  "log_level": "INFO"
}
```

Required configuration keys:
- `api_key`: New Relic API key for NerdGraph API access (must start with NRAK-)
- `account_id`: New Relic account ID for data extraction

Optional configuration keys:
- `region`: New Relic region (US or EU, defaults to US)
- `sync_frequency_minutes`: Sync frequency in minutes (1-1440, defaults to 15)
- `initial_sync_days`: Days of historical data for initial sync (1-365, defaults to 90)
- `max_records_per_query`: Maximum records per API query (1-10000, defaults to 1000)
- `enable_*_data`: Enable/disable specific data sources (defaults to true)
- `timeout_seconds`: API request timeout (5-300, defaults to 30)
- `retry_attempts`: Number of retry attempts on failure (0-10, defaults to 3)
- `retry_delay_seconds`: Delay between retry attempts (defaults to 5)
- `data_quality_threshold`: Data quality threshold for alerts (0-1, defaults to 0.95)
- `alert_on_errors`: Enable error alerting (defaults to true)
- `log_level`: Logging level (DEBUG, INFO, WARNING, ERROR, SEVERE, defaults to INFO)

## Production vs Development Configuration

### Production deployment (Recommended)
- Method: GitHub Actions workflow with GitHub secrets
- Security: Credentials stored securely in GitHub
- Automation: Push-to-deploy workflow
- Compliance: Enterprise-grade security practices

### Development & Testing (Available)
- Method: Local `configuration.json` or environment variables
- Security: Credentials stored locally (never commit to repository)
- Use case: Local development, debugging, testing
- Guidance: We recommend using GitHub secrets for production

Security note: For production deployments, we recommend using **GitHub secrets**. Local configuration files are available for development but we encourage using GitHub secrets for production environments.

Note: We recommend keeping the `configuration.json` file out of version control to protect sensitive information. You can add it to your `.gitignore` file for additional security.

## Requirements file

The `requirements.txt` file specifies the Python libraries required by the connector for New Relic API integration and data processing.

Example content of `requirements.txt`:

```
fivetran-connector-sdk>=2.0.0
requests>=2.28.0
```

Note: The `fivetran_connector_sdk:latest` package (v2.0.0+) is pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare it in your `requirements.txt`. 

## Authentication

The connector uses New Relic API keys for authentication with the NerdGraph API. Authentication is provided through:

1. API keys: Direct New Relic API key in configuration (NRAK- format).
2. Account access: Account ID for data scope definition.

Required API Permissions:
- NerdGraph API access for querying monitoring data
- Account-level access for the specified account ID

Steps to obtain credentials:
1. Go to your New Relic account.
2. Navigate to API Keys section.
3. Create a new API key with appropriate permissions.
4. For production: We recommend adding credentials as **GitHub Secrets** (see [Deployment](#deployment) section).
5. For development: You can add credentials to `configuration.json` (remember not to commit this file).

## Pagination

The connector handles data retrieval from New Relic NerdGraph API with efficient pagination:

- NRQL queries: Limited to 1000 results per query with time-based filtering
- Entity search: Supports cursor-based pagination for large result sets
- Data volume Management: Uses appropriate time ranges to manage data volume
- Incremental syncs: Implements incremental syncs to reduce data transfer

The connector implements efficient data handling by:
- Processing complete API responses in memory
- Using appropriate time ranges to manage data volume
- Implementing incremental syncs to reduce data transfer
- Applying filters to focus on relevant data subsets

Refer to functions `get_apm_data`, `get_infrastructure_data`, `get_browser_data`, `get_mobile_data`, and `get_synthetic_data` in `connector.py` for the data retrieval implementation.

## Dynamic time range management

The connector intelligently manages data fetching based on sync type:

### Initial sync
- Time range: Last 90 days of data
- Use case: First-time setup or full re-sync
- Data volume: Comprehensive historical data for baseline analysis

### Incremental sync
- Time range: Data since last successful sync timestamp
- Use case: Regular scheduled syncs
- Data volume: Only new/changed data since last sync
- Benefits: Faster sync times, reduced API calls, efficient resource usage

### Implementation details
- State management: Uses Fivetran's checkpoint system to track last sync time
- Automatic detection: Automatically determines sync type based on state
- Flexible queries: All NRQL queries dynamically adjust time ranges
- Logging: Clear indication of sync type and time ranges in logs

## Data handling

The connector processes New Relic monitoring data through several stages:

1. Data extraction: Direct GraphQL queries to NerdGraph endpoints.
2. Data transformation: Conversion of New Relic API responses to structured table format.
3. Schema mapping: Consistent data types and column naming across all tables.
4. State management: Checkpoint-based incremental sync support.
5. Error handling: Comprehensive error handling with logging and validation.

Data Processing Features:
- Type conversion: New Relic API responses converted to appropriate data types
- Default values: Missing dimensions populated with appropriate defaults
- Timestamp handling: ISO format timestamp conversion and period-based grouping
- Data aggregation: Multiple metric aggregations (averages, counts, percentages)
- Filtering: Service and account-level filtering based on configuration
- Dynamic time ranges: Intelligent time range selection for initial vs incremental syncs

Data Flow:
New Relic NerdGraph API → Requests Client → Dynamic Time Range Processing → Data Processing Functions → Fivetran Operations → Data Warehouse Tables

Refer to functions `get_time_range`, `execute_nerdgraph_query`, `get_apm_data`, `get_infrastructure_data`, `get_browser_data`, `get_mobile_data`, and `get_synthetic_data` in `connector.py` for detailed data handling logic.

## Error handling

The connector implements comprehensive error handling strategies to ensure robust operation:

Configuration validation errors:
- ValueError: Missing required configuration values (`api_key`, `account_id`, `region`)
- ValueError: Invalid API key format (must start with `NRAK-`)
- ValueError: Invalid region specification (must be `US` or `EU`)

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

Refer to functions `validate_configuration`, `execute_nerdgraph_query`, and the main `update` function in `connector.py` for error handling implementation.

## Tables created

The connector creates the following tables for comprehensive New Relic monitoring analysis. Column types are automatically inferred by Fivetran based on the actual data structure and content.

### apm_data
Primary table for application performance monitoring data with transaction-level metrics and performance indicators.

Primary key: `account_id`, `timestamp`, `transaction_name`

Sample columns (automatically inferred by Fivetran):
- `account_id` - New Relic account identifier
- `timestamp` - Data collection timestamp
- `transaction_name` - Name of the monitored transaction
- `duration` - Transaction response time in milliseconds
- `error_rate` - Error rate percentage (0-1)
- `throughput` - Transactions per second
- `apdex_score` - Apdex performance score (0-1)

### infrastructure_data
Infrastructure monitoring data for host status, domain analysis, and infrastructure health tracking.

Primary key: `account_id`, `timestamp`, `host_name`

Sample columns (automatically inferred by Fivetran):
- `account_id` - New Relic account identifier
- `timestamp` - Data collection timestamp
- `host_name` - Infrastructure host name
- `domain` - Host domain classification
- `type` - Host type classification
- `reporting` - Whether the host is currently reporting
- `tags` - JSON-formatted host tags and metadata

### browser_data
Browser performance monitoring data for user experience analysis and page performance insights.

Primary key: `account_id`, `timestamp`, `page_url`

Sample columns (automatically inferred by Fivetran):
- `account_id` - New Relic account identifier
- `timestamp` - Data collection timestamp
- `page_url` - Monitored page URL
- `browser_name` - Browser name and version
- `browser_version` - Browser version information
- `device_type` - Device type classification
- `load_time` - Page load time in milliseconds
- `dom_content_loaded` - DOM content loaded time in milliseconds

### mobile_data
Mobile application monitoring data for crash analysis, platform performance, and device utilization insights.

Primary key: `account_id`, `timestamp`, `app_name`

Sample columns (automatically inferred by Fivetran):
- `account_id` - New Relic account identifier
- `timestamp` - Data collection timestamp
- `app_name` - Mobile application name
- `platform` - Mobile platform (iOS/Android)
- `version` - Application version
- `device_model` - Device model information
- `os_version` - Operating system version
- `crash_count` - Number of crashes detected

### synthetic_data
Synthetic monitoring data for uptime monitoring, response time analysis, and failure detection.

Primary key: `account_id`, `timestamp`, `monitor_name`

Sample columns (automatically inferred by Fivetran):
- `account_id` - New Relic account identifier
- `timestamp` - Data collection timestamp
- `monitor_name` - Synthetic monitor name
- `monitor_type` - Type of synthetic monitor
- `status` - Monitor execution status
- `response_time` - Monitor response time in milliseconds
- `location` - Monitor execution location
- `error_message` - Error message if monitor failed

## Additional files

The connector includes several additional files to support functionality and deployment:

* `.github/workflows/deploy_connector.yaml` – Primary deployment mechanism: GitHub Actions workflow for automated deployment to Fivetran with parameterized configuration and secure credential management.

* `requirements.txt` – Python dependency specification for New Relic API integration and connector requirements.

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.

Important new relic limitations:
- NRQL queries limited to 1000 results per query
- API rate limits apply to large accounts
- Data retention varies by New Relic plan
- Some features require specific New Relic licenses
- Historical data availability depends on account settings

Performance considerations:
- Large accounts may experience longer sync times
- Multiple monitoring services increase API calls
- Historical data syncs can be resource-intensive
- Use incremental syncs for production environments

Security best practices:
- We recommend using API keys with minimal required permissions
- Consider regularly rotating API keys for enhanced security
- Monitor API usage and rate limits to maintain optimal performance
- Implement proper error handling and logging for better troubleshooting

GitHub actions workflow management:
- Recommended production method: We recommend using GitHub Actions workflow for production deployments
- Keep secrets and variables updated in GitHub repository settings
- Use environment protection rules for production deployments
- Monitor workflow execution logs for any deployment issues
- Regularly review and update workflow permissions and access controls
- Consider using branch protection rules to prevent unauthorized deployments
- Manual deployment: Available for development, testing, and debugging, but we encourage using GitHub Actions for production
