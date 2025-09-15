# New Relic Feature APIs Connector

This connector provides comprehensive New Relic monitoring data synchronization into your data warehouse for advanced performance analytics, infrastructure monitoring, and application insights. It leverages New Relic's NerdGraph API to extract APM, Infrastructure, Browser, Mobile, and Synthetic monitoring data for comprehensive observability analysis.

## Connector overview

The New Relic Feature APIs Connector is a Fivetran Connector SDK implementation that extracts comprehensive monitoring and performance data from New Relic's various feature APIs. It provides detailed insights into application performance, infrastructure health, user experience, and system reliability across multiple New Relic services.

**Data Source**: New Relic NerdGraph API (GraphQL queries for APM, Infrastructure, Browser, Mobile, and Synthetic monitoring data)

**Use Cases**: Application performance monitoring, infrastructure health tracking, user experience optimization, mobile app quality assurance, synthetic monitoring, and comprehensive observability analysis.

## Requirements

- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)   
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started

### Quick start (Recommended)

1. Fork or clone this repository.
2. Set up GitHub secrets and variables (see [Deployment](#deployment) section).
3. Push changes to `main` branch - deployment happens automatically!

### Alternative: Manual setup

For local development and testing, refer to the [Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) and the [Manual Deployment](#manualdeployment) section below.

## Deployment

> **PRODUCTION DEPLOYMENT RECOMMENDATION**  
> **We recommend using GitHub Actions workflow for all production deployments.**  
> Manual deployment is available for development, testing, and debugging purposes, but we encourage using the automated workflow for production environments.

The connector is designed to work with **GitHub Actions workflow** as the **recommended production deployment mechanism**, while keeping manual deployment available for development and testing scenarios.

### Primary: Automated deployment via GitHub Actions

The connector includes a GitHub Actions workflow (`.github/workflows/deploy_connector.yaml`) for automated deployment.

#### Workflow features:
- **Automated Triggers**: Deploys on pushes to the `main` branch
- **Path-Based Triggers**: Only runs when changes are made to the connector directory
- **Parameterized Configuration**: Easy to customize Python version, working directory, and other settings
- **Environment Management**: Uses GitHub Environments for secure credential management
- **Zero-Downtime Deployment**: Seamless updates without service interruption

#### Quick setup:

1. **Create GitHub Environment:**
   - Go to repository Settings → Environments
   - Create environment named `Fivetran`

2. **Add Repository Secrets** (Settings → Secrets and variables → Actions):
   ```
   NEWRELIC_API_KEY          # Your New Relic API key (NRAK-xxxxx)
   NEWRELIC_ACCOUNT_ID       # Your New Relic account ID
   NEWRELIC_REGION           # Your New Relic region (US or EU)
   FIVETRAN_API_KEY          # Your Fivetran API key
   ```

3. **Add Repository Variables** (Settings → Secrets and variables → Actions):
   ```
   FIVETRAN_DEV_DESTINATION  # Your Fivetran destination ID
   NEWRELIC_DEV              # Your Fivetran connection name
   ```

4. **Deploy**: Simply push changes to the `main` branch - deployment happens automatically!

#### Workflow configuration:

The workflow uses parameterized environment variables for easy customization:

```yaml
env:
  PYTHON_VERSION: '3.11'
  WORKING_DIRECTORY: '.'
  CONNECTOR_NAME: 'New Relic'
  CONFIG_FILE: 'configuration.json'
  EXCLUDED_DEPENDENCIES: '^requests\b'
```

#### Deployment process:

1. **Automatic Trigger**: Push changes to `main` branch
2. **Environment Setup**: Python 3.11, dependencies installation
3. **Configuration Creation**: Generates `configuration.json` from GitHub secrets
4. **Fivetran Deployment**: Executes `fivetran deploy` command
5. **Status Reporting**: Provides deployment success/failure feedback

#### Customization:

To adapt this workflow for other connectors, simply update the environment variables:

```yaml
env:
  PYTHON_VERSION: '3.11'
  WORKING_DIRECTORY: 'connections/csdk/other-connector'
  CONNECTOR_NAME: 'Other'
  CONFIG_FILE: 'configuration.json'
  EXCLUDED_DEPENDENCIES: '^requests\b'
```

### <a name="manualdeployment"></a>Secondary: Manual Deployment (Debugging & Testing)

For local development, testing, and debugging purposes:

1. **Install Dependencies:**
   ```bash
   pip install fivetran-connector-sdk
   ```

2. **Configure Credentials:**
   - Update `configuration.json` with your New Relic API credentials
   - Ensure the file is not committed to version control

3. **Deploy to Fivetran:**
   ```bash
   fivetran deploy --api-key YOUR_FIVETRAN_API_KEY --destination YOUR_DESTINATION_ID --connection YOUR_CONNECTION_NAME --configuration configuration.json --python-version 3.11
   ```

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
* **CI/CD Integration**: Automated deployment via GitHub Actions with parameterized configuration
* **Environment Management**: Secure credential management using GitHub Environments
* **Zero-Downtime Deployment**: Seamless updates without service interruption

## Configuration file

**PRODUCTION DEPLOYMENT GUIDANCE**: For production deployments, we recommend using **GitHub secrets** configured in the GitHub Actions workflow. Local configuration files can be used for development and testing, but we encourage using GitHub secrets for production environments.

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

**Required Configuration Keys:**
- `api_key`: New Relic API key for NerdGraph API access (must start with NRAK-)
- `account_id`: New Relic account ID for data extraction

**Optional Configuration Keys:**
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

### Production Deployment (Recommended)
- **Method**: GitHub Actions workflow with GitHub secrets
- **Security**: Credentials stored securely in GitHub
- **Automation**: Push-to-deploy workflow
- **Compliance**: Enterprise-grade security practices

### Development & Testing (Available)
- **Method**: Local `configuration.json` or environment variables
- **Security**: Credentials stored locally (never commit to repository)
- **Use Case**: Local development, debugging, testing
- **Guidance**: We recommend using GitHub secrets for production

**Security Note**: For production deployments, we recommend using **GitHub secrets**. Local configuration files are available for development but we encourage using GitHub secrets for production environments.

Note: We recommend keeping the `configuration.json` file out of version control to protect sensitive information. You can add it to your `.gitignore` file for additional security.

## Requirements file

The `requirements.txt` file specifies the Python libraries required by the connector for New Relic API integration and data processing.

**Example content of `requirements.txt`:**

```
fivetran-connector-sdk>=2.0.0
requests>=2.28.0
```

Note: The `fivetran_connector_sdk:latest` package (v2.0.0+) is pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare it in your `requirements.txt`. 

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
4. **For Production**: We recommend adding credentials as GitHub secrets (see [Deployment](#deployment) section)
5. **For Development**: You can add credentials to `configuration.json` (remember not to commit this file)

### Development configuration (Local Testing)

**Development Setup**: The following configuration methods are designed for local development and testing. We recommend using GitHub secrets for production deployments.

For local development and testing:

```bash
# Option 1: Set environment variables locally
export NEWRELIC_API_KEY="your_api_key"
export NEWRELIC_ACCOUNT_ID="your_account_id"
export NEWRELIC_REGION="US"

# Option 2: Use configuration.json file (never commit this file)
# Update configuration.json with your test credentials
```

**Production Deployment**: We recommend using the GitHub Actions workflow with GitHub secrets (see [Deployment](#deployment) section above).

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

## Dynamic time range management

The connector intelligently manages data fetching based on sync type:

### Initial sync
- **Time Range**: Last 90 days of data
- **Use Case**: First-time setup or full re-sync
- **Data Volume**: Comprehensive historical data for baseline analysis

### Incremental sync
- **Time Range**: Data since last successful sync timestamp
- **Use Case**: Regular scheduled syncs
- **Data Volume**: Only new/changed data since last sync
- **Benefits**: Faster sync times, reduced API calls, efficient resource usage

### Implementation details
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

The connector creates the following tables for comprehensive New Relic monitoring analysis. **Column types are automatically inferred by Fivetran** based on the actual data structure and content.

### apm_data
Primary table for application performance monitoring data with transaction-level metrics and performance indicators.

**Primary Key**: `account_id`, `timestamp`, `transaction_name`

**Sample Columns** (automatically inferred by Fivetran):
- `account_id` - New Relic account identifier
- `timestamp` - Data collection timestamp
- `transaction_name` - Name of the monitored transaction
- `duration` - Transaction response time in milliseconds
- `error_rate` - Error rate percentage (0-1)
- `throughput` - Transactions per second
- `apdex_score` - Apdex performance score (0-1)

### infrastructure_data
Infrastructure monitoring data for host status, domain analysis, and infrastructure health tracking.

**Primary Key**: `account_id`, `timestamp`, `host_name`

**Sample Columns** (automatically inferred by Fivetran):
- `account_id` - New Relic account identifier
- `timestamp` - Data collection timestamp
- `host_name` - Infrastructure host name
- `domain` - Host domain classification
- `type` - Host type classification
- `reporting` - Whether the host is currently reporting
- `tags` - JSON-formatted host tags and metadata

### browser_data
Browser performance monitoring data for user experience analysis and page performance insights.

**Primary Key**: `account_id`, `timestamp`, `page_url`

**Sample Columns** (automatically inferred by Fivetran):
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

**Primary Key**: `account_id`, `timestamp`, `app_name`

**Sample Columns** (automatically inferred by Fivetran):
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

**Primary Key**: `account_id`, `timestamp`, `monitor_name`

**Sample Columns** (automatically inferred by Fivetran):
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

* **`.github/workflows/deploy_connector.yaml`** – **Primary deployment mechanism**: GitHub Actions workflow for automated deployment to Fivetran with parameterized configuration and secure credential management.

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
- We recommend using API keys with minimal required permissions
- Consider regularly rotating API keys for enhanced security
- Monitor API usage and rate limits to maintain optimal performance
- Implement proper error handling and logging for better troubleshooting

**GitHub Actions Workflow Management:**
- **Recommended Production Method**: We recommend using GitHub Actions workflow for production deployments
- Keep secrets and variables updated in GitHub repository settings
- Use environment protection rules for production deployments
- Monitor workflow execution logs for any deployment issues
- Regularly review and update workflow permissions and access controls
- Consider using branch protection rules to prevent unauthorized deployments
- **Manual Deployment**: Available for development, testing, and debugging, but we encourage using GitHub Actions for production

## References

- [New Relic API Documentation](https://docs.newrelic.com/docs/apis/intro-apis/introduction-new-relic-apis)
- [NerdGraph API](https://docs.newrelic.com/docs/apis/nerdgraph/get-started/introduction-new-relic-nerdgraph/)
- [Fivetran Connector SDK Documentation](https://fivetran.com/docs/connector-sdk)
- [Fivetran Connector SDK Best Practices](https://fivetran.com/docs/connectors/connector-sdk/best-practices)
- [Fivetran Connector SDK v2.0.0+ Migration Guide](https://fivetran.com/docs/connector-sdk/tutorials/removing-yield-usage)
