# Quickbase API Connector

This connector provides comprehensive Quickbase application and data synchronization into your data warehouse for business process analysis, application data management, and operational insights. It leverages Quickbase's REST API to extract applications, tables, fields, and records data for comprehensive business intelligence and analytics.

## Connector overview

The Quickbase API Connector is a Fivetran Connector SDK implementation that extracts comprehensive application and business data from Quickbase's REST API endpoints. It provides detailed insights into application structure, data relationships, field definitions, and record-level information across multiple Quickbase applications.

**Data Source**: Quickbase REST API (v1) for applications, tables, fields, and records

**Use Cases**: Business process analysis, application data management, operational analytics, data governance, business intelligence, and comprehensive application insights.

## Requirements

- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)   
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started

### Quick Start (Recommended)

1. Fork or clone this repository.
2. Set up GitHub secrets and variables (see [Deployment](#deployment) section).
3. Push changes to `main` branch - deployment happens automatically!

### Alternative: Manual Setup

For local development and testing, refer to the [Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) and the [Manual Deployment](#-secondary-manual-deployment-debugging--testing) section below.

## Deployment

> **PRODUCTION DEPLOYMENT RECOMMENDATION**  
> **We recommend using GitHub Actions workflow for all production deployments.**  
> Manual deployment is available for development, testing, and debugging purposes, but we encourage using the automated workflow for production environments.

The connector is designed to work with **GitHub Actions workflow** as the **recommended production deployment mechanism**, while keeping manual deployment available for development and testing scenarios.

### Primary: Automated Deployment via GitHub Actions

The connector includes a GitHub Actions workflow (`.github/workflows/deploy_connector.yaml`) for automated deployment.

#### Workflow Features:
- **Automated Triggers**: Deploys on pushes to the `main` branch
- **Path-Based Triggers**: Only runs when changes are made to the connector directory
- **Parameterized Configuration**: Easy to customize Python version, working directory, and other settings
- **Environment Management**: Uses GitHub Environments for secure credential management
- **Code Quality Checks**: Black formatting, Flake8 linting, unit tests, and mock integration tests
- **Zero-Downtime Deployment**: Seamless updates without service interruption

#### Quick Setup:

1. **Create GitHub Environment:**
   - Go to repository Settings → Environments
   - Create environment named `Fivetran`

2. **Add Repository Secrets** (Settings → Secrets and variables → Actions):
   ```
   QUICKBASE_USER_TOKEN        # Your Quickbase user token
   QUICKBASE_REALM_HOSTNAME    # Your Quickbase realm hostname
   QUICKBASE_APP_ID            # Your Quickbase application ID
   FIVETRAN_API_KEY            # Your Fivetran API key
   ```

3. **Add Repository Variables** (Settings → Secrets and variables → Actions):
   ```
   FIVETRAN_DEV_DESTINATION    # Your Fivetran destination ID
   QUICKBASE_DEV               # Your Fivetran connection name
   QUICKBASE_TABLE_IDS         # Comma-separated table IDs (optional)
   ```

4. **Deploy**: Simply push changes to the `main` branch - deployment happens automatically!

#### Workflow Configuration:

The workflow uses parameterized environment variables for easy customization:

```yaml
env:
  PYTHON_VERSION: '3.11'
  WORKING_DIRECTORY: 'connections/csdk/quickbase-connector'
  CONNECTOR_NAME: 'Quickbase'
  CONFIG_FILE: 'configuration.json'
  EXCLUDED_DEPENDENCIES: '^requests\b'
```

#### Deployment Process:

1. **Automatic Trigger**: Push changes to `main` branch.
2. **Code Quality**: Black formatting, Flake8 linting, unit tests, mock integration tests.
3. **Environment Setup**: Python 3.11, dependencies installation.
4. **Configuration Creation**: Generates `configuration.json` from GitHub secrets.
5. **Fivetran Deployment**: Executes `fivetran deploy` command.
6. **Status Reporting**: Provides deployment success/failure feedback.

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

### Secondary: Manual Deployment (Debugging & Testing)

For local development, testing, and debugging purposes:

1. **Install Dependencies:**
   ```bash
   pip install fivetran-connector-sdk faker requests
   ```

2. **Configure Credentials:**
   - Update `configuration.json` with your Quickbase API credentials
   - Ensure the file is not committed to version control

3. **Deploy to Fivetran:**
   ```bash
   fivetran deploy --api-key YOUR_FIVETRAN_API_KEY --destination YOUR_DESTINATION_ID --connection YOUR_CONNECTION_NAME --configuration configuration.json --python-version 3.11
   ```

## Features

* **Comprehensive Application Data**: Complete application metadata including ancestry tracking, classification labels, memory usage, security properties, and configuration settings
* **Enhanced Field Schema Management**: Complete field definitions including audit tracking, help text, data copy settings, mode information, and all calculation capabilities
* **Table Structure Analysis**: Table definitions, relationships, field mappings, and data organization
* **Records Data Extraction**: Complete record-level data with field mappings, relationships, and intelligent timestamp handling
* **Advanced Pagination Metadata**: Detailed pagination tracking with total records, field counts, skip/top parameters for optimal sync monitoring
* **Governance & Compliance Support**: Application ancestry tracking, data classification labels, audit logging flags, and field-level governance metadata
* **Multi-Application Support**: Extract data from multiple Quickbase applications simultaneously
* **Dynamic Schema Discovery**: Automatic detection of application structure and field definitions with complete property capture
* **Intelligent Timestamp Handling**: Preserves original API timestamps when available, falls back to processing time only when necessary
* **Incremental Syncs**: Efficient data updates with checkpoint-based state management and enhanced metadata tracking
* **Configurable Table Selection**: Choose specific tables or sync entire applications
* **Error Handling**: Robust error handling with comprehensive logging and validation
* **Mock Testing Framework**: Complete testing suite with Faker-generated realistic data
* **CI/CD Integration**: Automated deployment via GitHub Actions with comprehensive quality checks
* **Environment Management**: Secure credential management using GitHub Environments
* **Zero-Downtime Deployment**: Seamless updates without service interruption

## Configuration file

**PRODUCTION DEPLOYMENT GUIDANCE**: For production deployments, we recommend using **GitHub secrets** configured in the GitHub Actions workflow. Local configuration files can be used for development and testing, but we encourage using GitHub secrets for production environments.

The connector requires the following configuration keys. For production deployments, we recommend configuring these as **GitHub secrets** (see [Deployment](#deployment) section). For local development and testing, you can define them in `configuration.json`:

```json
{
  "user_token": "QB-USER-TOKEN your_token_here",
  "realm_hostname": "your_company.quickbase.com",
  "app_id": "your_app_id",
  "table_ids": "table1,table2,table3",
  "sync_frequency_hours": "4",
  "initial_sync_days": "90",
  "max_records_per_page": "1000",
  "request_timeout_seconds": "30",
  "retry_attempts": "3",
  "enable_incremental_sync": "true",
  "enable_fields_sync": "true",
  "enable_records_sync": "true",
  "date_field_for_incremental": "3",
  "enable_debug_logging": "false"
}
```

**Required Configuration Keys:**
- `user_token`: Quickbase user token (QB-USER-TOKEN format) for REST API access
- `realm_hostname`: Quickbase realm hostname (e.g., company.quickbase.com)
- `app_id`: Quickbase application ID for data extraction

**Optional Configuration Keys:**
- `table_ids`: Comma-separated list of specific table IDs (empty = sync all tables)
- `sync_frequency_hours`: Sync frequency in hours (1-24, default: 4)
- `initial_sync_days`: Days of historical data for initial sync (1-365, default: 90)
- `max_records_per_page`: Maximum records per API request (1-1000, default: 1000)
- `request_timeout_seconds`: API request timeout in seconds (default: 30)
- `retry_attempts`: Number of retry attempts for failed requests (default: 3)
- `enable_incremental_sync`: Enable incremental sync (true/false, default: true)
- `enable_fields_sync`: Enable field metadata sync (true/false, default: true)
- `enable_records_sync`: Enable record data sync (true/false, default: true)
- `date_field_for_incremental`: Field ID for incremental sync filtering (default: 3)
- `enable_debug_logging`: Enable debug logging (true/false, default: false)

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

**User Token Setup:**
1. Go to your Quickbase application.
2. Navigate to Settings > App properties > Advanced settings.
3. Generate a user token with appropriate permissions.
4. Add the token to `configuration.json` with QB-USER-TOKEN prefix.

**Realm Hostname:**
- Found in your Quickbase URL (e.g., company.quickbase.com)
- Used to connect to your specific Quickbase realm

**Application ID:**
- Found in the Quickbase application URL
- Used to scope data extraction to specific applications

Note: We recommend keeping the `configuration.json` file out of version control to protect sensitive information. You can add it to your `.gitignore` file for additional security.

### Development Configuration (Local Testing)

**Development Setup**: The following configuration methods are designed for local development and testing. We recommend using GitHub secrets for production deployments.

For local development and testing:

```bash
# Option 1: Set environment variables locally
export QUICKBASE_USER_TOKEN="QB-USER-TOKEN your_token"
export QUICKBASE_REALM_HOSTNAME="company.quickbase.com"
export QUICKBASE_APP_ID="your_app_id"

# Option 2: Use configuration.json file (never commit this file)
# Update configuration.json with your test credentials
```

**Production Deployment**: We recommend using the GitHub Actions workflow with GitHub secrets (see [Deployment](#deployment) section above).

## Requirements file

The `requirements.txt` file specifies the Python libraries required by the connector for Quickbase API integration and data processing.

**Example content of `requirements.txt`:**

```
requests>=2.31.0
faker>=19.6.0
```

Note: The `fivetran_connector_sdk:latest` package (v2.0.0+) is pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare it in your `requirements.txt`. This connector uses the updated SDK version that does not require `yield` statements for operations.

## Authentication

The connector uses Quickbase user tokens for authentication with the REST API. Authentication is provided through:

1. **User Tokens**: Quickbase user token in QB-USER-TOKEN format.
2. **Realm Access**: Realm hostname for workspace access.
3. **Application-Level Access**: Application ID for data scope definition.

**Required API Permissions:**
- REST API access for querying application data
- Application-level read permissions for the specified application ID
- Table and field read permissions for data extraction

We recommend reviewing these permissions with your Quickbase administrator to ensure proper access levels.

**Steps to obtain credentials:**
1. Go to your Quickbase application.
2. Navigate to Settings > App properties > Advanced settings.
3. Generate a user token with read permissions.
4. **For Production**: We recommend adding credentials as GitHub secrets (see [Deployment](#deployment) section).
5. **For Development**: You can add credentials to `configuration.json` (remember not to commit this file).

## Pagination

The connector handles data retrieval from Quickbase REST API with efficient pagination:

- **Records API**: Supports up to 1000 results per page with skip/top parameters
- **Fields API**: Batch processing for field metadata extraction
- **Applications API**: Single application details per request
- **Data Volume Management**: Uses appropriate batch sizes to manage data volume
- **Incremental Syncs**: Implements incremental syncs to reduce data transfer

The connector implements efficient data handling by:
- Processing complete API responses in memory
- Using appropriate batch sizes to manage data volume
- Implementing incremental syncs to reduce data transfer
- Applying filters to focus on relevant data subsets

Refer to functions `get_records_data`, `get_applications_data`, `get_tables_data`, and `get_fields_data` in `connector.py` for the data retrieval implementation.

## Dynamic Time Range Management

The connector intelligently manages data fetching based on sync type:

### Initial Sync
- **Time Range**: Last 90 days of data
- **Use Case**: First-time setup or full re-sync
- **Data Volume**: Comprehensive historical data for baseline analysis

### Incremental Sync
- **Time Range**: Data since last successful sync timestamp
- **Use Case**: Regular scheduled syncs
- **Data Volume**: Only new/changed data since last sync
- **Benefits**: Faster sync times, reduced API calls, efficient resource usage

### Implementation Details
- **State Management**: Uses Fivetran's checkpoint system to track last sync time
- **Automatic Detection**: Automatically determines sync type based on state
- **Flexible Queries**: All API calls dynamically adjust time ranges
- **Logging**: Clear indication of sync type and time ranges in logs

## Data handling

The connector processes Quickbase data through several stages:

1. **Data Extraction**: Direct REST API calls to Quickbase endpoints.
2. **Data Transformation**: Conversion of Quickbase API responses to structured table format.
3. **Schema Mapping**: Consistent data types and column naming across all tables.
4. **State Management**: Checkpoint-based incremental sync support.
5. **Error Handling**: Comprehensive error handling with logging and validation.

**Data Processing Features:**
- **Type Conversion**: Quickbase API responses converted to appropriate data types
- **Default Values**: Missing dimensions populated with appropriate defaults
- **Timestamp Handling**: ISO format timestamp conversion and period-based grouping
- **Data Aggregation**: Multiple field types and record structures
- **Filtering**: Application and table-based filtering based on configuration
- **Dynamic Time Ranges**: Intelligent time range selection for initial vs incremental syncs

**Data Flow:**
Quickbase REST API → Requests Client → Dynamic Time Range Processing → Data Processing Functions → Fivetran Operations → Data Warehouse Tables

Refer to functions `get_time_range`, `execute_api_request`, `get_applications_data`, `get_tables_data`, `get_fields_data`, and `get_records_data` in `connector.py` for detailed data handling logic.

## Error handling

The connector implements comprehensive error handling strategies to ensure robust operation:

**Configuration Validation Errors:**
- **ValueError**: Missing required configuration values (`user_token`, `realm_hostname`)
- **ValueError**: Empty user token or realm hostname values
- **ValueError**: Invalid numeric parameters (sync frequency, initial sync days, max records)

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

Refer to functions `validate_configuration`, `execute_api_request`, and the main `update` function in `connector.py` for error handling implementation.

## Tables created

The connector creates the following tables for comprehensive Quickbase data analysis:

### applications
Primary table for application metadata and configuration information with complete governance support.

| Column Name | Type | Description |
|-------------|------|-------------|
| `app_id` | STRING | Unique Quickbase application identifier |
| `name` | STRING | Application name |
| `description` | STRING | Application description |
| `created` | STRING | Application creation timestamp |
| `updated` | STRING | Application last update timestamp |
| `date_format` | STRING | Application date format setting |
| `time_zone` | STRING | Application time zone setting |
| `ancestor_id` | STRING | ID of the app from which this app was copied (lineage tracking) |
| `has_everyone_on_internet` | BOOLEAN | Indicates if app includes Everyone On The Internet access |
| `data_classification` | STRING | Data classification label for compliance (None, Confidential, etc.) |
| `variables` | STRING | Application variables (JSON string) |
| `security_properties` | STRING | Security settings (JSON string) |
| `memory_info` | STRING | Memory usage information (JSON string with estMemory, estMemoryInclDependentApps) |
| `timestamp` | STRING | Data extraction timestamp (preserves API timestamp when available) |

### tables
Table structure and metadata for application data organization.

| Column Name | Type | Description |
|-------------|------|-------------|
| `table_id` | STRING | Unique table identifier |
| `app_id` | STRING | Parent application identifier |
| `name` | STRING | Table name |
| `description` | STRING | Table description |
| `created` | STRING | Table creation timestamp |
| `updated` | STRING | Table last update timestamp |
| `key_field_id` | STRING | Primary key field identifier |
| `next_field_id` | STRING | Next available field ID |
| `next_record_id` | STRING | Next available record ID |
| `default_sort_field_id` | STRING | Default sort field identifier |
| `timestamp` | STRING | Data extraction timestamp |

### fields
Complete field definitions and metadata for comprehensive data schema analysis and governance.

| Column Name | Type | Description |
|-------------|------|-------------|
| `field_id` | STRING | Unique field identifier |
| `table_id` | STRING | Parent table identifier |
| `name` | STRING | Field name/label |
| `field_type` | STRING | Quickbase field type (text, numeric, date, etc.) |
| `mode` | STRING | Field mode information |
| `no_wrap` | BOOLEAN | Text wrapping setting |
| `bold` | BOOLEAN | Bold text formatting |
| `required` | BOOLEAN | Required field indicator |
| `appears_by_default` | BOOLEAN | Default appearance setting |
| `find_enabled` | BOOLEAN | Search capability |
| `unique` | BOOLEAN | Unique value constraint |
| `does_total` | BOOLEAN | Total calculation capability |
| `does_average` | BOOLEAN | Average calculation capability |
| `does_max` | BOOLEAN | Maximum calculation capability |
| `does_min` | BOOLEAN | Minimum calculation capability |
| `does_stddev` | BOOLEAN | Standard deviation calculation |
| `does_count` | BOOLEAN | Count calculation capability |
| `does_data_copy` | BOOLEAN | Whether field supports data copying |
| `field_help` | STRING | Help text shown to users within the product |
| `audited` | BOOLEAN | Indicates if field is tracked in Quickbase Audit Logs |
| `timestamp` | STRING | Data extraction timestamp (preserves API timestamp when available) |

### records
Primary data table containing all record-level information with dynamic columns based on table structure and intelligent timestamp handling.

| Column Name | Type | Description |
|-------------|------|-------------|
| `table_id` | STRING | Parent table identifier |
| `record_id` | STRING | Unique record identifier |
| `timestamp` | STRING | Data extraction timestamp (preserves record modification time when available) |
| `[field_name]` | STRING | Dynamic columns based on field definitions |

### sync_metadata
Enhanced synchronization tracking and operational metadata with detailed pagination information for monitoring and troubleshooting.

| Column Name | Type | Description |
|-------------|------|-------------|
| `table_id` | STRING | Table identifier |
| `last_sync_time` | STRING | Last successful sync timestamp |
| `total_records_synced` | INT | Number of records synchronized in this batch |
| `sync_status` | STRING | Sync operation status (success, error) |
| `error_message` | STRING | Error details if sync failed |
| `total_records` | INT | Total records available in the API (from metadata) |
| `num_records` | INT | Number of records returned in API response |
| `num_fields` | INT | Number of fields per record in the response |
| `skip` | INT | Number of records skipped (pagination offset) |
| `top` | INT | Maximum records requested per page |
| `timestamp` | STRING | Metadata extraction timestamp |

## Additional files

The connector includes several additional files to support functionality and deployment:

* **`.github/workflows/deploy_connector.yaml`** – **Primary deployment mechanism**: GitHub Actions workflow for automated deployment to Fivetran with comprehensive code quality checks, parameterized configuration, and secure credential management.

* **`test_connector.py`** – Unit tests for connector validation and functionality verification.

* **`requirements.txt`** – Python dependency specification for Quickbase API integration and connector requirements.

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.

**Important Quickbase Limitations:**
- API rate limits apply based on subscription plan
- User token permissions determine data access scope
- Some features require specific Quickbase licenses
- Historical data availability depends on application settings
- API pagination limits (1000 records per page maximum)

We recommend reviewing these limitations with your Quickbase administrator to ensure it meets your data requirements.

**Performance Considerations:**
- Large applications may experience longer sync times
- Multiple tables increase API calls
- Historical data syncs can be resource-intensive
- We recommend using incremental syncs for production environments to optimize performance

**Security Best Practices:**
- We recommend using user tokens with minimal required permissions
- Consider regularly rotating user tokens for enhanced security
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

- [Quickbase API Documentation](https://developer.quickbase.com/)
- [Quickbase REST API Reference](https://developer.quickbase.com/operation/runQuery)
- [Fivetran Connector SDK Documentation](https://fivetran.com/docs/connector-sdk)
- [Fivetran Connector SDK Best Practices](https://fivetran.com/docs/connectors/connector-sdk/best-practices)
- [Fivetran Connector SDK v2.0.0+ Migration Guide](https://fivetran.com/docs/connector-sdk/tutorials/removing-yield-usage)