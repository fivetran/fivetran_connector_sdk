# Quickbase API Connector

This connector provides comprehensive Quickbase application and data synchronization into your data warehouse for business process analysis, application data management, and operational insights. It leverages Quickbase's REST API to extract applications, tables, fields, and records data for comprehensive business intelligence and analytics.

## Connector overview

The Quickbase API Connector is a Fivetran Connector SDK implementation that extracts comprehensive application and business data from Quickbase's REST API endpoints. It provides detailed insights into application structure, data relationships, field definitions, and record-level information across multiple Quickbase applications.

Data source: Quickbase REST API (v1) for applications, tables, fields, and records

Use cases: Business process analysis, application data management, operational analytics, data governance, business intelligence, and comprehensive application insights.

## Requirements

- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)   
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

- Comprehensive application data: Complete application metadata including ancestry tracking, classification labels, memory usage, security properties, and configuration settings
- Enhanced field schema management: Complete field definitions including audit tracking, help text, data copy settings, mode information, and all calculation capabilities
- Table structure analysis: Table definitions, relationships, field mappings, and data organization
- Records data extraction: Complete record-level data with field mappings, relationships, and intelligent timestamp handling
- Advanced pagination metadata: Detailed pagination tracking with total records, field counts, skip/top parameters for optimal sync monitoring
- Governance & compliance support: Application ancestry tracking, data classification labels, audit logging flags, and field-level governance metadata
- Multi-application support: Extract data from multiple Quickbase applications simultaneously
- Dynamic schema discovery: Automatic detection of application structure and field definitions with complete property capture
- Intelligent timestamp handling: Preserves original API timestamps when available, falls back to processing time only when necessary
- Incremental syncs: Efficient data updates with checkpoint-based state management and enhanced metadata tracking
- Configurable table selection: Choose specific tables or sync entire applications
- Error handling: Robust error handling with comprehensive logging and validation
- Mock testing framework: Complete testing suite with Faker-generated realistic data
- CI/CD integration: Automated deployment via GitHub Actions with comprehensive quality checks
- Environment management: Secure credential management using GitHub Environments
- Zero-downtime deployment: Seamless updates without service interruption

## Configuration file

PRODUCTION DEPLOYMENT GUIDANCE: For production deployments, we recommend using **GitHub secrets** configured in the GitHub Actions workflow. Local configuration files can be used for development and testing, but we encourage using GitHub secrets for production environments.

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

Required configuration keys:
- `user_token`: Quickbase user token (QB-USER-TOKEN format) for REST API access
- `realm_hostname`: Quickbase realm hostname (e.g., company.quickbase.com)
- `app_id`: Quickbase application ID for data extraction

Optional configuration keys:
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

## Requirements file

The `requirements.txt` file specifies the Python libraries required by the connector for Quickbase API integration and data processing.

Example content of `requirements.txt`:

```
requests>=2.31.0
faker>=19.6.0
```

Note: The `fivetran_connector_sdk:latest` package (v2.0.0+) is pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare it in your `requirements.txt`. This connector uses the updated SDK version that does not require `yield` statements for operations.

## Authentication

The connector uses Quickbase user tokens for authentication with the REST API. Authentication is provided through:

1. User tokens: Quickbase user token in QB-USER-TOKEN format.
2. Realm access: Realm hostname for workspace access.
3. Application-level access: Application ID for data scope definition.

Required API Permissions:
- REST API access for querying application data
- Application-level read permissions for the specified application ID
- Table and field read permissions for data extraction

We recommend reviewing these permissions with your Quickbase administrator to ensure proper access levels.

Steps to obtain credentials:
1. Go to your Quickbase application.
2. Navigate to **Settings > App properties > Advanced settings**.
3. Generate a user token with read permissions.
4. For production: We recommend adding credentials as **GitHub Secrets** (see [Deployment](#deployment) section).
5. For development: You can add credentials to `configuration.json` (remember not to commit this file).

## Pagination

The connector handles data retrieval from Quickbase REST API with efficient pagination:

- Records API: Supports up to 1000 results per page with skip/top parameters
- Fields API: Batch processing for field metadata extraction
- Applications API: Single application details per request
- Data volume management: Uses appropriate batch sizes to manage data volume
- Incremental syncs: Implements incremental syncs to reduce data transfer

The connector implements efficient data handling by:
- Processing complete API responses in memory
- Using appropriate batch sizes to manage data volume
- Implementing incremental syncs to reduce data transfer
- Applying filters to focus on relevant data subsets

Refer to functions `get_records_data`, `get_applications_data`, `get_tables_data`, and `get_fields_data` in `connector.py` for the data retrieval implementation.

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
- Flexible queries: All API calls dynamically adjust time ranges
- Logging: Clear indication of sync type and time ranges in logs

## Data handling

The connector processes Quickbase data through several stages:

1. Data extraction: Direct REST API calls to Quickbase endpoints.
2. Data transformation: Conversion of Quickbase API responses to structured table format.
3. Schema mapping: Consistent data types and column naming across all tables.
4. State management: Checkpoint-based incremental sync support.
5. Error handling: Comprehensive error handling with logging and validation.

Data Processing Features:
- Type conversion: Quickbase API responses converted to appropriate data types
- Default values: Missing dimensions populated with appropriate defaults
- Timestamp handling: ISO format timestamp conversion and period-based grouping
- Data aggregation: Multiple field types and record structures
- Filtering: Application and table-based filtering based on configuration
- Dynamic time ranges: Intelligent time range selection for initial vs incremental syncs

Data Flow:
Quickbase REST API → Requests Client → Dynamic Time Range Processing → Data Processing Functions → Fivetran Operations → Data Warehouse Tables

Refer to functions `get_time_range`, `execute_api_request`, `get_applications_data`, `get_tables_data`, `get_fields_data`, and `get_records_data` in `connector.py` for detailed data handling logic.

## Error handling

The connector implements comprehensive error handling strategies to ensure robust operation:

Configuration validation errors:
- ValueError: Missing required configuration values (`user_token`, `realm_hostname`)
- ValueError: Empty user token or realm hostname values
- ValueError: Invalid numeric parameters (sync frequency, initial sync days, max records)

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

The connector creates the following tables for comprehensive Quickbase data analysis. Fivetran will automatically infer column types and structures from the data.

### application
Primary table for application metadata and configuration information with complete governance support.

Key fields:
- `app_id` (Primary Key): Unique Quickbase application identifier
- `name`: Application name
- `description`: Application description
- `created`: Application creation timestamp
- `updated`: Application last update timestamp
- `date_format`: Application date format setting
- `time_zone`: Application time zone setting
- `ancestor_id`: ID of the app from which this app was copied (lineage tracking)
- `has_everyone_on_internet`: Indicates if app includes Everyone On The Internet access
- `data_classification`: Data classification label for compliance (None, Confidential, etc.)
- `variables`: Application variables (JSON string)
- `security_properties`: Security settings (JSON string)
- `memory_info`: Memory usage information (JSON string with estMemory, estMemoryInclDependentApps)
- `timestamp`: Data extraction timestamp (preserves API timestamp when available)

### table
Table structure and metadata for application data organization.

Key fields:
- `table_id` (Primary Key): Unique table identifier
- `app_id`: Parent application identifier
- `name`: Table name
- `description`: Table description
- `created`: Table creation timestamp
- `updated`: Table last update timestamp
- `key_field_id`: Primary key field identifier
- `next_field_id`: Next available field ID
- `next_record_id`: Next available record ID
- `default_sort_field_id`: Default sort field identifier
- `timestamp`: Data extraction timestamp

### field
Complete field definitions and metadata for comprehensive data schema analysis and governance.

Key fields:
- `field_id` + `table_id` (Composite Primary Key): Unique field identifier and parent table
- `name`: Field name/label
- `field_type`: Quickbase field type (text, numeric, date, etc.)
- `mode`: Field mode information
- `no_wrap`: Text wrapping setting
- `bold`: Bold text formatting
- `required`: Required field indicator
- `appears_by_default`: Default appearance setting
- `find_enabled`: Search capability
- `unique`: Unique value constraint
- `does_total`: Total calculation capability
- `does_average`: Average calculation capability
- `does_max`: Maximum calculation capability
- `does_min`: Minimum calculation capability
- `does_stddev`: Standard deviation calculation
- `does_count`: Count calculation capability
- `does_data_copy`: Whether field supports data copying
- `field_help`: Help text shown to users within the product
- `audited`: Indicates if field is tracked in Quickbase Audit Logs
- `timestamp`: Data extraction timestamp (preserves API timestamp when available)

### record
Primary data table containing all record-level information with dynamic columns based on table structure and intelligent timestamp handling.

Key fields:
- `table_id` + `record_id` (Composite Primary Key): Parent table identifier and unique record identifier
- `timestamp`: Data extraction timestamp (preserves record modification time when available)
- `[field_name]`: Dynamic columns based on field definitions

### sync_metadata
Enhanced synchronization tracking and operational metadata with detailed pagination information for monitoring and troubleshooting.

Key fields:
- `table_id` (Primary Key): Table identifier
- `last_sync_time`: Last successful sync timestamp
- `total_records_synced`: Number of records synchronized in this batch
- `sync_status`: Sync operation status (success, error)
- `error_message`: Error details if sync failed
- `total_records`: Total records available in the API (from metadata)
- `num_records`: Number of records returned in API response
- `num_fields`: Number of fields per record in the response
- `skip`: Number of records skipped (pagination offset)
- `top`: Maximum records requested per page
- `timestamp`: Metadata extraction timestamp

## Additional files

The connector includes several additional files to support functionality and deployment:

- `.github/workflows/deploy_connector.yaml` – Primary deployment mechanism: GitHub Actions workflow for automated deployment to Fivetran with comprehensive code quality checks, parameterized configuration, and secure credential management.

- `requirements.txt` – Python dependency specification for Quickbase API integration and connector requirements.

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.

Important quickbase limitations:
- API rate limits apply based on subscription plan
- User token permissions determine data access scope
- Some features require specific Quickbase licenses
- Historical data availability depends on application settings
- API pagination limits (1000 records per page maximum)

We recommend reviewing these limitations with your Quickbase administrator to ensure it meets your data requirements.

Performance considerations:
- Large applications may experience longer sync times
- Multiple tables increase API calls
- Historical data syncs can be resource-intensive
- We recommend using incremental syncs for production environments to optimize performance

Security best practices:
- We recommend using user tokens with minimal required permissions
- Consider regularly rotating user tokens for enhanced security
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
