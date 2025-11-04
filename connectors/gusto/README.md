# Gusto API Connector

This connector provides comprehensive Gusto payroll and HR data synchronization into your data warehouse for workforce analytics, payroll insights, and human resources management. It leverages Gusto's REST API to extract employee, payroll, benefits, time-off, and company data for comprehensive workforce analysis.

## Connector overview

The Gusto API Connector is a Fivetran Connector SDK implementation that extracts comprehensive payroll and HR data from Gusto's various API endpoints. It provides detailed insights into employee management, payroll processing, benefits administration, and time-off tracking across your organization.

Data Source: Gusto REST API for employees, payroll, benefits, time-off requests, company data, and location information

Use Cases: Workforce analytics, payroll reporting, benefits analysis, time-off tracking, HR compliance reporting, and comprehensive business intelligence.

## Requirements

- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

- Comprehensive employee data: Employee records, job details, compensation, and employment status tracking
- Payroll intelligence: Complete payroll runs, pay periods, tax calculations, and compensation analysis
- Pay schedule management: Company pay schedules, frequency settings, and payroll timing configuration
- Company benefits: Benefits plans, enrollment options, and company benefit offerings
- Company insights: Company profile, locations, organizational structure, and compliance data
- Cross-Entity correlation: Integrated analysis across all Gusto data types for comprehensive workforce insights
- Streaming architecture: Memory-efficient yield-based data processing for large datasets
- Incremental syncs: Efficient data updates with checkpoint-based state management for time-based data
- Configurable filtering: Employee and time-based filtering for focused analysis
- Production-Grade error handling: Exponential backoff with jitter, rate limiting, and comprehensive retry logic
- Comprehensive testing: Mock testing framework with realistic data generation for development
- Flexible configuration: 15+ configuration parameters with validation and range checking

## Configuration file

PRODUCTION DEPLOYMENT GUIDANCE: For production deployments, ensure your API credentials are securely managed and never committed to version control. The `configuration.json` file should be excluded from your repository using `.gitignore`.

The connector requires the following configuration parameters:

```json
{
  "api_token": "<YOUR_GUSTO_API_TOKEN>",
  "company_id": "<YOUR_COMPANY_ID>",
  "initial_sync_days": "<YOUR_INITIAL_SYNC_DAYS>",
  "max_records_per_page": "<YOUR_MAX_RECORDS_PER_PAGE>",
  "request_timeout_seconds": "<YOUR_REQUEST_TIMEOUT_SECONDS>",
  "retry_attempts": "<YOUR_RETRY_ATTEMPTS>",
  "enable_employees": "<TRUE_OR_FALSE>",
  "enable_payrolls": "<TRUE_OR_FALSE>",
  "enable_pay_schedules": "<TRUE_OR_FALSE>",
  "enable_company_benefits": "<TRUE_OR_FALSE>",
  "enable_locations": "<TRUE_OR_FALSE>",
  "enable_debug_logging": "<TRUE_OR_FALSE>"
}
```

Note: Ensure that the configuration.json file is not checked into version control to protect sensitive information.

Required configuration keys:
- `api_token`: Gusto API access token for REST API access
- `company_id`: Gusto company ID for data extraction scope

Optional configuration keys:
- `initial_sync_days`: Days of historical data for initial sync (1-730, default: 90)
- `max_records_per_page`: Maximum records per API page (1-100, default: 50)
- `request_timeout_seconds`: API request timeout in seconds (5-300, default: 30)
- `retry_attempts`: Number of retry attempts for failed requests (1-10, default: 3)
- `enable_employees`: Enable employee data fetching (true/false, default: true)
- `enable_payrolls`: Enable payroll data fetching (true/false, default: true)
- `enable_pay_schedules`: Enable pay schedules data fetching (true/false, default: true)
- `enable_company_benefits`: Enable company benefits data fetching (true/false, default: true)
- `enable_locations`: Enable location data fetching (true/false, default: true)
- `enable_debug_logging`: Enable debug logging (true/false, default: false)

## Requirements file

The `requirements.txt` file specifies the Python libraries required by the connector for Gusto API integration and data processing.

Example content of `requirements.txt`:

```
requests>=2.28.0
faker>=18.0.0
```

Note: The `fivetran_connector_sdk:latest` package (v2.0.0+) is pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare it in your `requirements.txt`. This connector uses the updated SDK version with streaming architecture patterns.

## Authentication

The connector uses Gusto API tokens for authentication with the REST API. Authentication is provided through:

1. API tokens: Direct Gusto API access token in configuration.
2. Company scope: Company ID for data scope definition and access control.

Required API permissions:
- REST API access for querying company data
- Company-level access for the specified company ID
- Read permissions for employees, payroll, benefits, and time-off data

We recommend reviewing these permissions with your Gusto account administrator to ensure proper access levels and compliance with your organization's security policies.

Steps to obtain credentials:
1. Log into your Gusto account.
2. Navigate to **Settings > Integrations** or **Developer section**.
3. Create a new API application or access token with appropriate permissions.
4. Add credentials to `configuration.json` (remember not to commit this file).
5. Test connectivity using the provided mock framework before production deployment.

## Pagination

The connector handles data retrieval from Gusto REST API with efficient pagination and memory management:

- Employee API: Supports pagination for large employee datasets with configurable page sizes
- Payroll API: Handles pagination for historical payroll data with time-based filtering
- Pay Schedules API: Fetches company pay schedule configuration and timing settings
- Company Benefits API: Implements pagination for company benefits and plan offerings
- Streaming architecture: Uses Python generators (yield) for memory-efficient processing
- Incremental syncs: Implements time-based incremental syncs to reduce data transfer

The connector implements efficient data handling by:
- Processing API responses using streaming architecture with yield statements
- Using configurable page sizes (1-100 records) to optimize API performance
- Implementing incremental syncs based on last modification timestamps
- Applying filters to focus on relevant data subsets and active records

Refer to functions `get_employees_data`, `get_payrolls_data`, `get_pay_schedules_data`, `get_company_benefits_data`, and `get_locations_data` in `connector.py` for the data retrieval implementation.

## Dynamic time range management

The connector intelligently manages data fetching based on sync type and data freshness:

### Initial sync
- Time range: Last 90 days of historical data (configurable 1-730 days)
- Use case: First-time setup or full re-sync scenarios
- Data volume: Comprehensive historical data for baseline workforce analysis

### Incremental sync
- Time range: Data since last successful sync timestamp
- Use case: Regular scheduled syncs for time-sensitive data types
- Data volume: Only new/changed data since last sync
- Benefits: Faster sync times, reduced API calls, efficient resource usage

### Implementation details
- State management: Uses Fivetran's checkpoint system to track last sync timestamps
- Automatic detection: Automatically determines sync type based on existing state
- Flexible queries: Employee and payroll API calls dynamically adjust time ranges
- Entity-Specific logic: Different sync strategies for static (company, locations) vs dynamic (payroll, time-off) data
- Logging: Clear indication of sync type and time ranges in debug logs

## Data handling

The connector processes Gusto data through several stages with enterprise-grade architecture:

1. Data extraction: Direct REST API calls to Gusto endpoints with streaming architecture.
2. Data transformation: Conversion of Gusto API responses to structured table format using yield-based processing.
3. Schema mapping: Consistent data types and column naming across all tables with comprehensive field mapping.
4. State management: Checkpoint-based incremental sync support for time-sensitive data.
5. Error handling: Production-grade error handling with exponential backoff and comprehensive logging.

Data processing features:
- Type conversion: Gusto API responses converted to appropriate data types with validation
- Default values: Missing fields populated with appropriate defaults and null handling
- Timestamp handling: ISO format timestamp conversion and timezone-aware processing
- Data validation: Comprehensive validation of required fields and data integrity
- Filtering: Company and employee-based filtering based on configuration and active status
- Streaming architecture: Memory-efficient processing using Python generators for large datasets

Data Flow:
Gusto REST API → Requests Client → Dynamic Time Range Processing → Streaming Data Processing Functions → Data Mapping & Validation → Fivetran Operations → Data Warehouse Tables

Refer to functions `get_time_range`, `execute_api_request`, `__map_employee_data`, `__map_payroll_data`, and other mapping functions in `connector.py` for detailed data handling logic.

## Error handling

The connector implements comprehensive error handling strategies to ensure robust operation in production environments:

Configuration validation errors:
- ValueError: Missing required configuration values (`api_token`, `company_id`)
- ValueError: Invalid parameter ranges for numeric configuration values
- ValueError: Invalid boolean values for feature toggle parameters

API request errors:
- RequestException: Handles network timeouts, connection errors, and HTTP failures
- HTTPError: Manages 4xx and 5xx HTTP responses with specific handling for rate limiting (429)
- Timeout: Handles request timeouts with exponential backoff retry logic

Sync operation errors:
- RuntimeError: Handles general sync failures with detailed error messages and context
- Logging: Uses Fivetran's logging system with comprehensive error reporting and debug information

Error Handling Implementation:
- Early validation: Configuration parameters validated before any API calls
- Exponential backoff: Intelligent retry logic with jitter for transient failures
- Rate Limit handling: Automatic handling of HTTP 429 responses with Retry-After header compliance
- Exception propagation: Errors are logged with context and re-raised for Fivetran to handle
- State preservation: Checkpoint system maintains sync state across failures for resumable syncs
- Graceful degradation: Partial sync capability with comprehensive error reporting

Refer to functions `execute_api_request`, `__calculate_wait_time`, and the main `update` function in `connector.py` for error handling implementation.

## Tables created

The connector creates the following tables for comprehensive Gusto workforce data analysis:

### company
Company profile and organizational information for business context and compliance reporting.

| Column Name | Type | Description |
|-------------|------|-------------|
| `company_id` | STRING | Unique Gusto company identifier |
| `name` | STRING | Company legal name |
| `trade_name` | STRING | Company trade name or DBA |
| `ein` | STRING | Employer Identification Number |
| `entity_type` | STRING | Business entity type |
| `company_status` | STRING | Company status in Gusto |
| `locations_count` | INT | Number of company locations |
| `primary_signatory_first_name` | STRING | Primary signatory first name |
| `primary_signatory_last_name` | STRING | Primary signatory last name |
| `primary_signatory_email` | STRING | Primary signatory email address |
| `created_at` | STRING | Company creation timestamp |
| `timestamp` | STRING | Data extraction timestamp |

### employee
Employee records and job details for workforce analytics and HR reporting.

| Column Name | Type | Description |
|-------------|------|-------------|
| `employee_id` | STRING | Unique Gusto employee identifier |
| `company_id` | STRING | Company identifier |
| `first_name` | STRING | Employee first name |
| `last_name` | STRING | Employee last name |
| `email` | STRING | Employee email address |
| `employee_number` | STRING | Company employee number |
| `department` | STRING | Employee department |
| `job_title` | STRING | Employee job title |
| `hire_date` | STRING | Employee hire date |
| `termination_date` | STRING | Employee termination date |
| `employment_status` | STRING | Employment status |
| `pay_rate` | STRING | Employee pay rate |
| `pay_period` | STRING | Pay period frequency |
| `created_at` | STRING | Employee record creation timestamp |
| `updated_at` | STRING | Employee record last update timestamp |
| `timestamp` | STRING | Data extraction timestamp |

### payroll
Payroll runs and compensation data for payroll analytics and financial reporting.

| Column Name | Type | Description |
|-------------|------|-------------|
| `payroll_id` | STRING | Unique Gusto payroll identifier |
| `company_id` | STRING | Company identifier |
| `pay_period_start_date` | STRING | Pay period start date |
| `pay_period_end_date` | STRING | Pay period end date |
| `check_date` | STRING | Payroll check date |
| `processed` | BOOLEAN | Whether payroll is processed |
| `processed_date` | STRING | Payroll processing timestamp |
| `employees_count` | INT | Number of employees in payroll |
| `gross_pay` | STRING | Total gross pay amount |
| `net_pay` | STRING | Total net pay amount |
| `employer_taxes` | STRING | Total employer taxes |
| `employee_taxes` | STRING | Total employee taxes |
| `created_at` | STRING | Payroll creation timestamp |
| `timestamp` | STRING | Data extraction timestamp |

### pay_schedule
Company pay schedules and payroll timing configuration for payroll processing and compliance.

| Column Name | Type | Description |
|-------------|------|-------------|
| `pay_schedule_id` | STRING | Unique pay schedule identifier |
| `company_id` | STRING | Company identifier |
| `frequency` | STRING | Pay frequency (weekly, bi-weekly, monthly, etc.) |
| `name` | STRING | Payroll name (Engineering, Sales, Staff) |
| `custom_name` | STRING | Payroll name (Engineering department pay schedule, Sales dept monthly schedule, Staff pay schedule) |
| `anchor_pay_date` | STRING | Anchor pay date for schedule calculation |
| `anchor_end_of_pay_period` | STRING | Anchor end of pay period date |
| `day_1` | STRING | First pay day of the period |
| `day_2` | STRING | Second pay day of the period (if applicable) |
| `auto_pilot` | BOOLEAN | Whether payroll runs automatically |
| `active` | BOOLEAN | Whether payroll is active |
| `version` | STRING | Pay schedule version |
| `timestamp` | STRING | Data extraction timestamp |

### company_benefit
Company benefits plans and offerings for benefits administration and employee enrollment.

| Column Name | Type | Description |
|-------------|------|-------------|
| `company_benefit_id` | STRING | Unique company benefit identifier |
| `company_id` | STRING | Company identifier |
| `version` | STRING | Benefit plan version |
| `benefit_type` | STRING | Type of benefit (health, dental, 401k, etc.) |
| `description` | STRING | Benefit plan description |
| `active` | BOOLEAN | Whether benefit plan is active |
| `source` | STRING | The source of the company benefit (internal, external, partnered)|
| `partner_name` | STRING | Benefit plan partner |
| `deletable` | BOOLEAN | Whether benefit plan is deletable |
| `supports_percentage_amounts` | BOOLEAN | Whether benefit can be set as percentages |
| `responsible_for_employer_taxes` | BOOLEAN | Whether the employer is subject to pay employer taxes |
| `responsible_for_employee_w2` | BOOLEAN | Whether the employer is subject to file W-2 forms |
| `timestamp` | STRING | Data extraction timestamp |

### location
Company locations and address information for organizational structure and compliance reporting.

| Column Name | Type | Description |
|-------------|------|-------------|
| `location_id` | STRING | Unique location identifier |
| `company_id` | STRING | Company identifier |
| `street_1` | STRING | Street address line 1 |
| `street_2` | STRING | Street address line 2 |
| `city` | STRING | City name |
| `state` | STRING | State or province |
| `zip` | STRING | ZIP or postal code |
| `country` | STRING | Country code |
| `phone_number` | STRING | Location phone number |
| `active` | BOOLEAN | Whether location is active |
| `timestamp` | STRING | Data extraction timestamp |

## Additional files

The connector includes several additional files to support functionality, testing, and deployment:

* `requirements.txt` – Python dependency specification for Gusto API integration and connector requirements including faker for mock testing.

* `configuration.json` – Configuration template for API credentials and connector parameters (should be excluded from version control).

## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
