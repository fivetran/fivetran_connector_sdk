# Paylocity Connector Example

This connector demonstrates how to sync employee data, payroll information, and HR data from Paylocity's API using the Fivetran Connector SDK. The implementation features cognitive complexity optimization, memory-efficient streaming, and comprehensive OAuth2 authentication with realistic mock data testing.

## Connector overview

The Paylocity connector integrates with Paylocity's Human Capital Management (HCM) platform to extract employee demographics, payroll data, benefits information, and other HR-related data. It supports incremental synchronization, handles OAuth2 authentication with token refresh, and implements streaming data processing to efficiently handle large datasets without memory accumulation issues.

Key features include:
- OAuth2 Client Credentials authentication with automatic token management
- Memory-efficient streaming data processing using Python generators
- Comprehensive error handling with exponential backoff and retry logic
- Configurable pagination with nextToken-based traversal (20 records per page limit)
- Incremental sync support with timestamp-based cursors
- Cognitive complexity optimization with helper function extraction
- Production-ready rate limiting and timeout handling

## Requirements

- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

The connector implements several key features for robust data synchronization:

### Authentication management
OAuth2 Client Credentials flow with automatic token refresh (refer to `get_access_token` function). Supports both sandbox and production environments with environment-specific endpoints.

### Data extraction
Memory-efficient employee data fetching using streaming generators (refer to `get_employees` function). Includes comprehensive employee demographics, position information, compensation data, and contact details.

Optional payroll data extraction with configurable enable/disable flag (refer to `get_payroll_data` function). Processes pay periods, gross/net pay calculations, deductions, and tax information.

### Error handling
Comprehensive retry logic with exponential backoff for transient failures (refer to `execute_api_request` function). Specific handling for HTTP 429 rate limiting with Retry-After header support (refer to `__handle_rate_limit` function).

### Configuration validation
Automatic configuration validation with built-in type checking and intelligent defaults (refer to `__get_config_int` and `__get_config_bool` functions).

## Configuration file

```json
{
  "client_id": "<YOUR_PAYLOCITY_CLIENT_ID>",
  "client_secret": "<YOUR_PAYLOCITY_CLIENT_SECRET>",
  "company_id": "<YOUR_PAYLOCITY_COMPANY_ID>",
  "use_sandbox": "<YOUR_PAYLOCITY_SANDBOX_FLAG>",
  "initial_sync_days": "<YOUR_PAYLOCITY_INITIAL_SYNC_DAYS>",
  "max_records_per_page": "<YOUR_PAYLOCITY_MAX_RECORDS_PER_PAGE>",
  "request_timeout_seconds": "<YOUR_PAYLOCITY_REQUEST_TIMEOUT_SECONDS>",
  "retry_attempts": "<YOUR_PAYLOCITY_RETRY_ATTEMPTS>",
  "enable_payroll": "<YOUR_PAYLOCITY_ENABLE_PAYROLL_FLAG>",
  "active_employees_only": "<YOUR_PAYLOCITY_ACTIVE_EMPLOYEES_ONLY_FLAG>"
}
```

### Configuration parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `client_id` | string | Yes | - | Paylocity OAuth2 client identifier |
| `client_secret` | string | Yes | - | Paylocity OAuth2 client secret |
| `company_id` | string | Yes | - | Paylocity company identifier |
| `use_sandbox` | boolean | No | true | Use sandbox environment for testing |
| `initial_sync_days` | integer | No | 90 | Days of historical data for initial sync |
| `max_records_per_page` | integer | No | 20 | Records per API request (Paylocity limit: 20) |
| `request_timeout_seconds` | integer | No | 30 | HTTP request timeout in seconds |
| `retry_attempts` | integer | No | 3 | Number of retry attempts for failed requests |
| `enable_payroll` | boolean | No | false | Enable payroll data extraction |
| `active_employees_only` | boolean | No | false | Sync only active employees |

## Requirements file

This connector does not require any additional packages beyond those provided by the Fivetran environment.

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication

To connect to Paylocity, you need OAuth2 credentials from your Paylocity account:

1. Access developer portal: Log in to the Paylocity Developer Portal at https://developer.paylocity.com.
2. Create application: Register a new application to obtain client credentials.
3. Obtain credentials: Copy the `client_id` and `client_secret` from your application settings.
4. Get company id: Retrieve your company identifier from Paylocity administrators.
5. Environment selection: Use sandbox credentials for testing, production credentials for live sync.

Security note: The connector automatically handles OAuth2 token refresh (1-hour expiration). Credentials are never logged or exposed in plain text.

## Pagination

Paylocity uses nextToken-based pagination with a maximum of 20 records per request. The connector implements streaming pagination to process large datasets efficiently:

Strategy: Token-based pagination with automatic page traversal (refer to `get_employees` function)
Implementation: Generator-based processing prevents memory accumulation for large employee datasets
Performance: Processes pages sequentially while yielding individual records for immediate processing

Example pagination flow:
1. Request first page with `limit=20`.
2. Process employees and yield individual records.
3. Use `nextToken` from response for subsequent page.
4. Continue until `nextToken` is null or empty.

## Data handling

### Memory efficiency
The connector uses Python generators throughout the data pipeline to prevent memory accumulation (refer to data fetching functions). This approach allows processing of large employee datasets without memory overflow issues.

Pattern: `for employee in get_employees(): op.upsert()` - immediate processing
Anti-pattern: `employees = get_all_employees()` - memory accumulation

### Field mapping
Employee data is mapped from Paylocity's API format to normalized database columns (refer to `__map_employee_data` function). Nested objects are flattened, and all timestamps are converted to UTC format for consistency.

### Incremental sync
Supports timestamp-based incremental synchronization using the `last_sync_time` state parameter (refer to `get_time_range` function). Initial sync can be configured to fetch historical data up to 365 days.

## Error handling

The connector implements comprehensive error handling across multiple categories:

### HTTP errors
- 401 Unauthorized: Invalid or expired OAuth2 credentials
- 403 Forbidden: Insufficient API permissions
- 404 Not Found: Invalid endpoints or company ID
- 429 Rate Limited: Automatic retry with Retry-After header support (refer to `__handle_rate_limit` function)
- 500 Server Error: Automatic retry with exponential backoff

### Network errors
Timeout handling with configurable retry attempts (refer to `__handle_request_error` function). Exponential backoff with jitter prevents thundering herd scenarios.

### Configuration errors
Parameter validation with descriptive error messages (refer to `validate_configuration` function). Range checking for numeric parameters prevents invalid API requests.

## Tables created

The connector creates the following tables in your destination:

### employees
Primary Key: `["company_id", "employee_id"]`

Sample columns include: company_id, employee_id, first_name, last_name, email, department, position, hire_date, pay_rate, employee_status, and additional demographic and contact information. Column types are automatically inferred by Fivetran.

### payroll
Primary Key: `["company_id", "employee_id", "pay_period_start"]`

Sample columns include: company_id, employee_id, pay_period_start, pay_period_end, gross_pay, net_pay, total_deductions, total_taxes, regular_hours, overtime_hours, payroll_run_id. Column types are automatically inferred by Fivetran.

Note: Column types are automatically inferred by Fivetran core based on the data types in the API responses. This approach reduces maintenance overhead and automatically adapts to API changes.

## Additional files

### Testing framework
- `test_connector.py`: Comprehensive unit test suite using faker library for realistic mock data generation
- `faker_mock/mock_data_generator.py`: Faker-based data generator creating realistic employee and payroll data with proper relationships
- `debug_connector.py`: Interactive debugging framework with multiple test scenarios including rate limiting, performance testing, and data quality analysis

### Deployment
- `.github/workflows/deploy_connector.yaml`: Automated GitHub Actions deployment pipeline with secret management and environment configuration
- `requirements.txt`: Python dependencies specification for testing and development

### Configuration
- `configuration.json`: Sample configuration file with OAuth2 parameters and feature flags
- Template follows Fivetran Connector SDK standards for production deployment

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.