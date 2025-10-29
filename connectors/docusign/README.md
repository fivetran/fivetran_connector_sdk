# DocuSign Connector Example

This connector syncs data from DocuSign eSign REST API to your data warehouse, including envelopes, recipients, documents, and templates. The connector handles OAuth2 authentication, automatic pagination, incremental syncing, and memory-efficient processing for large datasets.

## Connector overview

The DocuSign connector fetches electronic signature data from your DocuSign account using the eSign REST API. It extracts envelope metadata, recipient information, document details, and template definitions. The connector supports both initial full syncs and incremental updates based on modification timestamps. Data is processed using memory-efficient streaming patterns to handle large volumes without resource exhaustion.

Key capabilities include:
- Envelope management: Complete envelope lifecycle tracking including status changes, timestamps, and custom fields
- Recipient tracking: Detailed recipient information with signing status, delivery confirmations, and custom tabs
- Document metadata: Document properties, signatures locations, and download references
- Template synchronization: Template definitions, sharing settings, and folder organization
- Incremental dync: Timestamp-based incremental updates to minimize API usage and sync time
- Memory efficiency: Generator-based processing prevents memory accumulation with large datasets

## Requirements

- Python: 3.9, 3.10, 3.11, or 3.12
- Operating System: Windows 10+, macOS 13+, Linux distributions like Ubuntu 20.04+
- DocuSign Account: Valid DocuSign account with API access
- OAuth2 Credentials: Access token with appropriate permissions for envelope and template access

## Getting started

Follow the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk#setupguide) to install Fivetran CLI and initialize your environment.

## Features

### Core data entities
- Envelopes: Retrieved via `get_envelopes` function with comprehensive metadata and status tracking
- Recipients: Processed with `__map_recipient_data` function covering all recipient types (signers, carbon copies, etc.)
- Documents: Handled through `__map_document_data` function with signature location tracking
- Templates: Synchronized using `get_templates` function for template management

### Authentication & Security
The connector uses OAuth2 Bearer token authentication as implemented in the `execute_api_request` function. Access tokens are passed securely through configuration and validated on each API call.

### Pagination strategy
DocuSign API uses offset-based pagination with `start_position` and `count` parameters. The connector implements automatic pagination in both `get_envelopes` and `get_templates` functions, processing pages sequentially until all data is retrieved.

### Error handling & Reliability
Comprehensive error handling is implemented through specialized helper functions:
- Rate limiting: `__handle_rate_limit` function manages HTTP 429 responses with exponential backoff
- Request errors: `__handle_request_error` function provides retry logic with configurable attempts
- Authentication: Automatic detection and clear messaging for 401 unauthorized responses

## Configuration file

```json
{
  "access_token": "YOUR_DOCUSIGN_ACCESS_TOKEN",
  "account_id": "YOUR_DOCUSIGN_ACCOUNT_ID",
  "sync_frequency_hours": "YOUR_SYNC_FREQUENCY_HOURS",
  "initial_sync_days": "YOUR_INITIAL_SYNC_DAYS",
  "max_records_per_page": "YOUR_MAX_RECORDS_PER_PAGE",
  "request_timeout_seconds": "YOUR_REQUEST_TIMEOUT_SECONDS",
  "retry_attempts": "YOUR_RETRY_ATTEMPTS",
  "enable_templates": "YOUR_ENABLE_TEMPLATES",
  "enable_incremental_sync": "YOUR_ENABLE_INCREMENTAL_SYNC",
  "enable_debug_logging": "YOUR_ENABLE_DEBUG_LOGGING"
}
```

### Configuration Parameters

- `access_token` (required): OAuth2 access token for DocuSign API authentication
- `account_id` (required): DocuSign account ID for API requests
- `base_url` (required): DocuSign API base URL (demo or production environment)
- `sync_frequency_hours`: How often to sync data (1-24 hours, default: 4)
- `initial_sync_days`: Days of historical data for initial sync (1-365, default: 90)
- `max_records_per_page`: Records per API call (10-1000, default: 100)
- `request_timeout_seconds`: API request timeout (5-300 seconds, default: 30)
- `retry_attempts`: Number of retry attempts for failed requests (1-10, default: 3)
- `enable_templates`: Whether to sync template data (true/false, default: true)
- `enable_incremental_sync`: Enable timestamp-based incremental syncing (true/false, default: true)
- `enable_debug_logging`: Enable detailed logging for troubleshooting (true/false, default: false)

## Requirements file

The `requirements.txt` file specifies the Python libraries required by the connector for Gusto API integration and data processing.

Example content of `requirements.txt`:

```
requests>=2.28.0
faker>=18.0.0
```

Note: The `fivetran_connector_sdk:latest` package (v2.0.0+) is pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare it in your `requirements.txt`. This connector uses the updated SDK version with streaming architecture patterns.

## Authentication

### OAuth2 setup

1. Create DocuSign developer account: Register at [DocuSign Developer Center](https://developers.docusign.com)
2. Create integration key: Generate an integration key in the DocuSign Admin console
3. Configure OAuth2: Set up OAuth2 application with appropriate scopes
4. Generate access token: Use OAuth2 flow to obtain access token with required permissions
5. Obtain account id: Retrieve your DocuSign account ID from the API or admin console

### Required permissions
- `signature` scope for envelope access
- `extended` scope for advanced envelope features
- `impersonation` scope for account-level operations

### Token management
Access tokens typically expire after 8 hours. Implement token refresh logic in production deployments or use long-lived JWT grants for service accounts.

## Pagination

The connector implements DocuSign's offset-based pagination using `start_position` and `count` parameters. Pagination logic is handled in:

- Envelope pagination: `get_envelopes` function automatically iterates through pages until no more data
- Template pagination: `get_templates` function processes template pages with the same logic
- Memory efficiency: Pages are processed individually without accumulating all results in memory

Pagination parameters are automatically calculated and the connector stops when receiving empty result sets or when the returned count is less than the requested page size.

## Data handling

### Memory-Efficient processing
The connector uses generator-based streaming to handle large datasets efficiently:

- No memory accumulation: Data is yielded immediately rather than collected in lists
- Individual record processing: Each record is processed and upserted immediately via generator patterns
- Streaming architecture: Functions like `get_envelopes` and `get_templates` use `yield` statements

### Field mapping and Transformation
Data transformation is handled through dedicated mapping functions:

- Envelope mapping: `__map_envelope_data` function standardizes envelope fields and handles nested objects
- Recipient mapping: `__map_recipient_data` function processes all recipient types with proper relationships
- Document mapping: `__map_document_data` function extracts document metadata and signature information
- Template mapping: `__map_template_data` function handles template properties and ownership data

### Incremental sync strategy
Incremental synchronization uses timestamp-based filtering implemented in the `get_time_range` function:

- Last sync tracking: State dictionary maintains `last_sync_time` and `last_template_sync` timestamps
- Date range filtering: API requests include `from_date` and `to_date` parameters for envelopes
- Modified date filtering: Templates use `modified_from_date` parameter for incremental updates
- UTC consistency: All timestamps use UTC timezone for consistency across environments

## Error handling

### Retry logic and Rate limiting
The connector implements comprehensive error handling with specialized functions:

- Exponential backoff: `__calculate_wait_time` function provides jitter-based delays for retries
- Rate limit management: `__handle_rate_limit` function specifically handles HTTP 429 responses
- Request error recovery: `__handle_request_error` function manages various request failures

### Error categories and Responses
- Authentication errors (401): Clear messaging and immediate failure for invalid credentials
- Rate limiting (429): Automatic retry with respect to `retry-after` headers
- Network errors: Configurable retry attempts with exponential backoff
- API errors (4xx/5xx): Appropriate error propagation with context information

### Timeout and Connection Management
- Request timeouts: Configurable timeout values prevent hanging requests
- Connection reuse: Efficient HTTP connection management through requests library
- Graceful failures: Proper exception handling and cleanup on failures

## Tables created

The connector creates four main tables with automatic column type inference:

### envelopes
Primary key: `envelope_id`

Column types are automatically inferred by Fivetran based on data content. Sample columns include:
- envelope_id, account_id, subject, status, created_date_time, sent_date_time, completed_date_time, sender_email, sender_name, custom_fields, notification, email_settings, synced_at

### recipients
Primary key: `recipient_id`, `envelope_id`

Column types are automatically inferred by Fivetran. Sample columns include:
- recipient_id, envelope_id, account_id, recipient_type, email, name, status, routing_order, signed_date_time, delivered_date_time, tabs, custom_fields, synced_at

### documents
Primary key: `document_id`, `envelope_id`

Column types are automatically inferred by Fivetran. Sample columns include:
- document_id, envelope_id, account_id, name, type, uri, order, pages, display, signature_locations, synced_at

### templates
Primary key: `template_id`

Column types are automatically inferred by Fivetran. Sample columns include:
- template_id, account_id, name, description, shared, uri, created, last_modified, last_modified_by, owner, folder_id, synced_at

## Additional files

### Testing Framework
- `test_connector.py`: Comprehensive unit tests using faker-generated realistic mock data
- `faker_mock/mock_data_generator.py`: DocuSignMockGenerator class for creating test data with proper relationships
- `debug_connector.py`: Interactive debugging script with multiple test scenarios including rate limiting, authentication errors, and performance testing

### Development tools
- `requirements.txt`: Minimal dependencies focusing on testing libraries
- `configuration.json`: Template configuration file with all available parameters
- `faker_mock/`: Directory containing realistic mock data generation for testing

### Testing capabilities
The faker-based testing framework provides:
- Realistic data generation: Proper envelope/recipient/document relationships
- Scenario testing: Rate limiting, authentication errors, pagination edge cases
- Performance validation: Memory usage testing with large datasets
- Data quality analysis: Verification of data consistency and relationships

## Additional considerations

The connector follows Fivetran Connector SDK best practices for production deployment. Key implementation features include cognitive complexity optimization with all functions maintaining complexity ratings below 15, memory-efficient streaming architecture preventing out-of-memory issues with large datasets, and comprehensive error handling with modular helper functions.

Production deployment requires proper OAuth2 token management including refresh logic for long-running operations. The connector supports both DocuSign demo and production environments through configurable base URLs. For high-volume accounts, consider adjusting pagination size and implementing additional rate limiting respect.

Monitor sync performance and adjust configuration parameters based on your DocuSign account's API limits and data volume. The connector's streaming architecture scales effectively for accounts with thousands of envelopes and documents.