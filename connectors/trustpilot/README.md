# Trustpilot API Connector Example

This connector provides comprehensive Trustpilot review and business data synchronization into your data warehouse for customer feedback analysis, reputation management, and business insights. It leverages Trustpilot's REST API to extract reviews, business information, categories, consumer reviews, and invitation links data for comprehensive customer experience analysis.

## Connector overview

The Trustpilot API Connector is a Fivetran Connector SDK implementation that extracts comprehensive review and business data from Trustpilot's various API endpoints. It provides detailed insights into customer feedback, business reputation, review management, and customer engagement across multiple Trustpilot services.

Data source: Trustpilot REST API (v1) for reviews, business units, categories, consumer reviews, and invitation links

Use cases: Customer feedback analysis, reputation management, review monitoring, customer experience optimization, business intelligence, and comprehensive customer insights.

## Requirements

- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)   
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

- Comprehensive reviews data: Customer feedback, ratings, review text, verification status, and helpfulness metrics
- Business intelligence: Business unit information, trust scores, review counts, and reputation metrics
- Category management: Business categorization, hierarchical structure, and localized naming
- Consumer reviews API: Individual consumer review history, verification status, and engagement tracking
- Invitation management: Review invitation tracking, response rates, and campaign effectiveness
- Cross-service correlation: Integrated analysis across all Trustpilot services for comprehensive insights
- Dynamic time ranges: Intelligent data fetching based on sync type (initial vs incremental)
- Incremental syncs: Efficient data updates with checkpoint-based state management
- Configurable filtering: Business unit and time-based filtering for focused analysis
- Error handling: Robust error handling with comprehensive logging and validation

## Configuration file

```json
{
  "api_key": "<YOUR_TRUSTPILOT_API_KEY>",
  "business_unit_id": "<YOUR_BUSINESS_UNIT_ID>",
  "consumer_id": "<YOUR_CONSUMER_ID>",
  "initial_sync_days": "<YOUR_INITIAL_SYNC_DAYS>",
  "max_records_per_page": "<YOUR_MAX_RECORDS_PER_PAGE>",
  "request_timeout_seconds": "<YOUR_REQUEST_TIMEOUT_SECONDS>",
  "retry_attempts": "<YOUR_RETRY_ATTEMPTS>",
  "enable_incremental_sync": "<YOUR_ENABLE_INCREMENTAL_SYNC>",
  "enable_consumer_reviews": "<YOUR_ENABLE_CONSUMER_REVIEWS>",
  "enable_invitation_links": "<YOUR_ENABLE_INVITATION_LINKS>",
  "enable_categories": "<YOUR_ENABLE_CATEGORIES>",
  "data_retention_days": "<YOUR_DATA_RETENTION_DAYS>",
  "enable_debug_logging": "<YOUR_ENABLE_DEBUG_LOGGING>"
}
```

Required configuration keys:
- `api_key`: Trustpilot API key for REST API access
- `business_unit_id`: Trustpilot business unit ID for data extraction

Optional configuration keys:
- `consumer_id`: Trustpilot consumer ID for fetching individual consumer reviews (required only when `enable_consumer_reviews` is true)
- `initial_sync_days`: Days of historical data for initial sync (1-365, default: 90)
- `max_records_per_page`: Maximum records per API page (1-100, default: 100)
- `request_timeout_seconds`: API request timeout in seconds (default: 30)
- `retry_attempts`: Number of retry attempts for failed requests (default: 3)
- `enable_incremental_sync`: Enable incremental sync (true/false, default: true)
- `enable_consumer_reviews`: Enable consumer reviews data fetching (true/false, default: false)
- `enable_invitation_links`: Enable invitation links data fetching (true/false, default: true)
- `enable_categories`: Enable categories data fetching (true/false, default: true)
- `data_retention_days`: Data retention period in days (default: 730)
- `enable_debug_logging`: Enable debug logging (true/false, default: false)

## Requirements file

This connector does not require any additional packages beyond those provided by the Fivetran environment.

Note: The fivetran_connector_sdk:latest and requests:latest packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your requirements.txt.

## Authentication

The connector uses Trustpilot API keys for authentication with the REST API. Authentication is provided through:

1. API keys: Direct Trustpilot API key in configuration.
2. Business unit access: Business unit ID for data scope definition.

Required API Permissions:
- REST API access for querying business data
- Business unit-level access for the specified business unit ID
- Read permissions for reviews, consumers, and invitations

We recommend reviewing these permissions with your Trustpilot account administrator to ensure proper access levels.

Steps to obtain credentials:
1. Go to your Trustpilot Business account.
2. Navigate to Settings > API Keys.
3. Create a new API key with appropriate permissions.
4. For production: We recommend adding credentials as **GitHub Secrets** (see [Deployment](#deployment) section).
5. For development: You can add credentials to `configuration.json` (remember not to commit this file).

## Pagination

The connector handles data retrieval from Trustpilot REST API with efficient pagination:

- Reviews API: Limited to 100 results per page with time-based filtering
- Consumers API: Supports pagination for large result sets
- Invitations API: Implements pagination for invitation data
- Data volume management: Uses appropriate time ranges to manage data volume
- Incremental syncs: Implements incremental syncs to reduce data transfer

The connector implements efficient data handling by:
- Streaming paginated API responses and processing records incrementally
- Using appropriate time ranges to manage data volume
- Implementing incremental syncs to reduce data transfer
- Applying filters to focus on relevant data subsets

Refer to functions `get_reviews_data`, `get_business_data`, `get_categories_data`, `get_consumers_data`, and `get_invitations_data` in `connector.py` for the data retrieval implementation.

## Data handling

The connector processes Trustpilot data through several stages:

1. Data extraction: Direct REST API calls to Trustpilot endpoints.
2. Data transformation: Conversion of Trustpilot API responses to structured table format.
3. Schema mapping: Consistent data types and column naming across all tables.
4. State management: Checkpoint-based incremental sync support.
5. Error handling: Comprehensive error handling with logging and validation.

Data Processing Features:
- Type conversion: Trustpilot API responses converted to appropriate data types
- Default values: Missing dimensions populated with appropriate defaults
- Timestamp handling: ISO format timestamp conversion and period-based grouping
- Data aggregation: Multiple metric aggregations (counts, averages, percentages)
- Filtering: Business unit and time-based filtering based on configuration
- Dynamic time ranges: Intelligent time range selection for initial vs incremental syncs

Data Flow:
Trustpilot REST API → Requests Client → Dynamic Time Range Processing → Data Processing Functions → Fivetran Operations → Data Warehouse Tables

Refer to functions `get_time_range`, `execute_api_request`, `get_reviews_data`, `get_business_data`, `get_categories_data`, `get_consumers_data`, and `get_invitations_data` in `connector.py` for detailed data handling logic.

## Error handling

The connector implements comprehensive error handling strategies to ensure robust operation:

Configuration validation errors:
- ValueError: Missing required configuration values (`api_key`, `business_unit_id`, `consumer_id`)
- ValueError: Empty API key, business unit ID, or consumer ID values

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

The connector creates the following tables for comprehensive Trustpilot data analysis. Column types are automatically inferred by Fivetran based on the actual data structure and content.

### REVIEW
Primary table for customer reviews and feedback data with comprehensive review metadata.

Primary key: `review_id`, `business_unit_id`

Sample columns (automatically inferred by Fivetran):
- `review_id` - Unique Trustpilot review identifier
- `business_unit_id` - Business unit identifier
- `consumer_id` - Customer identifier
- `consumer_name` - Customer display name
- `stars` - Review rating (1-5 stars)
- `title` - Review title
- `text` - Review content text
- `language` - Review language
- `created_at` - Review creation timestamp
- `updated_at` - Review last update timestamp
- `status` - Review status
- `is_verified` - Whether review is verified
- `helpful_count` - Number of helpful votes
- `reply_text` - Business reply text
- `reply_created_at` - Reply creation timestamp
- `timestamp` - Data extraction timestamp

### BUSINESS_UNIT
Business unit information and reputation metrics for comprehensive business analysis.

Primary key: `business_unit_id`

Sample columns (automatically inferred by Fivetran):
- `business_unit_id` - Unique business unit identifier
- `name` - Business name
- `display_name` - Business display name
- `website_url` - Business website URL
- `country_code` - Business country code
- `language` - Business language
- `number_of_reviews` - Total number of reviews
- `trust_score` - Trustpilot trust score
- `stars` - Average star rating
- `created_at` - Business creation timestamp
- `updated_at` - Business last update timestamp
- `timestamp` - Data extraction timestamp

### CATEGORY
Business categories and hierarchical classification for business categorization analysis.

Primary key: `category_id`

Sample columns (automatically inferred by Fivetran):
- `category_id` - Unique category identifier
- `name` - Category name
- `localized_name` - Localized category name
- `parent_id` - Parent category identifier
- `level` - Category hierarchy level
- `created_at` - Category creation timestamp
- `updated_at` - Category last update timestamp
- `timestamp` - Data extraction timestamp

### CONSUMER_REVIEW
Individual consumer review history and engagement data for customer analysis and segmentation.

Primary key: `consumer_id`, `review_id`

Sample columns (automatically inferred by Fivetran):
- `consumer_id` - Unique customer identifier
- `review_id` - Unique review identifier
- `business_unit_id` - Business unit identifier
- `business_unit_name` - Business unit display name
- `stars` - Review rating (1-5 stars)
- `title` - Review title
- `text` - Review content text
- `language` - Review language
- `status` - Review status
- `is_verified` - Whether review is verified
- `number_of_likes` - Number of helpful votes
- `created_at` - Review creation timestamp
- `updated_at` - Review last update timestamp
- `experienced_at` - When the experience occurred
- `review_verification_level` - Level of review verification
- `counts_towards_trust_score` - Whether review counts towards trust score
- `counts_towards_location_trust_score` - Whether review counts towards location trust score
- `company_reply_text` - Business reply text
- `company_reply_created_at` - Reply creation timestamp
- `company_reply_updated_at` - Reply last update timestamp
- `location_id` - Location identifier
- `location_name` - Location name
- `timestamp` - Data extraction timestamp

### INVITATION_LINK
Review invitation management and response tracking for campaign effectiveness analysis.

Primary key: `invitation_id`, `business_unit_id`

Sample columns (automatically inferred by Fivetran):
- `invitation_id` - Unique invitation identifier
- `business_unit_id` - Business unit identifier
- `consumer_id` - Customer identifier
- `consumer_email` - Customer email address
- `status` - Invitation status
- `type` - Invitation type
- `created_at` - Invitation creation timestamp
- `updated_at` - Invitation last update timestamp
- `sent_at` - Invitation sent timestamp
- `responded_at` - Customer response timestamp
- `timestamp` - Data extraction timestamp

## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
