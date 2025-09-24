# Trustpilot API Connector

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

### Quick start (Recommended)

1. Fork or clone this repository.
2. Set up GitHub secrets and variables (see [Deployment](#deployment) section).
3. Push changes to `main` branch - deployment happens automatically!

### Alternative: Manual setup

For local development and testing, refer to the [Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) and the [Manual Deployment](#manualdeployment) section below.

## Deployment

> PRODUCTION DEPLOYMENT RECOMMENDATION
> We recommend using GitHub Actions workflow for all production deployments.
> Manual deployment is available for development, testing, and debugging purposes, but we encourage using the automated workflow for production environments.

The connector is designed to work with **GitHub Actions workflow** as the recommended production deployment mechanism, while keeping manual deployment available for development and testing scenarios.

### Primary: Automated deployment via GitHub Actions

The connector includes a GitHub Actions workflow (`.github/workflows/deploy_connector.yaml`) for automated deployment.

#### Workflow features:
- Automated Triggers: Deploys on pushes to the `main` branch
- Path-Based Triggers: Only runs when changes are made to the connector directory
- Parameterized Configuration: Easy to customize Python version, working directory, and other settings
- Environment Management: Uses GitHub Environments for secure credential management
- Zero-Downtime Deployment: Seamless updates without service interruption

#### Quick setup:

1. Create GitHub environment:
   - i. Go to repository **Settings → Environments**
   - ii. Create environment named `Fivetran`

2. Add repository secrets (**Settings → Secrets** and **Variables → Actions**):
   ```
   TRUSTPILOT_API_KEY          # Your Trustpilot API key
   TRUSTPILOT_BUSINESS_UNIT_ID # Your Trustpilot business unit ID
   TRUSTPILOT_CONSUMER_ID      # Your Trustpilot consumer ID
   FIVETRAN_API_KEY            # Your Fivetran API key
   ```

3. Add repository variables (**Settings → Secrets** and **Variables → Actions**):
   ```
   FIVETRAN_DEV_DESTINATION    # Your Fivetran destination ID
   TRUSTPILOT_DEV              # Your Fivetran connection name
   ```

4. Deploy: Simply push changes to the `main` branch - deployment happens automatically!

#### Workflow configuration:

The workflow uses parameterized environment variables for easy customization:

```yaml
env:
  PYTHON_VERSION: '3.11'
  WORKING_DIRECTORY: 'examples/source_examples/trustpilot'
  CONNECTOR_NAME: 'Trustpilot'
  CONFIG_FILE: 'configuration.json'
  EXCLUDED_DEPENDENCIES: '^requests\b'
```

#### Deployment process:

1. Automatic trigger: Push changes to `main` branch.
2. Environment setup: Python 3.11, dependencies installation.
3. Configuration creation: Generates `configuration.json` from GitHub secrets.
4. Fivetran deployment: Executes `fivetran deploy` command.
5. Status reporting: Provides deployment success/failure feedback.

#### Customization:

To adapt this workflow for other connectors, simply update the environment variables:

```yaml
env:
  PYTHON_VERSION: '3.11'
  WORKING_DIRECTORY: 'examples/source_examples/trustpilot'
  CONNECTOR_NAME: 'Other'
  CONFIG_FILE: 'configuration.json'
  EXCLUDED_DEPENDENCIES: '^requests\b'
```

### <a name="manualdeployment"></a>Secondary: Manual Deployment (Debugging & Testing)

For local development, testing, and debugging purposes:

1. Install dependencies:
   ```bash
   pip install fivetran-connector-sdk
   ```

2. Configure credentials:
   - Update `configuration.json` with your Trustpilot API credentials
   - Ensure the file is not committed to version control

3. Deploy to Fivetran:
   ```bash
   fivetran deploy --api-key YOUR_FIVETRAN_API_KEY --destination YOUR_DESTINATION_ID --connection YOUR_CONNECTION_NAME --configuration configuration.json --python-version 3.11
   ```

## Features

* Comprehensive reviews data: Customer feedback, ratings, review text, verification status, and helpfulness metrics
* Business intelligence: Business unit information, trust scores, review counts, and reputation metrics
* Category management: Business categorization, hierarchical structure, and localized naming
* Consumer reviews API: Individual consumer review history, verification status, and engagement tracking
* Invitation management: Review invitation tracking, response rates, and campaign effectiveness
* Cross-service correlation: Integrated analysis across all Trustpilot services for comprehensive insights
* Dynamic time ranges: Intelligent data fetching based on sync type (initial vs incremental)
* Incremental syncs: Efficient data updates with checkpoint-based state management
* Configurable filtering: Business unit and time-based filtering for focused analysis
* Error handling: Robust error handling with comprehensive logging and validation
* CI/CD integration: Automated deployment via GitHub Actions with parameterized configuration
* Environment management: Secure credential management using GitHub Environments
* Zero-downtime deployment: Seamless updates without service interruption

## Configuration file

PRODUCTION DEPLOYMENT GUIDANCE: For production deployments, we recommend using **GitHub secrets** configured in the GitHub Actions workflow. Local configuration files can be used for development and testing, but we encourage using GitHub secrets for production environments.

The connector requires the following configuration keys. For production deployments, we recommend configuring these as **GitHub secrets** (see [Deployment](#deployment) section). For local development and testing, you can define them in `configuration.json`:

```json
{
  "api_key": "YOUR_TRUSTPILOT_API_KEY",
  "business_unit_id": "YOUR_BUSINESS_UNIT_ID",
  "consumer_id": "YOUR_CONSUMER_ID",
  "sync_frequency_hours": "4",
  "initial_sync_days": "90",
  "max_records_per_page": "100",
  "request_timeout_seconds": "30",
  "retry_attempts": "3",
  "enable_incremental_sync": "true",
  "enable_consumer_reviews": "true",
  "enable_invitation_links": "true",
  "enable_categories": "true",
  "data_retention_days": "730",
  "enable_debug_logging": "false"
}
```

Required configuration keys:
- `api_key`: Trustpilot API key for REST API access
- `business_unit_id`: Trustpilot business unit ID for data extraction
- `consumer_id`: Trustpilot consumer ID for fetching individual consumer reviews

Optional configuration keys:
- `sync_frequency_hours`: Sync frequency in hours (1-24, default: 4)
- `initial_sync_days`: Days of historical data for initial sync (1-365, default: 90)
- `max_records_per_page`: Maximum records per API page (1-100, default: 100)
- `request_timeout_seconds`: API request timeout in seconds (default: 30)
- `retry_attempts`: Number of retry attempts for failed requests (default: 3)
- `enable_incremental_sync`: Enable incremental sync (true/false, default: true)
- `enable_consumer_reviews`: Enable consumer reviews data fetching (true/false, default: true)
- `enable_invitation_links`: Enable invitation links data fetching (true/false, default: true)
- `enable_categories`: Enable categories data fetching (true/false, default: true)
- `data_retention_days`: Data retention period in days (default: 730)
- `enable_debug_logging`: Enable debug logging (true/false, default: false)

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

API key setup:
1. Go to your Trustpilot Business account.
2. Navigate to **Settings > API Keys**.
3. Create a new API key with appropriate permissions.
4. Add the API key to `configuration.json`.

Business unit ID:
- Found in your Trustpilot Business dashboard
- Used to scope data extraction to specific business units

Consumer ID:
- Found in consumer profile URLs or API responses
- Used to fetch individual consumer review history

Note: We recommend keeping the `configuration.json` file out of version control to protect sensitive information. You can add it to your `.gitignore` file for additional security.

### Development configuration (Local Testing)

Development setup: The following configuration methods are designed for local development and testing. We recommend using GitHub secrets for production deployments.

For local development and testing:

```bash
# Option 1: Set environment variables locally
export TRUSTPILOT_API_KEY="your_api_key"
export TRUSTPILOT_BUSINESS_UNIT_ID="your_business_unit_id"
export TRUSTPILOT_CONSUMER_ID="your_consumer_id"

# Option 2: Use configuration.json file (never commit this file)
# Update configuration.json with your test credentials
```

Production deployment: We recommend using the GitHub Actions workflow with GitHub secrets (see [Deployment](#deployment) section above).

## Requirements file

The `requirements.txt` file specifies the Python libraries required by the connector for Trustpilot API integration and data processing.

Example content of `requirements.txt`:

```
requests>=2.28.0
```

Note: The `fivetran_connector_sdk:latest` package (v2.0.0+) is pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare it in your `requirements.txt`. This connector uses the updated SDK version that does not require `yield` statements for operations.

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
- Processing complete API responses in memory
- Using appropriate time ranges to manage data volume
- Implementing incremental syncs to reduce data transfer
- Applying filters to focus on relevant data subsets

Refer to functions `get_reviews_data`, `get_business_data`, `get_categories_data`, `get_consumers_data`, and `get_invitations_data` in `connector.py` for the data retrieval implementation.

## Dynamic Time Range Management

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
- Dynamic rime ranges: Intelligent time range selection for initial vs incremental syncs

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

### review
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

### business_unit
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

### category
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

### consumer_review
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

### invitation_link
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

## Additional files

The connector includes several additional files to support functionality and deployment:

* `.github/workflows/deploy_connector.yaml` – Primary deployment mechanism: GitHub Actions workflow for automated deployment to Fivetran with parameterized configuration and secure credential management.

* `requirements.txt` – Python dependency specification for Trustpilot API integration and connector requirements.

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.

Important Trustpilot limitations:
- API rate limits apply to large accounts
- Data retention varies by Trustpilot plan
- Some features require specific Trustpilot licenses
- Historical data availability depends on account settings
- API pagination limits (100 results per page)

We recommend reviewing these limitations with your Trustpilot account plan to ensure it meets your data requirements.

Performance considerations:
- Large accounts may experience longer sync times
- Multiple API endpoints increase API calls
- Historical data syncs can be resource-intensive
- We recommend using incremental syncs for production environments to optimize performance

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

## References

- [Trustpilot API Documentation](https://developers.trustpilot.com/)
- [Trustpilot Business API](https://developers.trustpilot.com/business-api)
- [Fivetran Connector SDK Documentation](https://fivetran.com/docs/connector-sdk)
- [Fivetran Connector SDK Best Practices](https://fivetran.com/docs/connectors/connector-sdk/best-practices)
- [Fivetran Connector SDK v2.0.0+ Migration Guide](https://fivetran.com/docs/connector-sdk/tutorials/removing-yield-usage)
