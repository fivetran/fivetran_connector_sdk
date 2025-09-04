# Trustpilot API Connector

*This connector provides comprehensive Trustpilot review and business data synchronization into your data warehouse for customer feedback analysis, reputation management, and business insights. It leverages Trustpilot's REST API to extract reviews, business information, categories, consumer reviews, and invitation links data for comprehensive customer experience analysis.*

## Connector overview

The Trustpilot API Connector is a Fivetran Connector SDK implementation that extracts comprehensive review and business data from Trustpilot's various API endpoints. It provides detailed insights into customer feedback, business reputation, review management, and customer engagement across multiple Trustpilot services.

**Data Source**: Trustpilot REST API (v1) for reviews, business units, categories, consumer reviews, and invitation links

**Use Cases**: Customer feedback analysis, reputation management, review monitoring, customer experience optimization, business intelligence, and comprehensive customer insights.

## Requirements

- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)   
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started

### **Quick Start (Recommended)**

1. **Fork or clone this repository**
2. **Set up GitHub secrets and variables** (see [Deployment](#deployment) section)
3. **Push changes to `main` branch** - deployment happens automatically!

### **Alternative: Manual Setup**

For local development and testing, refer to the [Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) and the [Manual Deployment](#-secondary-manual-deployment-debugging--testing) section below.

## Deployment

> **🚀 PRODUCTION DEPLOYMENT RECOMMENDATION**  
> **We recommend using GitHub Actions workflow for all production deployments.**  
> Manual deployment is available for development, testing, and debugging purposes, but we encourage using the automated workflow for production environments.

The connector is designed to work with **GitHub Actions workflow** as the **recommended production deployment mechanism**, while keeping manual deployment available for development and testing scenarios.

### **🚀 Primary: Automated Deployment via GitHub Actions**

The connector includes a GitHub Actions workflow (`.github/workflows/deploy_connector.yaml`) for automated deployment.

#### **Workflow Features:**
- **Automated Triggers**: Deploys on pushes to the `main` branch
- **Path-Based Triggers**: Only runs when changes are made to the connector directory
- **Parameterized Configuration**: Easy to customize Python version, working directory, and other settings
- **Environment Management**: Uses GitHub Environments for secure credential management
- **Zero-Downtime Deployment**: Seamless updates without service interruption

#### **Quick Setup:**

1. **Create GitHub Environment:**
   - Go to repository Settings → Environments
   - Create environment named `Fivetran`

2. **Add Repository Secrets** (Settings → Secrets and variables → Actions):
   ```
   TRUSTPILOT_API_KEY          # Your Trustpilot API key
   TRUSTPILOT_BUSINESS_UNIT_ID # Your Trustpilot business unit ID
   TRUSTPILOT_CONSUMER_ID      # Your Trustpilot consumer ID
   FIVETRAN_API_KEY            # Your Fivetran API key
   ```

3. **Add Repository Variables** (Settings → Secrets and variables → Actions):
   ```
   FIVETRAN_DEV_DESTINATION    # Your Fivetran destination ID
   TRUSTPILOT_DEV              # Your Fivetran connection name
   ```

4. **Deploy**: Simply push changes to the `main` branch - deployment happens automatically!

#### **Workflow Configuration:**

The workflow uses parameterized environment variables for easy customization:

```yaml
env:
  PYTHON_VERSION: '3.11'
  WORKING_DIRECTORY: 'connections/csdk/trustpilot-connector'
  CONNECTOR_NAME: 'Trustpilot'
  CONFIG_FILE: 'configuration.json'
  EXCLUDED_DEPENDENCIES: '^requests\b'
```

#### **Deployment Process:**

1. **Automatic Trigger**: Push changes to `main` branch
2. **Environment Setup**: Python 3.11, dependencies installation
3. **Configuration Creation**: Generates `configuration.json` from GitHub secrets
4. **Fivetran Deployment**: Executes `fivetran deploy` command
5. **Status Reporting**: Provides deployment success/failure feedback

#### **Customization:**

To adapt this workflow for other connectors, simply update the environment variables:

```yaml
env:
  PYTHON_VERSION: '3.11'
  WORKING_DIRECTORY: 'connections/csdk/other-connector'
  CONNECTOR_NAME: 'Other'
  CONFIG_FILE: 'configuration.json'
  EXCLUDED_DEPENDENCIES: '^requests\b'
```

### **🔧 Secondary: Manual Deployment (Debugging & Testing)**

For local development, testing, and debugging purposes:

1. **Install Dependencies:**
   ```bash
   pip install fivetran-connector-sdk
   ```

2. **Configure Credentials:**
   - Update `configuration.json` with your Trustpilot API credentials
   - Ensure the file is not committed to version control

3. **Deploy to Fivetran:**
   ```bash
   fivetran deploy --api-key YOUR_FIVETRAN_API_KEY --destination YOUR_DESTINATION_ID --connection YOUR_CONNECTION_NAME --configuration configuration.json --python-version 3.11
   ```

## Features

* **Comprehensive Reviews Data**: Customer feedback, ratings, review text, verification status, and helpfulness metrics
* **Business Intelligence**: Business unit information, trust scores, review counts, and reputation metrics
* **Category Management**: Business categorization, hierarchical structure, and localized naming
* **Consumer Reviews API**: Individual consumer review history, verification status, and engagement tracking
* **Invitation Management**: Review invitation tracking, response rates, and campaign effectiveness
* **Cross-Service Correlation**: Integrated analysis across all Trustpilot services for comprehensive insights
* **Dynamic Time Ranges**: Intelligent data fetching based on sync type (initial vs incremental)
* **Incremental Syncs**: Efficient data updates with checkpoint-based state management
* **Configurable Filtering**: Business unit and time-based filtering for focused analysis
* **Error Handling**: Robust error handling with comprehensive logging and validation
* **CI/CD Integration**: Automated deployment via GitHub Actions with parameterized configuration
* **Environment Management**: Secure credential management using GitHub Environments
* **Zero-Downtime Deployment**: Seamless updates without service interruption

## Configuration file

💡 **PRODUCTION DEPLOYMENT GUIDANCE**: For production deployments, we recommend using **GitHub secrets** configured in the GitHub Actions workflow. Local configuration files can be used for development and testing, but we encourage using GitHub secrets for production environments.

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

**Required Configuration Keys:**
- `api_key`: Trustpilot API key for REST API access
- `business_unit_id`: Trustpilot business unit ID for data extraction
- `consumer_id`: Trustpilot consumer ID for fetching individual consumer reviews

**Optional Configuration Keys:**
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

### **🚀 Production Deployment (Recommended)**
- **Method**: GitHub Actions workflow with GitHub secrets
- **Security**: Credentials stored securely in GitHub
- **Automation**: Push-to-deploy workflow
- **Compliance**: Enterprise-grade security practices

### **🔧 Development & Testing (Available)**
- **Method**: Local `configuration.json` or environment variables
- **Security**: Credentials stored locally (never commit to repository)
- **Use Case**: Local development, debugging, testing
- **Guidance**: We recommend using GitHub secrets for production

**💡 Security Note**: For production deployments, we recommend using **GitHub secrets**. Local configuration files are available for development but we encourage using GitHub secrets for production environments.

**API Key Setup:**
1. Go to your Trustpilot Business account
2. Navigate to Settings > API Keys
3. Create a new API key with appropriate permissions
4. Add the API key to `configuration.json`

**Business Unit ID:**
- Found in your Trustpilot Business dashboard
- Used to scope data extraction to specific business units

**Consumer ID:**
- Found in consumer profile URLs or API responses
- Used to fetch individual consumer review history

Note: We recommend keeping the `configuration.json` file out of version control to protect sensitive information. You can add it to your `.gitignore` file for additional security.

### **Development Configuration (Local Testing)**

💡 **Development Setup**: The following configuration methods are designed for local development and testing. We recommend using GitHub secrets for production deployments.

For local development and testing:

```bash
# Option 1: Set environment variables locally
export TRUSTPILOT_API_KEY="your_api_key"
export TRUSTPILOT_BUSINESS_UNIT_ID="your_business_unit_id"
export TRUSTPILOT_CONSUMER_ID="your_consumer_id"

# Option 2: Use configuration.json file (never commit this file)
# Update configuration.json with your test credentials
```

**Production Deployment**: We recommend using the GitHub Actions workflow with GitHub secrets (see [Deployment](#deployment) section above).

## Requirements file

The `requirements.txt` file specifies the Python libraries required by the connector for Trustpilot API integration and data processing.

**Example content of `requirements.txt`:**

```
requests>=2.28.0
```

Note: The `fivetran_connector_sdk:latest` package (v2.0.0+) is pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare it in your `requirements.txt`. This connector uses the updated SDK version that does not require `yield` statements for operations.

## Authentication

The connector uses Trustpilot API keys for authentication with the REST API. Authentication is provided through:

1. **API Keys**: Direct Trustpilot API key in configuration
2. **Business Unit Access**: Business unit ID for data scope definition

**Required API Permissions:**
- REST API access for querying business data
- Business unit-level access for the specified business unit ID
- Read permissions for reviews, consumers, and invitations

We recommend reviewing these permissions with your Trustpilot account administrator to ensure proper access levels.

**Steps to obtain credentials:**
1. Go to your Trustpilot Business account
2. Navigate to Settings > API Keys
3. Create a new API key with appropriate permissions
4. **For Production**: We recommend adding credentials as GitHub secrets (see [Deployment](#deployment) section)
5. **For Development**: You can add credentials to `configuration.json` (remember not to commit this file)

## Pagination

The connector handles data retrieval from Trustpilot REST API with efficient pagination:

- **Reviews API**: Limited to 100 results per page with time-based filtering
- **Consumers API**: Supports pagination for large result sets
- **Invitations API**: Implements pagination for invitation data
- **Data Volume Management**: Uses appropriate time ranges to manage data volume
- **Incremental Syncs**: Implements incremental syncs to reduce data transfer

The connector implements efficient data handling by:
- Processing complete API responses in memory
- Using appropriate time ranges to manage data volume
- Implementing incremental syncs to reduce data transfer
- Applying filters to focus on relevant data subsets

Refer to functions `get_reviews_data`, `get_business_data`, `get_categories_data`, `get_consumers_data`, and `get_invitations_data` in `connector.py` for the data retrieval implementation.

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
- **Flexible Queries**: All API calls dynamically adjust time ranges
- **Logging**: Clear indication of sync type and time ranges in logs

## Data handling

The connector processes Trustpilot data through several stages:

1. **Data Extraction**: Direct REST API calls to Trustpilot endpoints
2. **Data Transformation**: Conversion of Trustpilot API responses to structured table format
3. **Schema Mapping**: Consistent data types and column naming across all tables
4. **State Management**: Checkpoint-based incremental sync support
5. **Error Handling**: Comprehensive error handling with logging and validation

**Data Processing Features:**
- **Type Conversion**: Trustpilot API responses converted to appropriate data types
- **Default Values**: Missing dimensions populated with appropriate defaults
- **Timestamp Handling**: ISO format timestamp conversion and period-based grouping
- **Data Aggregation**: Multiple metric aggregations (counts, averages, percentages)
- **Filtering**: Business unit and time-based filtering based on configuration
- **Dynamic Time Ranges**: Intelligent time range selection for initial vs incremental syncs

**Data Flow:**
Trustpilot REST API → Requests Client → Dynamic Time Range Processing → Data Processing Functions → Fivetran Operations → Data Warehouse Tables

Refer to functions `get_time_range`, `execute_api_request`, `get_reviews_data`, `get_business_data`, `get_categories_data`, `get_consumers_data`, and `get_invitations_data` in `connector.py` for detailed data handling logic.

## Error handling

The connector implements comprehensive error handling strategies to ensure robust operation:

**Configuration Validation Errors:**
- **ValueError**: Missing required configuration values (`api_key`, `business_unit_id`, `consumer_id`)
- **ValueError**: Empty API key, business unit ID, or consumer ID values

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

The connector creates the following tables for comprehensive Trustpilot data analysis:

### **reviews**
Primary table for customer reviews and feedback data with comprehensive review metadata.

| Column Name | Type | Description |
|-------------|------|-------------|
| `review_id` | STRING | Unique Trustpilot review identifier |
| `business_unit_id` | STRING | Business unit identifier |
| `consumer_id` | STRING | Customer identifier |
| `consumer_name` | STRING | Customer display name |
| `stars` | INT | Review rating (1-5 stars) |
| `title` | STRING | Review title |
| `text` | STRING | Review content text |
| `language` | STRING | Review language |
| `created_at` | STRING | Review creation timestamp |
| `updated_at` | STRING | Review last update timestamp |
| `status` | STRING | Review status |
| `is_verified` | BOOLEAN | Whether review is verified |
| `helpful_count` | INT | Number of helpful votes |
| `reply_text` | STRING | Business reply text |
| `reply_created_at` | STRING | Reply creation timestamp |
| `timestamp` | STRING | Data extraction timestamp |

### **business_units**
Business unit information and reputation metrics for comprehensive business analysis.

| Column Name | Type | Description |
|-------------|------|-------------|
| `business_unit_id` | STRING | Unique business unit identifier |
| `name` | STRING | Business name |
| `display_name` | STRING | Business display name |
| `website_url` | STRING | Business website URL |
| `country_code` | STRING | Business country code |
| `language` | STRING | Business language |
| `number_of_reviews` | INT | Total number of reviews |
| `trust_score` | FLOAT | Trustpilot trust score |
| `stars` | FLOAT | Average star rating |
| `created_at` | STRING | Business creation timestamp |
| `updated_at` | STRING | Business last update timestamp |
| `timestamp` | STRING | Data extraction timestamp |

### **categories**
Business categories and hierarchical classification for business categorization analysis.

| Column Name | Type | Description |
|-------------|------|-------------|
| `category_id` | STRING | Unique category identifier |
| `name` | STRING | Category name |
| `localized_name` | STRING | Localized category name |
| `parent_id` | STRING | Parent category identifier |
| `level` | INT | Category hierarchy level |
| `created_at` | STRING | Category creation timestamp |
| `updated_at` | STRING | Category last update timestamp |
| `timestamp` | STRING | Data extraction timestamp |

### **consumer_reviews**
Individual consumer review history and engagement data for customer analysis and segmentation.

| Column Name | Type | Description |
|-------------|------|-------------|
| `consumer_id` | STRING | Unique customer identifier |
| `review_id` | STRING | Unique review identifier |
| `business_unit_id` | STRING | Business unit identifier |
| `business_unit_name` | STRING | Business unit display name |
| `stars` | INT | Review rating (1-5 stars) |
| `title` | STRING | Review title |
| `text` | STRING | Review content text |
| `language` | STRING | Review language |
| `status` | STRING | Review status |
| `is_verified` | BOOLEAN | Whether review is verified |
| `number_of_likes` | INT | Number of helpful votes |
| `created_at` | STRING | Review creation timestamp |
| `updated_at` | STRING | Review last update timestamp |
| `experienced_at` | STRING | When the experience occurred |
| `review_verification_level` | STRING | Level of review verification |
| `counts_towards_trust_score` | BOOLEAN | Whether review counts towards trust score |
| `counts_towards_location_trust_score` | BOOLEAN | Whether review counts towards location trust score |
| `company_reply_text` | STRING | Business reply text |
| `company_reply_created_at` | STRING | Reply creation timestamp |
| `company_reply_updated_at` | STRING | Reply last update timestamp |
| `location_id` | STRING | Location identifier |
| `location_name` | STRING | Location name |
| `timestamp` | STRING | Data extraction timestamp |

### **invitation_links**
Review invitation management and response tracking for campaign effectiveness analysis.

| Column Name | Type | Description |
|-------------|------|-------------|
| `invitation_id` | STRING | Unique invitation identifier |
| `business_unit_id` | STRING | Business unit identifier |
| `consumer_id` | STRING | Customer identifier |
| `consumer_email` | STRING | Customer email address |
| `status` | STRING | Invitation status |
| `type` | STRING | Invitation type |
| `created_at` | STRING | Invitation creation timestamp |
| `updated_at` | STRING | Invitation last update timestamp |
| `sent_at` | STRING | Invitation sent timestamp |
| `responded_at` | STRING | Customer response timestamp |
| `timestamp` | STRING | Data extraction timestamp |

## Additional files

The connector includes several additional files to support functionality and deployment:

* **`.github/workflows/deploy_connector.yaml`** – **Primary deployment mechanism**: GitHub Actions workflow for automated deployment to Fivetran with parameterized configuration and secure credential management.

* **`dashboard_queries.sql`** – Comprehensive SQL queries for building Trustpilot analytics dashboards and reports. Includes customer feedback analysis, reputation monitoring, review management, and business intelligence queries.

* **`deploy_connector.sh`** – Alternative deployment script for manual connector deployment to Fivetran environment (for debugging and testing).

* **`requirements.txt`** – Python dependency specification for Trustpilot API integration and connector requirements.

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.

**Important Trustpilot Limitations:**
- API rate limits apply to large accounts
- Data retention varies by Trustpilot plan
- Some features require specific Trustpilot licenses
- Historical data availability depends on account settings
- API pagination limits (100 results per page)

We recommend reviewing these limitations with your Trustpilot account plan to ensure it meets your data requirements.

**Performance Considerations:**
- Large accounts may experience longer sync times
- Multiple API endpoints increase API calls
- Historical data syncs can be resource-intensive
- We recommend using incremental syncs for production environments to optimize performance

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

- [Trustpilot API Documentation](https://developers.trustpilot.com/)
- [Trustpilot Business API](https://developers.trustpilot.com/business-api)
- [Fivetran Connector SDK Documentation](https://fivetran.com/docs/connector-sdk)
- [Fivetran Connector SDK Best Practices](https://fivetran.com/docs/connectors/connector-sdk/best-practices)
- [Fivetran Connector SDK v2.0.0+ Migration Guide](https://fivetran.com/docs/connector-sdk/tutorials/removing-yield-usage)
