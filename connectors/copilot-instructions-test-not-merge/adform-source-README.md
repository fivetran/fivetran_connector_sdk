# Adform Connector

This connector provides Adform advertising campaign data synchronization into your data warehouse for campaign management and advertising insights. It leverages Adform's Buyer API to extract campaign metadata and configuration data for digital advertising analysis.

## Connector overview

The Adform Connector is a Fivetran Connector SDK implementation that extracts campaign data from Adform's Buyer API endpoints. It provides detailed insights into campaign configuration, status, and metadata for digital advertising management.

**Data Source**: Adform Buyer API (v1) for campaign data

**Use Cases**: Campaign management, advertising configuration analysis, campaign status tracking, and digital marketing intelligence.

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

### Alternative: Manual Setup

For local development and testing, refer to the [Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) and the [Manual Deployment](#manualdeployment) section below.

## Deployment

> **PRODUCTION DEPLOYMENT RECOMMENDATION**
>
> We recommend using GitHub Actions workflow for all production deployments.
> Manual deployment is available for development, testing, and debugging purposes, but we encourage using the automated workflow for production environments.

The connector is designed to work with **GitHub Actions workflow** as the **recommended production deployment mechanism**, while keeping manual deployment available for development and testing scenarios.

### Primary: Automated Deployment via GitHub Actions

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
   ADFORM_API_KEY                  # Your Adform API key
   ADFORM_CLIENT_ID                # Your Adform client ID
   FIVETRAN_API_KEY                # Your Fivetran API key
   ```

3. **Add Repository Variables** (Settings → Secrets and variables → Actions):
   ```
   FIVETRAN_DEV_DESTINATION        # Your Fivetran destination ID
   ADFORM_DEV                      # Your Fivetran connection name
   ```

4. **Deploy**: Simply push changes to the `main` branch - deployment happens automatically!

#### Workflow configuration:

The workflow uses parameterized environment variables for easy customization:

```yaml
env:
  PYTHON_VERSION: '3.11'
  WORKING_DIRECTORY: 'examples/source_examples/adform'
  CONNECTOR_NAME: 'Adform'
  CONFIG_FILE: 'configuration.json'
  EXCLUDED_DEPENDENCIES: '^requests\b'
```

#### Deployment Process:

1. **Automatic Trigger**: Push changes to `main` branch.
2. **Environment Setup**: Python 3.11, dependencies installation.
3. **Configuration Creation**: Generates `configuration.json` from GitHub secrets.
4. **Fivetran Deployment**: Executes `fivetran deploy` command.
5. **Status Reporting**: Provides deployment success/failure feedback.

#### Customization:

To adapt this workflow for other connectors, simply update the environment variables:

```yaml
env:
  PYTHON_VERSION: '3.11'
  WORKING_DIRECTORY: 'examples/source_examples/other-connector'
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
   - Update `configuration.json` with your Adform API credentials
   - Ensure the file is not committed to version control

3. **Deploy to Fivetran:**
   ```bash
   fivetran deploy --api-key YOUR_FIVETRAN_API_KEY --destination YOUR_DESTINATION_ID --connection YOUR_CONNECTION_NAME --configuration configuration.json --python-version 3.11
   ```

## Features

* **Campaign Data**: Campaign metadata, budgets, status, and configuration tracking
* **Campaign Management**: Campaign status tracking, budget management, and configuration analysis
* **Dynamic Time Ranges**: Intelligent data fetching based on sync type (initial vs incremental)
* **Incremental Syncs**: Efficient data updates with checkpoint-based state management
* **Error Handling**: Robust error handling with comprehensive logging and validation
* **CI/CD Integration**: Automated deployment via GitHub Actions with parameterized configuration
* **Environment Management**: Secure credential management using GitHub Environments
* **Zero-Downtime Deployment**: Seamless updates without service interruption

## Configuration file

**PRODUCTION DEPLOYMENT GUIDANCE**: For production deployments, we recommend using **GitHub secrets** configured in the GitHub Actions workflow. Local configuration files can be used for development and testing, but we encourage using GitHub secrets for production environments.

The connector requires the following configuration keys. For production deployments, we recommend configuring these as **GitHub secrets** (see [Deployment](#deployment) section). For local development and testing, you can define them in `configuration.json`:

```json
{
  "api_key": "YOUR_ADFORM_API_KEY",
  "client_id": "YOUR_CLIENT_ID",
  "sync_frequency_hours": "4",
  "initial_sync_days": "90",
  "max_records_per_page": "100",
  "request_timeout_seconds": "30",
  "retry_attempts": "3",
  "enable_incremental_sync": "true",
  "enable_debug_logging": "false"
}
```

**Required Configuration Keys:**
- `api_key`: Adform API key for Buyer API access
- `client_id`: Adform client ID for data extraction

**Optional Configuration Keys:**
- `sync_frequency_hours`: Sync frequency in hours (1-24, default: 4)
- `initial_sync_days`: Days of historical data for initial sync (1-365, default: 90)
- `max_records_per_page`: Maximum records per API page (1-1000, default: 100)
- `request_timeout_seconds`: API request timeout in seconds (default: 30)
- `retry_attempts`: Number of retry attempts for failed requests (default: 3)
- `enable_incremental_sync`: Enable incremental sync (true/false, default: true)
- `enable_debug_logging`: Enable debug logging (true/false, default: false)

## Production vs Development Configuration

### Production deployment (Recommended)
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

**API Key Setup:**
1. Go to your Adform account dashboard
2. Navigate to Settings > API Keys
3. Create a new API key with appropriate permissions
4. Add the API key to `configuration.json`

**Client ID:**
- Found in your Adform account settings
- Used to scope data extraction to specific client accounts

Note: We recommend keeping the `configuration.json` file out of version control to protect sensitive information. You can add it to your `.gitignore` file for additional security.

### Development configuration (Local Testing)

**Development Setup**: The following configuration methods are designed for local development and testing. We recommend using GitHub secrets for production deployments.

For local development and testing:

```bash
# Option 1: Set environment variables locally
export ADFORM_API_KEY="your_api_key"
export ADFORM_CLIENT_ID="your_client_id"

# Option 2: Use configuration.json file (never commit this file)
# Update configuration.json with your test credentials
```

**Production Deployment**: We recommend using the GitHub Actions workflow with GitHub secrets (see [Deployment](#deployment) section above).

## Requirements file

The `requirements.txt` file specifies the Python libraries required by the connector for Adform API integration and data processing.

**Example content of `requirements.txt`:**

```
requests>=2.28.0
```

Note: The `fivetran_connector_sdk:latest` package (v2.0.0+) is pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare it in your `requirements.txt`. This connector uses the updated SDK version that does not require `yield` statements for operations.

## Authentication

The connector uses Adform API keys for authentication with the Buyer API. Authentication is provided through:

1. **API Keys**: Direct Adform API key in configuration
2. **Client Access**: Client ID for data scope definition

**Required API Permissions:**
- Buyer API access for querying campaign data
- Client-level access for the specified client ID
- Read permissions for campaigns

We recommend reviewing these permissions with your Adform account administrator to ensure proper access levels.

**Steps to obtain credentials:**
1. Go to your Adform account dashboard
2. Navigate to Settings > API Keys
3. Create a new API key with appropriate permissions
4. **For Production**: We recommend adding credentials as GitHub secrets (see [Deployment](#deployment) section)
5. **For Development**: You can add credentials to `configuration.json` (remember not to commit this file)

## Pagination

The connector handles data retrieval from Adform Buyer API with efficient pagination:

- **Campaigns API**: Supports pagination for large campaign sets
- **Data Volume Management**: Uses appropriate time ranges to manage data volume
- **Incremental Syncs**: Implements incremental syncs to reduce data transfer

The connector implements efficient data handling by:
- Processing complete API responses in memory
- Using appropriate time ranges to manage data volume
- Implementing incremental syncs to reduce data transfer
- Applying filters to focus on relevant data subsets

Refer to function `get_campaigns` in `connector.py` for the data retrieval implementation.

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

The connector processes Adform data through several stages:

1. **Data Extraction**: Direct Buyer API calls to Adform endpoints
2. **Data Transformation**: Conversion of Adform API responses to structured table format
3. **Schema Mapping**: Consistent data types and column naming across all tables
4. **State Management**: Checkpoint-based incremental sync support
5. **Error Handling**: Comprehensive error handling with logging and validation

**Data Processing Features:**
- **Type Conversion**: Adform API responses converted to appropriate data types
- **Default Values**: Missing dimensions populated with appropriate defaults
- **Timestamp Handling**: ISO format timestamp conversion and period-based grouping
- **Filtering**: Campaign and time-based filtering based on configuration
- **Dynamic Time Ranges**: Intelligent time range selection for initial vs incremental syncs

**Data Flow:**
Adform Buyer API → Requests Client → Dynamic Time Range Processing → Data Processing Functions → Fivetran Operations → Data Warehouse Tables

Refer to functions `get_time_range`, `execute_api_request`, and `get_campaigns` in `connector.py` for detailed data handling logic.

## Error handling

The connector implements comprehensive error handling strategies to ensure robust operation:

**Configuration Validation Errors:**
- **ValueError**: Missing required configuration values (`api_key`, `client_id`)
- **ValueError**: Empty API key or client ID values

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

The connector creates the following table for Adform campaign data analysis. **Column types are automatically inferred by Fivetran** based on the actual data structure and content.

### campaign
Primary table for campaign information and configuration data with comprehensive campaign metadata.

**Primary Key**: `id`

**Sample Columns** (automatically inferred by Fivetran):
- `id` - Unique Adform campaign identifier
- `client_id` - Adform client identifier
- `name` - Campaign name
- `status` - Campaign status
- `start_date` - Campaign start date
- `end_date` - Campaign end date
- `budget` - Campaign budget amount
- `currency` - Budget currency code
- `campaign_type` - Type of campaign
- `objective` - Campaign objective
- `created_at` - Campaign creation timestamp
- `updated_at` - Campaign last update timestamp
- `sync_timestamp` - Data extraction timestamp

## Additional files

The connector includes several additional files to support functionality and deployment:

* **`.github/workflows/deploy_connector.yaml`** – **Primary deployment mechanism**: GitHub Actions workflow for automated deployment to Fivetran with parameterized configuration and secure credential management.

* **`requirements.txt`** – Python dependency specification for Adform API integration and connector requirements.

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.

**Important Adform Limitations:**
- API rate limits apply to large accounts
- Data retention varies by Adform plan
- Some features require specific Adform licenses
- Historical data availability depends on account settings
- API pagination limits apply to campaign endpoints

We recommend reviewing these limitations with your Adform account plan to ensure it meets your data requirements.

**Performance Considerations:**
- Large accounts may experience longer sync times
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

- [Adform API Documentation](https://api.adform.com/help/)
- [Fivetran Connector SDK Documentation](https://fivetran.com/docs/connector-sdk)
- [Fivetran Connector SDK Best Practices](https://fivetran.com/docs/connectors/connector-sdk/best-practices)
