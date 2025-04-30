# OAuth2 and Accelo API Connector Example with Multithreading

This connector demonstrates how to integrate Accelo's API with Fivetran using the Connector SDK, featuring OAuth 2.0 authentication and multithreaded data extraction. It provides an example of efficiently syncing multiple data entities (companies, invoices, payments, prospects, jobs, and staff) using parallel API requests to improve performance.

## Requirements

* [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)   
* Operating System:  
  * Windows 10 or later  
  * macOS 13 (Ventura) or later
* Accelo account with API access
* OAuth 2.0 client credentials (client ID and secret)
* Accelo deployment name
* Access to Accelo API v0 endpoints

## Getting Started

Refer to the [Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

* Demonstrates OAuth 2.0 client credentials flow
* Implements multithreaded data extraction
* Supports multiple entity types (companies, invoices, payments, prospects, jobs, staff)
* Implements efficient pagination handling
* Includes state checkpointing for resumable syncs
* Handles rate limiting and retries
* Provides comprehensive error handling
* Supports incremental syncs using modification dates
* Configurable batch sizes and timeouts
* Thread-safe state management
* Automatic data type conversion

## Configuration File

The connector requires the following configuration parameters:

```json
{
  "deployment": "YOUR_ACCELO_DEPLOYMENT",
  "client_id": "YOUR_OAUTH_CLIENT_ID",
  "client_secret": "YOUR_OAUTH_CLIENT_SECRET"
}
```

* `deployment`: Your Accelo deployment name (e.g., "mycompany" for mycompany.accelo.com)
* `client_id`: OAuth 2.0 client ID from Accelo
* `client_secret`: OAuth 2.0 client secret from Accelo

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements File

The connector requires the following Python packages:

```
requests
python-dotenv
```

Note: The `fivetran_connector_sdk:latest` package is pre-installed in the Fivetran environment.

## Authentication

The connector uses OAuth 2.0 client credentials flow:
1. Client credentials provided in configuration
2. Access token obtained through OAuth 2.0 token endpoint
3. Token automatically included in API requests
4. Read-only scope used for data access

## Additional Files

* **api_threading_utils.py** – Handles parallel API request execution and response processing
* **constants.py** – Defines configuration constants for API limits, timeouts, and batch sizes

## Data Handling

The connector syncs the following tables:

### Companies Table
| Column       | Type         | Description                    |
|-------------|--------------|--------------------------------|
| id          | INTEGER      | Primary key                    |
| name        | STRING       | Company name                   |
| date_modified| UTC_DATETIME | Last modification date         |
| website     | STRING       | Company website                |
| phone       | STRING       | Contact phone number           |

### Invoices Table
| Column       | Type         | Description                    |
|-------------|--------------|--------------------------------|
| id          | INTEGER      | Primary key                    |
| date_created| UTC_DATETIME | Creation date                  |
| date_modified| UTC_DATETIME | Last modification date         |
| amount      | FLOAT       | Invoice amount                 |
| status      | STRING       | Invoice status                 |

Similar schemas exist for Payments, Prospects, Jobs, and Staff tables.

The connector implements the following data handling features:
* Configurable batch size (default: 100 records per request)
* Parallel API requests (default: 5 workers)
* Automatic type conversion for integers, floats, and dates
* Incremental sync using modification dates
* State management for resumable syncs
* Rate limit handling (5000 requests per hour)

## Error Handling

The connector implements the following error handling:
* OAuth 2.0 authentication error handling
* Exponential backoff for failed requests
* Configurable retry attempts (default: 3)
* Request timeout handling
* Thread-safe state management
* Comprehensive logging
* API rate limit management
* Proper error propagation

## Additional Considerations

This example is intended for learning purposes and demonstrates advanced Accelo API integration. For production use, you should:

1. Implement appropriate error retry mechanisms
2. Adjust rate limiting based on your API tier
3. Configure optimal thread count for your use case
4. Add monitoring for sync performance
5. Implement proper logging strategy
6. Consider implementing custom entity filtering
7. Add proper handling for API outages
8. Consider implementing data validation
9. Add proper cleanup procedures
10. Consider implementing custom data transformations
11. Implement proper error notification system
12. Consider implementing connection pooling

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team. 