# Veeva Vault Connector Example with Basic Authentication

This connector demonstrates how to integrate Veeva Vault with Fivetran using the Connector SDK. It provides an example of retrieving data from all object types in Veeva Vault using VQL (Vault Query Language) with support for basic authentication, dynamic schema generation, and incremental syncs.

## Requirements

* [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)   
* Operating System:  
  * Windows 10 or later  
  * macOS 13 (Ventura) or later
* Veeva Vault account
* Veeva Vault API credentials
* Access to Veeva Vault API v19.3 or later
* Network access to Veeva Vault instance

## Getting Started

Refer to the [Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

* Demonstrates Veeva Vault API integration
* Implements Basic Authentication
* Supports dynamic schema discovery
* Uses VQL for data retrieval
* Implements efficient pagination
* Includes state management for resumable syncs
* Handles burst limits and rate limiting
* Provides comprehensive error handling
* Supports incremental syncs using modification dates
* Handles object type deduplication
* Processes multiple data types
* Implements field mapping

## Configuration File

The connector requires the following configuration parameters:

```json
{
    "username": "YOUR_VEEVA_VAULT_USERNAME",
    "password": "YOUR_VEEVA_VAULT_PASSWORD",
    "subdomain": "YOUR_VEEVA_VAULT_SUBDOMAIN"
}
```

* `username`: Veeva Vault username
* `password`: Veeva Vault password
* `subdomain`: Veeva Vault instance subdomain

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements File

The connector requires the following Python packages:

```
requests
```

Note: The `fivetran_connector_sdk:latest` package is pre-installed in the Fivetran environment.

## Authentication

The connector uses Basic Authentication:
1. Credentials provided in configuration
2. Base64 encoded username:password
3. Token included in request headers
4. Automatic validation on each request

## Data Handling

The connector dynamically creates tables based on Veeva Vault object types. Each object type becomes a table with the following characteristics:

### Common Table Structure
| Column       | Type         | Description                    |
|-------------|--------------|--------------------------------|
| id          | STRING       | Primary key                    |
| created_by  | STRING       | Record creator                 |
| modified_by | STRING       | Last modifier                  |
| created_date| UTC_DATETIME | Creation timestamp             |
| modified_date| UTC_DATETIME| Last modification timestamp    |
| ...         | VARIOUS      | Object-specific fields         |

The connector implements the following data handling features:
* Dynamic schema generation from object types
* Batch processing (1000 records per page)
* Automatic field type mapping
* Incremental sync using modified_date
* JSON serialization for array fields
* Field deduplication
* State management for tracking sync progress
* Efficient pagination handling

## Error Handling

The connector implements the following error handling:
* API authentication error handling
* Response status validation
* Burst limit handling with backoff
* Rate limit management
* Request timeout handling
* Comprehensive logging
* API error propagation
* Field validation

## Additional Considerations

This example is intended for learning purposes and demonstrates Veeva Vault integration. For production use, you should:

1. Implement appropriate error retry mechanisms
2. Add proper rate limit handling
3. Optimize batch sizes for your data volume
4. Add monitoring for sync performance
5. Implement proper logging strategy
6. Consider implementing custom field mapping
7. Add proper handling for API outages
8. Consider implementing data validation
9. Add proper cleanup procedures
10. Consider implementing custom object filtering
11. Implement proper error notification system
12. Consider implementing connection pooling

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team. 