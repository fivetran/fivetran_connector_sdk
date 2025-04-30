# Amazon Redshift Connector Example

This connector demonstrates how to integrate Amazon Redshift with Fivetran using the Connector SDK. It provides an example of syncing customer data from a Redshift database with support for incremental updates based on timestamp tracking.

## Requirements

* [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)   
* Operating System:  
  * Windows 10 or later  
  * macOS 13 (Ventura) or later
* Amazon Redshift cluster
* Database credentials with appropriate permissions
* Network access to Redshift cluster

## Getting Started

Refer to the [Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

* Demonstrates Amazon Redshift integration
* Implements incremental sync using timestamp-based tracking
* Supports batch processing with checkpointing
* Provides schema definition
* Includes state management for resumable syncs
* Handles data type conversions
* Demonstrates proper connection management
* Includes example table creation and data population
* Supports primary key-based updates
* Implements efficient query patterns

## Configuration File

The connector requires the following configuration parameters:

```json
{
  "host": "YOUR_REDSHIFT_HOST",
  "database": "YOUR_DATABASE_NAME",
  "port": "YOUR_PORT",
  "user": "YOUR_USERNAME",
  "password": "YOUR_PASSWORD"
}
```

* `host`: Redshift cluster endpoint
* `database`: Name of the database to connect to
* `port`: Port number for the connection (typically 5439)
* `user`: Database username
* `password`: Database password

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements File

The connector requires the following Python packages:

```
redshift_connector
```

Note: The `fivetran_connector_sdk:latest` package is pre-installed in the Fivetran environment.

## Data Handling

The connector syncs the following table:

### customers Table
| Column       | Type         | Description                    |
|-------------|--------------|--------------------------------|
| customer_id | INTEGER      | Primary key                    |
| first_name  | STRING       | Customer's first name          |
| last_name   | STRING       | Customer's last name           |
| email       | STRING       | Customer's email address       |
| updated_at  | UTC_DATETIME | Last modification timestamp    |

The connector implements the following data handling features:
* Incremental sync using updated_at timestamp
* Batch processing (2 records per batch)
* Automatic timestamp formatting
* Primary key-based updates
* State management for tracking sync progress
* Sorted result sets for consistency

## Error Handling

The connector implements the following error handling:
* Database connection error handling
* Schema validation
* Data type conversion handling
* State management validation
* Proper connection cleanup
* Comprehensive logging

## Additional Considerations

This example is intended for learning purposes and demonstrates Redshift integration. For production use, you should:

1. Implement appropriate error retry mechanisms
2. Add connection pooling
3. Optimize batch sizes for your data volume
4. Add monitoring for sync performance
5. Implement proper logging strategy
6. Consider implementing custom data filtering
7. Add proper handling for network issues
8. Consider implementing data validation
9. Add proper cleanup procedures
10. Consider implementing custom data transformations
11. Implement proper error notification system
12. Consider implementing connection timeouts

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team. 