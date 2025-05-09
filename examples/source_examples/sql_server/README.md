# Microsoft SQL Server Connector Example

This connector demonstrates how to integrate Microsoft SQL Server with Fivetran using the Connector SDK. It provides an example of syncing employee data from a SQL Server database with support for batch processing and proper connection management.

## Requirements

* [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)   
* Operating System:  
  * Windows 10 or later  
  * macOS 13 (Ventura) or later
* Microsoft SQL Server instance
* SQL Server ODBC Driver installed locally
* Database credentials with appropriate permissions
* Network access to SQL Server instance

## Getting Started

Refer to the [Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

* Demonstrates SQL Server integration
* Implements batch processing
* Provides schema definition
* Includes state management for resumable syncs
* Handles data type conversions
* Demonstrates proper connection management
* Includes example table creation and data population
* Supports primary key-based updates
* Implements efficient query patterns
* Uses ODBC driver for reliable connectivity

## Configuration File

The connector requires the following configuration parameters:

```json
{
  "driver": "{ODBC Driver 18 for SQL Server}",
  "server": "tcp:sql_test.database.windows.net,1433",
  "database": "YOUR_DATABASE_NAME",
  "user": "YOUR_USERNAME",
  "password": "YOUR_PASSWORD"
}
```

* `driver`: SQL Server ODBC driver name
* `server`: SQL Server instance address and port
* `database`: Name of the database to connect to
* `user`: Database username
* `password`: Database password

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements File

The connector requires the following Python packages:

```
pyodbc
```

Note: The `fivetran_connector_sdk:latest` package is pre-installed in the Fivetran environment.

## Data Handling

The connector syncs the following table:

### employee_details Table
| Column       | Type         | Description                    |
|-------------|--------------|--------------------------------|
| employee_id | INTEGER      | Primary key (auto-increment)   |
| first_name  | STRING       | Employee's first name          |
| last_name   | STRING       | Employee's last name           |
| hire_date   | NAIVE_DATE   | Employee's hire date          |
| salary      | LONG         | Employee's salary              |

The connector implements the following data handling features:
* Batch processing (2 records per batch)
* Automatic date formatting
* Primary key-based updates
* State management for tracking sync progress
* Proper connection cleanup
* Efficient memory usage with batch fetching

## Error Handling

The connector implements the following error handling:
* Database connection error handling
* ODBC driver error handling
* Query execution error handling
* Data type conversion handling
* Connection cleanup in finally blocks
* Comprehensive logging
* Proper error propagation
* Resource cleanup

## Additional Considerations

This example is intended for learning purposes and demonstrates SQL Server integration. For production use, you should:

- Implement appropriate error retry mechanisms
- Add connection pooling
- Optimize batch sizes for your data volume
- Add monitoring for sync performance
- Implement proper logging strategy
- Consider implementing custom data filtering
- Add proper handling for network issues
- Consider implementing data validation
- Add proper cleanup procedures
- Consider implementing custom data transformations
- Implement proper error notification system
- Consider implementing connection timeouts

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team. 
