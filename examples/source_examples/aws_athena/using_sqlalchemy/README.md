# AWS Athena Connector Example (using SQLAlchemy)

This connector demonstrates how to integrate AWS Athena with Fivetran using the Connector SDK, SQLAlchemy, and PyAthena. It provides an example of querying and retrieving data from AWS Athena tables using a familiar SQLAlchemy interface and syncing it to your Fivetran destination.

## Requirements

* [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)   
* Operating System:  
  * Windows 10 or later  
  * macOS 13 (Ventura) or later
* AWS Account with Athena access
* AWS IAM credentials with appropriate permissions
* S3 bucket for query results

## Getting Started

Refer to the [Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

* Demonstrates AWS Athena integration using SQLAlchemy
* Uses PyAthena as the database driver
* Executes SQL queries through SQLAlchemy interface
* Implements batch processing of results
* Includes state checkpointing for resumable syncs
* Provides error handling for database operations

## Configuration File

The connector requires the following configuration parameters:

```json
{
  "aws_access_key_id": "YOUR_AWS_ACCESS_KEY_ID",
  "aws_secret_access_key": "YOUR_AWS_SECRET_ACCESS_KEY",
  "region_name": "YOUR_AWS_REGION",
  "database_name": "YOUR_ATHENA_DATABASE",
  "s3_staging_dir": "s3://your-bucket/path/to/query/results/"
}
```

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements File

The connector requires the following Python packages:

```
sqlalchemy
PyAthena
```

Note: The `fivetran_connector_sdk:latest` package is pre-installed in the Fivetran environment.

## Authentication

The connector uses AWS credentials for authentication through SQLAlchemy connection string:
```
awsathena+rest://{access_key}:{secret_key}@athena.{region}.amazonaws.com:443/{database}?s3_staging_dir={staging_dir}
```

The connection string is automatically constructed using the provided configuration parameters.

## Data Handling

The connector syncs the following table:

### Customers Table
| Column      | Type           | Description                    |
|-------------|----------------|--------------------------------|
| customer_id | STRING         | Primary key                    |
| first_name  | STRING         | Customer's first name          |
| last_name   | STRING         | Customer's last name           |
| email       | STRING         | Customer's email address       |

The connector uses SQLAlchemy's result fetching with batch processing:
* Fetches results in batches of 2 rows
* Automatically handles data type conversion
* Provides efficient memory usage

## Error Handling

The connector implements the following error handling:
* Validates AWS credentials and configuration
* Uses SQLAlchemy's error handling mechanisms
* Implements connection management
* Includes proper resource cleanup
* Manages batch processing errors

## Additional Considerations

This example is intended for learning purposes and demonstrates AWS Athena integration using SQLAlchemy. For production use, you should:

1. Adjust batch size based on your data volume
2. Implement appropriate connection pooling
3. Add proper error retry mechanisms
4. Consider implementing query optimization
5. Add monitoring for query performance
6. Implement cost control measures
7. Add proper IAM role-based authentication
8. Consider implementing query result caching
9. Add proper cleanup of query results in S3
10. Implement proper error handling for AWS service outages

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team. 