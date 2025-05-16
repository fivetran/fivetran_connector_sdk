# AWS Athena Connector Example (using boto3)

This connector demonstrates how to integrate AWS Athena with Fivetran using the Connector SDK and boto3. It provides an example of querying and retrieving data from AWS Athena tables and syncing it to your Fivetran destination.

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

* Demonstrates AWS Athena integration using boto3
* Executes SQL queries on Athena
* Handles query execution and result retrieval
* Supports pagination of query results
* Implements data type conversion
* Includes state checkpointing for resumable syncs
* Provides error handling for AWS operations

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
boto3
```

Note: The `fivetran_connector_sdk:latest` package is pre-installed in the Fivetran environment.

## Authentication

The connector uses AWS credentials for authentication:
1. AWS access key ID and secret access key for boto3 client initialization
2. Region name for AWS service endpoint
3. S3 bucket permissions for storing query results

## Data Handling

The connector syncs the following table:

### Customers Table
| Column      | Type           | Description                    |
|-------------|----------------|--------------------------------|
| customer_id | STRING         | Primary key                    |
| first_name  | STRING         | Customer's first name          |
| last_name   | STRING         | Customer's last name           |
| email       | STRING         | Customer's email address       |

The connector handles various AWS Athena data types:
* VarChar values
* BigInt values
* Double values
* Boolean values
* Null values

## Error Handling

The connector implements the following error handling:
* Validates AWS credentials and configuration
* Monitors query execution status
* Handles pagination tokens
* Implements data type conversion
* Includes comprehensive logging
* Manages query timeouts and failures

## Additional Considerations

This example is intended for learning purposes and demonstrates AWS Athena integration. For production use, you should:

- Implement appropriate query optimization
- Add proper error retry mechanisms
- Consider implementing query result caching
- Add monitoring for query performance
- Implement cost control measures
- Consider implementing query concurrency limits
- Add proper IAM role-based authentication
- Implement proper error handling for AWS service outages
- Consider implementing query result partitioning
- Add proper cleanup of query results in S3

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team. 
