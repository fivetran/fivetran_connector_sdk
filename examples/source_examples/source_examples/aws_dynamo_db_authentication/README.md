# AWS DynamoDB Authentication Connector Example

This connector demonstrates how to integrate AWS DynamoDB with Fivetran using the Connector SDK and IAM role-based authentication. It provides an example of retrieving data from DynamoDB tables using parallel scanning and handling AWS authentication with assumed roles.

## Requirements

* [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)   
* Operating System:  
  * Windows 10 or later  
  * macOS 13 (Ventura) or later
* AWS Account with DynamoDB access
* AWS IAM credentials and role with appropriate permissions
* AWS STS (Security Token Service) access

## Getting Started

Refer to the [Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

* Demonstrates AWS DynamoDB integration using boto3
* Implements IAM role-based authentication
* Uses AWS STS for assuming roles
* Performs parallel scanning of DynamoDB tables
* Automatically discovers table schemas
* Implements efficient batch processing
* Includes state checkpointing for resumable syncs
* Provides comprehensive error handling

## Configuration File

The connector requires the following configuration parameters:

```json
{
  "AWS_ACCESS_KEY_ID": "YOUR_AWS_ACCESS_KEY_ID",
  "AWS_SECRET_ACCESS_KEY": "YOUR_AWS_SECRET_ACCESS_KEY",
  "ROLE_ARN": "YOUR_IAM_ROLE_ARN",
  "REGION": "YOUR_AWS_REGION"
}
```

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements File

The connector requires the following Python packages:

```
aws_dynamodb_parallel_scan==1.1.0
boto3==1.37.4
```

Note: The `fivetran_connector_sdk:latest` package is pre-installed in the Fivetran environment.

## Authentication

The connector implements a multi-step authentication process:

1. Initial AWS Authentication:
   * Uses AWS access key ID and secret access key
   * Creates STS client for role assumption

2. Role Assumption:
   * Assumes specified IAM role using STS
   * Obtains temporary credentials

3. DynamoDB Authentication:
   * Creates DynamoDB client using temporary credentials
   * Includes session token for API requests

## Data Handling

The connector dynamically discovers and syncs all tables in the DynamoDB instance. For each table:

* Automatically detects primary key configuration
* Performs parallel scanning with:
  * 4 parallel threads
  * 10 items per page
  * Consistent read enabled
* Handles nested attributes and list values
* Converts complex data types to strings when necessary

## Error Handling

The connector implements the following error handling:
* Validates AWS credentials and configuration
* Handles role assumption failures
* Manages DynamoDB API errors
* Includes comprehensive logging
* Implements proper error propagation
* Handles parallel scan failures

## Additional Files

* **aws_dynamodb_parallel_scan.py** – Provides parallel scanning functionality for efficient data retrieval

## Additional Considerations

This example is intended for learning purposes and demonstrates AWS DynamoDB integration. For production use, you should:

1. Implement appropriate IAM policies and roles
2. Adjust parallel scan parameters based on your data volume
3. Add proper error retry mechanisms
4. Consider implementing incremental syncs
5. Add monitoring for scan performance
6. Implement cost control measures
7. Consider implementing backup procedures
8. Add proper handling for rate limiting
9. Implement proper error handling for AWS service outages
10. Consider implementing data type validation
11. Add proper cleanup procedures
12. Consider implementing custom scan strategies

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team. 