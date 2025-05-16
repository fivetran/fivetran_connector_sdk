# Amazon S3 CSV Validation Connector Example

This connector demonstrates how to integrate Amazon S3 with Fivetran using the Connector SDK, featuring comprehensive data validation for CSV files. It provides an example of reading CSV data from S3 buckets, validating various data types, and syncing the validated data to your destination.

## Requirements

* [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)   
* Operating System:  
  * Windows 10 or later  
  * macOS 13 (Ventura) or later
* AWS account with S3 access
* AWS credentials with appropriate permissions
* S3 bucket containing CSV files
* CSV files with specific column structure

## Getting Started

Refer to the [Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

* Demonstrates Amazon S3 integration
* Implements comprehensive data validation
* Supports multiple data types:
  * Integers
  * Longs
  * Booleans
  * Strings
  * JSON
  * Dates
  * Timestamps
* Provides detailed error reporting
* Includes schema definition
* Uses pandas for efficient CSV processing
* Implements proper AWS authentication
* Handles data type conversions

## Configuration File

The connector requires the following configuration parameters:

```json
{
  "aws_access_key_id": "YOUR_AWS_ACCESS_KEY_ID",
  "aws_secret_access_key": "YOUR_AWS_SECRET_ACCESS_KEY",
  "region_name": "YOUR_AWS_REGION_NAME",
  "bucket_name": "YOUR_AWS_BUCKET_NAME",
  "file_key": "YOUR_AWS_FILE_KEY"
}
```

* `aws_access_key_id`: AWS access key ID
* `aws_secret_access_key`: AWS secret access key
* `region_name`: AWS region where the S3 bucket is located
* `bucket_name`: Name of the S3 bucket containing the CSV file
* `file_key`: Path to the CSV file within the bucket

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements File

The connector requires the following Python packages:

```
pandas
boto3
```

Note: The `fivetran_connector_sdk:latest` package is pre-installed in the Fivetran environment.

## Data Handling

The connector syncs the following table:

### data Table
| Column                 | Type           | Description                    |
|-----------------------|----------------|--------------------------------|
| int_value             | INTEGER        | Primary key                    |
| long_value            | LONG           | Long integer value             |
| bool_value            | BOOLEAN        | Boolean value                  |
| string_value          | STRING         | String value                   |
| json_value            | JSON           | JSON structured data           |
| naive_date_value      | NAIVE_DATE     | Date without timezone         |
| naive_date_time_value | NAIVE_DATETIME | Timestamp without timezone    |

The connector implements the following data validation features:
* Integer validation
* Long integer validation
* Boolean validation (true/false strings)
* String non-empty validation
* JSON structure validation
* Date format validation (YYYY-MM-DD)
* Timestamp format validation (YYYY-MM-DD HH:MM:SS)
* Detailed error reporting for invalid data
* Row-level validation and skipping

## Error Handling

The connector implements the following error handling:
* AWS connection error handling
* S3 access error handling
* CSV parsing error handling
* Data type validation errors
* Invalid format handling
* Row-level error reporting
* Proper error logging
* Invalid data skipping

## Additional Considerations

This example is intended for learning purposes and demonstrates S3 CSV validation. For production use, you should:

- Implement appropriate error retry mechanisms
- Add S3 connection pooling
- Optimize CSV reading for large files
- Add monitoring for sync performance
- Implement proper logging strategy
- Consider implementing custom validation rules
- Add proper handling for S3 timeouts
- Consider implementing data transformation
- Add proper cleanup procedures
- Consider implementing batch processing
- Implement proper error notification system
- Consider implementing file pattern matching

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team. 
