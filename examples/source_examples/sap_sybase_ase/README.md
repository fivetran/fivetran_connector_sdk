# SAP Sybase ASE Connector

## Connector overview

This connector allows you to extract data from SAP Sybase ASE databases and load it into destination using Fivetran Connector SDK. It establishes a connection to your Sybase ASE instance and replicates table data using a memory-efficient batching approach, making it suitable for both small and large datasets.

## Requirements

* [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)   
* Operating system:
  * Windows: 10 or later (64-bit only)
  * macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  * Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started

Refer to the [Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

- Connects directly to Sybase ASE databases using industry-standard ODBC driver
- Memory-efficient batch processing for handling large datasets
- Configurable batch sizes to optimize performance and memory usage
- Detailed logging for monitoring and debugging

## Configuration file

The connector requires the following configuration parameters: 

```json
{
  "server": "<YOUR_SERVER_NAME>",
  "port": "<YOUR_PORT_NUMBER>",
  "user": "<YOUR_USERNAME>",
  "password": "<YOUR_PASSWORD>",
  "database": "<YOUR_DATABASE_NAME>",
  "table_name": "<YOUR_TABLE_NAME>",
}
```

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file

This connector requires the following Python packages specified in `requirements.txt`:

```
pyodbc
```

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication

This connector uses basic database authentication with `username` and `password`. The credentials are specified in the configuration file and passed to the Sybase ASE server during connection establishment. Ensure that:

- The database user has appropriate permissions to read from the tables you want to replicate
- The network configuration allows connections from the connector runtime environment to the database server
- Credentials are kept secure and not exposed in code repositories

## Data handling

The connector processes data in memory-efficient batches (refer to `read_sybase_and_upsert` function):

- Data is read from the source table specified in the configuration.
- Records are processed in batches of a configurable size (default 1000) to minimize memory usage.
- `DateTime` objects are converted to ISO format strings.

For optimal memory usage, the connector uses a cursor-based approach with `fetchmany()` to retrieve data in controlled batches rather than loading the entire result set at once.

## Error handling

The connector implements several error handling mechanisms:

- Configuration validation (refer to `validate_configuration` function) - Checks that all required parameters are present before attempting connection
- Connection error handling - Wraps database connection attempts in try/except blocks with detailed error messages
- Graceful resource cleanup - Ensures cursors and connections are properly closed even when errors occur

## Tables Created

The connector creates a single table named `customers` with the following schema:

```json
{
  "table": "customers",
  "primary_key": ["id"],
  "columns": {
    "id": "INT",
    "customer_name": "STRING",
    "customer_email": "STRING",
    "customer_address": "STRING",
    "customer_phone": "STRING",
    "created_at": "UTC_DATETIME",
    "updated_at": "UTC_DATETIME"
  }
}
```

## Additional files

The connector includes the following additional files:

- `drivers/installation.sh`: A script to install the required ODBC driver for SAP Sybase ASE on Production environment. This script ensures that the connector can connect to the database by installing the necessary dependencies.

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
