# Sybase IQ Connector Example

## Connector overview

This connector extracts data from a Sybase IQ database using the `FreeTDS` driver and `PyODBC`. The connector demonstrates how to establish a connection to a Sybase IQ database, execute SQL queries to fetch data in batches, and efficiently upsert this data into Fivetran's destination. This example also shows how to sync the data incrementally by tracking the last synced timestamp and using it to filter new or updated records.

## Requirements

* [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)   
* Operating system:
  * Windows: 10 or later (64-bit only)
  * macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  * Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started

Refer to the [Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

- Connects to Sybase IQ database using `FreeTDS` and `PyODBC`
- Implements incremental updates based on record creation date
- Processes data in batches to optimize memory usage
- Validates configuration parameters before attempting connections
- Error handling for connection failures

## Configuration file

The connector requires the following configuration parameters:

```json
{
  "server": "<YOUR_SYBASE_IQ_SERVER>",
  "port": "<YOUR_SYBASE_IQ_PORT>",
  "database": "<YOUR_SYBASE_IQ_DATABASE>",
  "user_id": "<YOUR_SYBASE_IQ_USER_ID>",
  "password": "<YOUR_SYBASE_IQ_PASSWORD>"
}
```

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file

This connector requires the `PyODBC` library to connect to Sybase IQ databases.

```
pyodbc==5.2.0
```

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication

The connector uses basic database authentication with a `username` and `password`. These credentials are specified in the configuration file and used to establish a connection to the Sybase IQ database.

To obtain credentials for your Sybase IQ database:

1. Contact your database administrator for the relevant access credentials.
2. Ensure your database user has appropriate read permissions to the required tables.

Refer to `create_sybase_connection()` function for implementation details.

## Pagination

The connector uses a batch-based approach for data retrieval rather than traditional API pagination. Data is fetched in configurable batches (default: 1000 rows) using the `cursor.fetchmany()` method.

This approach accomplishes the following:

1. Prevents memory overflow when handling large datasets.
2. Enables incremental processing of data without loading the entire result set
3. Allows for checkpointing progress after each batch

Refer to the `fetch_and_upsert()` function, specifically the `cursor.fetchmany(batch_size)` implementation.

## Data handling

The connector processes data as follows:

1. Defines a schema for the `customers` table with specific data types.
2. Fetches data incrementally based on the `created_date` field.
3. Processes data in configurable batch sizes (default: 1000 rows). This prevents memory overflow errors when syncing large datasets.
4. Uses checkpoints to save progress during synchronization. This allows the connector to resume from the last synced record in case of interruptions.

Refer to `fetch_and_upsert()` function for data processing implementation.

## Error handling

The connector implements error handling in several key areas:

1. Configuration validation ensures all required parameters are present.
2. Connection errors are caught and raised with meaningful error messages.
3. Resource cleanup is handled properly even if exceptions occur

## Tables created

This connector replicates the `customer` table which contains customer information with the following schema:

- `customer_id` (INT) - Primary key
- `created_date` (NAIVE_DATE) - Date when the customer was created
- `first_name` (STRING) - Customer's first name
- `last_name` (STRING) - Customer's last name
- `email` (STRING) - Customer's email address
- `status` (STRING) - Customer's status (e.g., active, inactive)

## Additional files

This connector includes the following additional files:

- `drivers/installation.sh` : This script installs the `FreeTDS` driver required for connecting to Sybase IQ database.

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.