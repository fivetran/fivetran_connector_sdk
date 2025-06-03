# IBM DB2 Connector Example

## Connector overview

This connector allows you to sync data from IBM DB2 to a destination using the Fivetran Connector SDK. The IBM DB2 connector establishes a connection to your DB2 database, reads data from tables, and incrementally syncs changes using timestamp-based tracking. This example connector demonstrates extracting employee data but can be modified to work with any DB2 tables.

## Requirements

* [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)   
* Operating system:
  * Windows: 10 or later (64-bit only)
  * macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  * Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started

Refer to the [Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

- Connects to IBM DB2 databases with secure authentication
- Supports incremental data sync using timestamp-based tracking
- Efficient row-level extraction with state management for resumable syncs
- Handles various DB2 data types

## Configuration file

The example connector uses a `configuration.json` file to define the connection parameters for the IBM DB2 database. The connection parameters required for the example to work are:

```json
{
    "hostname" : "<YOUR_DB2_HOSTNAME>",
    "port" : "<YOUR_DB2_PORT>",
    "database" : "<YOUR_DB2_DATABASE_NAME>",
    "user_id" : "<YOUR_DB2_USER_ID>",
    "password" : "<YOUR_DB2_PASSWORD>",
    "schema_name" : "<YOUR_DB2_SCHEMA>",
    "table_name" : "<YOUR_DB2_TABLE_NAME>"
}
```

The configuration parameters are:
- `hostname`: The hostname of your IBM DB2 server 
- `port`: The port number for the DB2 connection
- `database`: The name of the DB2 database
- `user_id`: The username to authenticate with DB2
- `password`: The password to authenticate with DB2
- `schema_name`: The schema name in the DB2 database where the table resides
- `table_name`: The name of the table to sync data from

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file

The `requirements.txt` file specifies the Python libraries required by the connector. The content of `requirements.txt` required is:

```
ibm_db==3.2.6
```

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication

The connector uses direct database authentication with a `username` and `password`. These credentials are specified in the configuration file and used to establish a secure connection to the IBM DB2 database. The connection is created using the IBM DB Python driver, which handles the authentication process.

## Data handling

The connector performs the following data handling operations:  
- Establishes a connection to the IBM DB2 database using the provided credentials
- Tracks the last synced timestamp to enable incremental updates
- Queries tables with a timestamp filter to retrieve only changed records
- Handles various DB2 data types and converts them to appropriate formats

## Error handling

The connector implements the following error handling strategies:  
- Connection failures: Catches database connection errors and raises informative messages
- Configuration validation: Checks for required configuration parameters before attempting connections
- State management: Checkpoints sync progress to enable resumable operations
- Logging: Uses the SDK logging framework to provide detailed information about operations and errors

## Tables Created

The example connector creates and syncs the following table:  
- employee: A sample employee table containing data about employees with various fields demonstrating different data types

Schema definition from connector:

```json
{
    "table": "employee",
    "primary_key": ["id"],
    "columns":{
        "id": "INT",
        "first_name": "STRING",
        "last_name": "STRING",
        "email": "STRING",
        "department": "STRING",
        "hire_date": "NAIVE_DATE"
    }
}
```

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
