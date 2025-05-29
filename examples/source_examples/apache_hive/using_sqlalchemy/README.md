# Apache Hive Connector Example

## Connector overview

This connector demonstrates how to fetch data from Apache Hive using `SQLAlchemy` with the `PyHive` dialect and `fivetran-connector-sdk` library. The connector is designed to handle large datasets by processing data in streaming fashion and maintaining sync state to resume from the last successful position.

## Requirements

* [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)   
* Operating system:
  * Windows: 10 or later (64-bit only)
  * macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  * Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started

Refer to the [Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

- Connection to Apache Hive data source using SQLAlchemy ORM..
- Incremental sync based on `created_at` timestamp
- Streaming data retrieval using execution options to handle large datasets efficiently.

## Configuration file

This connector requires the following configuration parameters to establish a connection to your Hive instance:

```
{
  "hostname": "YOUR_HIVE_HOSTNAME",
  "port": "<YOUR_HIVE_PORT>",
  "username": "<YOUR_HIVE_USERNAME>",
  "database": "<YOUR_HIVE_DATABASE>",
}
```

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file

Include the following dependencies in your requirements.txt file:

```
SQLAlchemy==2.0.40
PyHive==0.7.0
thrift_sasl
sasl
```
The `PyHive` is required for actual dialect implementation for Hive along with the `SQLAlchemy` ORM. The `thrift_sasl` and `sasl` packages are necessary for SASL authentication, which is commonly used with Hive.

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication

The connector supports authentication for Apache Hive through SQLAlchemy. You need to provide:  
- `hostname`: The address of your Hive server
- `port`: The port number Hive is listening on (typically 10000)
- `username`: Your Hive username
- `database`: The name of the Hive database you want to connect to

Authentication can be handled in the `create_hive_connection` function.

## Data handling

The connector performs the following data handling operations:  
- Fetching: Data is retrieved from Apache Hive using SQLAlchemy with raw SQL queries and stream options..
- Processing: The `process_row` function converts raw Hive data into dictionary format suitable for Fivetran 
  - Datetime objects are converted to ISO format with UTC timezone.
  - Column names are extracted and mapped to their values.
- Streaming: Uses SQLAlchemy's `stream_results` execution option to efficiently process large datasets without loading everything into memory.
- State Management: The connector tracks the latest created timestamp to enable incremental syncs.

## Tables Created

The connector creates a table named `people` with the following schema:

```
{
    "table": "people",
    "primary_key": ["id"],
    "columns": {
        "id": "INT",
        "name": "STRING",
        "age": "INT",
        "created_at": "UTC_DATETIME"
    },
}
```

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
