# Delete Example with Composite Primary Key

## Connector overview

This connector demonstrates how to implement delete operations with composite primary keys using the Fivetran Connector SDK. It connects to a PostgreSQL database and shows two delete patterns:

- Deleting a single record by specifying the complete composite primary key
- Deleting multiple records by identifying each with its complete primary key

This example is particularly useful for understanding how to properly handle delete operations when working with tables that have composite primary keys.

## Requirements

* [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)   
* Operating system:
  * Windows: 10 or later (64-bit only)
  * macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  * Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started

Refer to the [Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

- Demonstrates proper handling of delete operations with composite primary keys
- Shows how to connect to and query a PostgreSQL database
- Flags common errors when handling composite primary keys incorrectly

## Configuration file

The connector requires the following configuration parameters to connect to your PostgreSQL database:

```
{
  "HOST": "<YOUR_PG_DATABASE_HOST>",
  "PORT": "<YOUR_PG_DATABASE_PORT>",
  "DATABASE": "<YOUR_PG_DATABASE_NAME>",
  "USERNAME": "<YOUR_PG_DATABASE_USERNAME>",
  "PASSWORD": "<YOUR_PG_DATABASE_PASSWORD>"
}
```

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file

The connector requires the `psycopg2` Python library to connect to PostgreSQL databases.

```
psycopg2_binary==2.9.10
```

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication

This connector uses basic database authentication with a username and password to connect to PostgreSQL. Ensure the database user has appropriate permissions to read from the `sample_table` table.

## Data handling

The connector demonstrates several data operations:

- Data retrieval from PostgreSQL using the `fetch_data` method (lines 42-54)
- Upsert operations for synchronizing all records
- Delete operations with composite primary keys

Each record is synchronized to the destination table with its complete set of fields. When deleting records, the connector demonstrates how to properly specify all components of a composite primary key.

## Error handling

The connector implements error handling in several areas:

- Database connection errors
- Data fetching errors
- Configuration validation
- General error handling during the delete process

The connector also includes warnings about improper delete operations that would fail, such as attempts to delete records with incomplete primary keys.

## Tables Created

The connector syncs the data to table `sample_table` in the destination.
This connector replicates data from the source's `sample_table` to a destination table with the same name. The table has a composite primary key consisting of `id` and `department_id`.

The schema of the table is as follows:

```json
{
  "table": "sample_table",
  "primary_key": ["id","department_id"],
  "columns": {
    "id": "INT",
    "department_id": "INT",
    "address": "STRING",
    "name": "STRING"
  }
}
```

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
