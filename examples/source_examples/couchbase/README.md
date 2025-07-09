# Couchbase Connector Example

## Connector overview


This connector example demonstrates how to sync data from Couchbase Capella using the Connector SDK. It connects to a Couchbase Capella instance, executes SQL++ (N1QL) queries to fetch data from a specific bucket, scope, and collection, and efficiently streams the data to destination table while implementing best practices for handling large datasets.

## Requirements

* [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)   
* Operating system:
  * Windows: 10 or later (64-bit only)
  * macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  * Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started

Refer to the [Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

- Connect to Couchbase Capella instances using secure authentication
- Execute SQL++ (N1QL) queries against Couchbase collections
- Stream data efficiently to handle large datasets. This prevents any potential memory overflow errors.
- Implement regular checkpointing
- Configure WAN development profile to handle latency issues when accessing Capella from different networks

## Configuration file

The connector requires the following configuration parameters to connect to your Couchbase Capella instance:

```
{
    "username": "YOUR_COUCHBASE_USERNAME",
    "password": "YOUR_COUCHBASE_PASSWORD",
    "endpoint": "YOUR_COUCHBASE_ENDPOINT",
    "bucket_name": "YOUR_COUCHBASE_BUCKET_NAME",
    "scope": "YOUR_COUCHBASE_SCOPE_NAME",
    "collection": "YOUR_COUCHBASE_COLLECTION_NAME"
}
```

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file

The connector requires the Couchbase Python SDK:

```
couchbase==4.3.6
```

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication

The connector authenticates with Couchbase using a username and password authentication mechanism. These credentials are provided in the configuration file and used to create a `PasswordAuthenticator` object for establishing a secure connection to the Couchbase cluster.

## Data handling

The connector handles data processing through the following steps:  
- Establishes a connection to the Couchbase cluster using the provided credentials.
- Executes a `SQL++` query against the specified collection.
- Streams the query results to avoid loading the entire dataset into memory
- Uses the `op.upsert()` method to sync data using Fivetran connector SDK.
- Implements checkpointing every 1000 records to ensure sync progress is saved in case of any interruptions.

## Error handling

The connector implements error handling in several critical functions:  
- In `create_couchbase_client`: Catches any exception during cluster connection and raises a meaningful error message
- In `execute_query_and_upsert`: Catches exceptions during query execution and data processing, raising descriptive runtime errors

## Tables Created

The `schema()` function defines the structure of the destination table:

```
{
    "table": "airline_table",
    "primary_key": ["id"],
    "columns": {
        "id": "INT",
        "name": "STRING",
        "country": "STRING",
        "type": "STRING",
        "callsign": "STRING",
        "iata": "STRING",
        "icao": "STRING",
    },
}
```
The table contains `airline` information from the Couchbase `travel-sample` bucket.


## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
