# **Cassandra Database Example**

**Connector Overview**

This connector integrates Cassandra databases with Fivetran, syncing data from Cassandra clusters to your destination. It connects to a Cassandra instance, efficiently retrieves data using pagination, and handles incremental updates based on the `created_at` timestamp column. 

The connector is designed to handle large datasets efficiently through streaming and pagination techniques, making it suitable for production environments with significant data volumes. It includes functionality for creating test environments with dummy data for development and testing purposes. 

## **Requirements**

* [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)   
* Operating System:  
  * Windows 10 or later  
  * macOS 13 (Ventura) or later

## **Getting Started**

Refer to the [Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## **Features**

- Connect to Cassandra clusters with authentication
- Incremental updates based on timestamp tracking
- Memory-efficient data processing with pagination and generators
- Checkpoint state management for reliable syncs
- Support for large datasets through pagination techniques
- Detailed logging for monitoring and troubleshooting

## **Configuration File**

The connector requires the following configuration parameters:

```
{
  "hostname": "<YOUR_CASSANDRA_HOSTNAME>",
  "username": "<YOUR_CASSANDRA_USERNAME>",
  "password": "<YOUR_CASSANDRA_PASSWORD>",
  "keyspace": "<YOUR_CASSANDRA_KEYSPACE>",
  "port": "<YOUR_CASSANDRA_PORT>"
}
```

- hostname: The Cassandra server hostname or IP address
- username: Username for authentication
- password: Password for authentication
- keyspace: The Cassandra keyspace to connect to
- port: The port number for the Cassandra server

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## **Requirements File**

The connector requires the Cassandra Python driver and dateutil for timestamp parsing:

```
cassandra-driver
python-dateutil
```

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## **Authentication**


The connector uses PlainTextAuthProvider for authentication with Cassandra. Provide the following credentials in the configuration: 

- username: A valid Cassandra user with read permissions on the specified keyspace
- password: The corresponding password for the user

## **Pagination**

The connector implements efficient pagination when retrieving data from Cassandra:  
- Uses Cassandra's native pagination capabilities with the fetch_size parameter
- Default page size is set to 100 records but can be adjusted
- Performs upserts one record at a time, avoiding excessive memory usage
- Handles checkpointing every 1000 records to maintain state during long-running syncs

## **Data Handling**

The connector processes data with the following approach:  
- Connects to the specified Cassandra keyspace
- Retrieves records incrementally based on the `created_at` timestamp
- Transforms Cassandra row objects into dictionaries for Fivetran
- Handles timezone-aware datetime objects consistently across queries and comparisons
- Uses the `ALLOW FILTERING` directive with `created_at` index for efficient querying
- Maintains state between runs by tracking the latest timestamp processed
- Delivers data with the following schema mapping:
  - id (UUID → STRING)
  - name (text → STRING)
  - created_at (timestamp → UTC_DATETIME)

## **Error Handling**

The connector implements the following error handling strategies:  
- Validates configuration parameters before attempting connection
- Provides detailed error messages for connection failures
- Handles timezone-related errors by ensuring consistent timezone awareness
- Wraps data fetching operations in try/except blocks with informative error messages
- Gracefully handles pagination issues that may occur with large datasets
- Implements regular checkpointing to minimize data loss in case of failures

## **Additional Files**

- `adding_dummy_data_to_cassandra.py`: This python file contains functions to add dummy data to the Cassandra database. It creates dummy database and table and generates random records with unique IDs and timestamps. This dummy data is inserted into the Cassandra table for testing purposes. In production, you will not need to insert dummy data, as the connector will work with your existing Cassandra database.

## **Additional Considerations**

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
