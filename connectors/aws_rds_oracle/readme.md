# AWS RDS Oracle Connector Example

## Connector Overview

This connector demonstrates how to sync records from an AWS RDS Oracle database using the Fivetran Connector SDK.  
It establishes a connection to an Oracle instance and incrementally extracts table data based on an update timestamp, transforming the records into a standardized format for loading into the destination.

---

## Requirements

**Supported Python versions:**  
- Python 3.8 or later

**Operating system:**  
- Windows: 10 or later (64-bit only)  
- macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])  
- Linux: Ubuntu 20.04 or later, Debian 10 or later, Amazon Linux 2 or later (arm64 or x86_64)

---

## Getting Started

Refer to the Setup Guide below to get started.

---

## Features

- Direct connection to AWS RDS Oracle database
- Incremental data extraction based on timestamp column
- Support for primary key identification
- Easy configuration via JSON file

---

## Configuration File

The connector requires the following configuration parameters in `configuration.json`:

```json
{
  "host": "<YOUR_RDS_ORACLE_HOST>",
  "port": "1521",
  "service_name": "<YOUR_SERVICE_NAME>",
  "user": "<YOUR_DB_USERNAME>",
  "password": "<YOUR_DB_PASSWORD>"
}
```

**Note:**  
Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

---

## Requirements File

The connector requires the `oracledb` package to connect to Oracle databases.

```
oracledb==3.3.0
```

**Note:**  
The `fivetran_connector_sdk` and `requests` packages are pre-installed in the Fivetran environment.  
To avoid dependency conflicts, do not declare them in your `requirements.txt`.

---

## Authentication

This connector uses username and password authentication to connect to Oracle.  
The credentials are specified in the configuration file and passed to the `oracledb.connect()` function (see the `connect_oracle()` function).

**To set up authentication:**
- Create an Oracle user with appropriate permissions to access the required tables.
- Provide the username and password in the `configuration.json` file.
- Ensure the user has `SELECT` permissions on the tables that need to be synced.

---

## Data Handling

The connector handles data as follows (see the `update()` function):

1. Connects to the Oracle database using the provided configuration.
2. Retrieves the last sync timestamp from state (or uses a default date for initial sync).
3. Executes a SQL query to fetch records modified after the last sync timestamp.
4. Transforms each database record into the target schema format.
5. Performs upsert operations for each record.
6. Maintains state with the latest processed timestamp.

---

## Tables Synced

The connector is configured to sync the following table (see the `TABLES` list in `connector.py`):

```json
{
  "table": "FIVETRAN_LOGMINER_TEST",
  "primary_key": ["ID"],
  "columns": {
    "ID": "INT",
    "NAME": "STRING",
    "LAST_UPDATED": "UTC_DATETIME"
  }
}
```

You can add more tables to the `TABLES` list in `connector.py` as needed.

---

## Additional Considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK.  
While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples.  
For inquiries, please reach out to our Support team.

---