# Requirements
- Python ≥3.9 and ≤3.11
- Operating System:
  - Windows 10 or later
  - MacOS 13 (Ventura) or later

# Getting started
See [Quickstart guide](https://fivetran.com/docs/connectors/connector-sdk/quickstart-guide) to get started.

# Examples
There are several examples available under `/examples`:

<details>
  <summary>
    Quick start examples
  </summary>

### hello
Simplest example, append-only

### local
Emulated source, without any calls out to the internet

### configuration
Shows how to use secrets

### user_profiles
Shows the use of Pandas DataFrames to manipulate data prior to sending to Fivetran.

### weather
A realistic example, using a public API, fetching data from NOAA
</details>

<details>
<summary>
Common patterns for connectors
</summary>

### multiple_tables_with_cursors
The parent-child relationship between tables from incremental API endpoints, with the complex cursor.

### pagination
Simple pagination example templates for the following types of paginations:
- keyset
- next_page_url
- offset_based
- page_number

### specified_types
Declares a schema and upserts all data types

### unspecified_types
Upserts all data types without specifying a schema

### three_operations
Shows how to use upsert, update and delete operations
</details>

<details>
<summary>
Database source examples
</summary>

### records with no created_at
Shows how to work with records where the source does not provide a created_at(or equivalent) field.
Useful when its desired to keep track of when the record was first observed.

### aws dynamo db authentication
Shows how to authenticate to aws using IAM role credentials and use it to sync records from dynamodb
boto3 package is used to create aws client. Refer its [Docs](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html)

### redshift
This is an example to show how we can sync records from redshift DB via Connector SDK.
You would need to provide your redshift credentials for this example to work.

### key based replication
This shows key based replication from DB sources.
Replication keys are columns that are used to identify new and updated data for replication.
When you set a table to use Incremental Replication, you’ll also need to define a replication key for that table.
</details>