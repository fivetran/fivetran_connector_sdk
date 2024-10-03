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

# Friendly reminders

These Examples are provided to help you successfully use Fivetran's Connector SDK. We have where possible tested the code, however Fivetran is not responsible if connector SDK written based on these examples ends up having unexpected or negative consequences.

API calls made by an SDK Connector may count against your total allocation for a service and could cause you to trigger service rate-limits that impact other uses of the source’s APIs.

Using the wrong pattern for a particular service’s APIs could result in data integrity issues, review all our examples and pick the one that works for your target API - it is possible that your target API doesn’t support a pattern we currently have an example for.

Running examples will generate MAR for your Fivetran account - As with other new connectors, SDK connectors have a [trial period of 14 days where MAR is not charged](https://fivetran.com/docs/getting-started/free-trials#newconnectorfreeuseperiod), after that time MAR will be charged, please ensure the connectors you created to run these examples are paused or deleted before the 14-day timeframe is complete.