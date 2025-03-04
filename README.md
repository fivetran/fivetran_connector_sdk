# Requirements
- Python ≥3.9 and ≤3.12
- Operating System:
  - Windows 10 or later
  - MacOS 13 (Ventura) or later

# Getting started

See [Setup guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

# Examples
There are several examples available under `/examples`:

<details>
  <summary>
    Quickstart examples
  </summary>

### hello
This is the simplest, append-only example.

### simple_three_step_cursor
This is an emulated source, without any calls out to the internet.

### configuration
This example shows how to use secrets.

### multiple_code_files
This example shows how you can write a complex connector comprising multiple `.py` files.

### object_oriented_programming_approach
This example shows an Object Oriented Programming Approach while building the connector for National Parks Service API.

### using_pd_dataframes
This example shows the use of Pandas DataFrames to manipulate data prior to sending to Fivetran.

### large_data_set
This example shows how to handle the large data from API response with pagination and without pagination. 

### weather
This is a realistic example, using a public API, fetching data from NOAA.

### complex_configuration_options
Shows how to cast configuration field to list, integer, boolean and dict for use in connector code.
</details>

<details>
<summary>
Common patterns for connectors
</summary>

<details>
<summary>
Authentication
</summary>

### oauth2_with_token_refresh
It is an example of using OAuth 2.0 client credentials flow, and the refresh of Access token from the provided refresh token.

Refer to the OAuth Refresh flow in `readme.md`.
</details>

<details>
<summary>
Cursors
</summary>

### marketstack
This code retrieves different stock tickers and the daily price for those tickers using Marketstack API. Refer to Marketstack's [documentation](https://polygon.io/docs/stocks/getting-started).
</details>

### multiple_tables_with_cursors
The parent-child relationship between tables from incremental API endpoints, with the complex cursor.

### pagination
This is a simple pagination example template set for the following types of paginations:
- keyset
- next_page_url
- offset_based
- page_number

### export
This example consumes the export data from REST API and syncs it via Connector SDK, for the following response types:
- csv

### specified_types
This example declares a schema and upserts all data types.

### unspecified_types
This example upserts all data types without specifying a schema.

### three_operations
This example shows how to use upsert, update and delete operations.

### records_with_no_created_at_timestamp
This example shows how to work with records where the source does not provide a `created_at` (or equivalent) field.
It is useful when it's desired to keep track of when the record was first observed.

### hashes
This example shows how to calculate a hash of fields and use it as primary key. It is useful in scenarios where the incoming rows do not have any field suitable to be used as a primary key.

### priority_first_sync_for_high_volume_initial_syncs
A priority-first sync (pfs), is very helpful for high-volume historical syncs. It is a sync strategy that prioritises fetching the most recent data first so that fresh data is ready for you to use more quickly.
This is a simple example of how you could implement the Priority-first sync strategy in a `connector.py` file for your connection.
</details>

<details>
<summary>
Source examples
</summary>

### aws_dynamo_db_authentication
This example shows how to authenticate to AWS using the IAM role credentials and use them to sync records from DynamoDB. Boto3 package is used to create an AWS client. Refer to the [Boto3 documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html).

### redshift
This is an example to show how to sync records from Redshift by using Connector SDK. You need to provide your Redshift credentials for this example to work.

### key_based_replication
This example shows key-based replication from database sources. Replication keys are columns that are used to identify new and updated data for replication. When you set a table to use Incremental Replication, you’ll also need to define a replication key for that table.

### oauth2_and_accelo_api_connector_multithreading_enabled
This module implements a connector for syncing data from the Accelo API. It uses **OAuth 2.0 Client Credentials flow** authentication, rate limiting, and data synchronization for companies, invoices, payments, prospects, jobs, and staff. This is an example of multithreading used in the extraction of data from the source to improve connector performance. Multithreading allows to make API calls in parallel to pull data faster. This is also an example of using **OAuth 2.0 Client Credentials** flow. You need to provide your Accelo OAuth credentials for this example to work.

Refer to the Multithreading Guidelines in `api_threading_utils.py`.

### smartsheets
This is an example of how we can sync Smartsheets sheets by using Connector SDK. You need to provide your Smartsheets api_key for this example to work.

### sql_server
This example uses pyodbc to connect to SQL Server Db for querying/syncing data using Connector SDK. You need to provide your SQL Server Db credentials for this example to work.

### aws_athena
This is an example of how we can sync data from AWS Athena by using Connector SDK. We have two examples, one utilises Boto3 and another utilizes SQLAlchemy with PyAthena.
You can use either, based on your requirements. You need to provide your AWS Athena credentials for this example to work.

### toast
This is an example of how we can sync Toast data using the Connector SDK. You would need to provide your Toast credentials for this example to work.

</details>

# Additional considerations

We provide examples to help you effectively use Fivetran's Connector SDK. While we've tested the code provided in these examples, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples.

Note that API calls made by your Connector SDK connection may count towards your service’s API call allocation. Exceeding this limit could trigger rate limits, potentially impacting other uses of the source API.

It's important to choose the right design pattern for your target API. Using an inappropriate pattern may lead to data integrity issues. We recommend that you review all our examples carefully to select the one that best suits your target API. Keep in mind that some APIs may not support patterns for which we currently have examples.

As with other new connectors, SDK connectors have a [14-day trial period](https://fivetran.com/docs/getting-started/free-trials#newconnectorfreeuseperiod) during which your usage counts towards free [MAR](https://fivetran.com/docs/usage-based-pricing). After the 14-day trial period, your usage counts towards paid MAR. To avoid incurring charges, pause or delete any connections you created to run these examples before the trial ends.

# Maintenance
This repository is actively maintained by Fivetran Developers. Reach out to our [Support team](https://support.fivetran.com/hc/en-us) for any inquiries.
