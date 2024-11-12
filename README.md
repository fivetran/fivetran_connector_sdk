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
    Quickstart examples
  </summary>

### hello
Simplest example, append-only

### local
Emulated source, without any calls out to the internet

### configuration
Shows how to use secrets

### hashes
Shows how to calculate hash of fields and use it as primary key. Useful in scenarios where the
incoming rows do not have any field suitable to be used as a Primary Key.

### user_profiles
Shows the use of Pandas DataFrames to manipulate data prior to sending to Fivetran.

### weather
A realistic example, using a public API, fetching data from NOAA
</details>

<details>
<summary>
Common patterns for connectors
</summary>

<details>
<summary>
cursors
</summary>

### multiple_tables_with_cursors
The parent-child relationship between tables from incremental API endpoints, with the complex cursor.

### marketstack
This code retrieves different stock tickers and the daily price for those tickers using marketstack API. Refer its [Docs](https://polygon.io/docs/stocks/getting-started)
</details>

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

### priority_first_sync_for_high_volume_initial_syncs
Priority-first sync, pfs for short, is very helpful for high volume historical syncs. It is a sync strategy
which prioritises fetching the most recent data first so that fresh data is ready for you to use more quickly.
This is a simple example of how you could implement the Priority-first sync strategy in a `connector.py` file for your connection.
</details>

<details>
<summary>
Source examples
</summary>

### records with no created_at
Shows how to work with records where the source does not provide a created_at(or equivalent) field.
Useful when its desired to keep track of when the record was first observed.

### multiple code files
Shows how you can write a complex connector comprising multiple .py files.

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

### accelo api connector multithreading enabled
This module implements a connector for syncing data from the Accelo API.
It handles OAuth2 authentication, rate limiting, and data synchronization for companies,
invoices, payments, prospects, jobs, and staff.
It is an example of multithreading the extraction of data from the source to improve connector performance.
Multithreading helps to make api calls in parallel to pull data faster.
It is also an example of using OAuth 2.0 client credentials flow.
Requires Accelo OAuth credentials to be passed in to work.

Refer Multithreading Guidelines in api_threading_utils.py
</details>

# Additional considerations

We provide examples to help you effectively use Fivetran's Connector SDK. While we've tested the code provided in these examples, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples.

Note that API calls made by your SDK connector may count against your service’s API call allocation. Exceeding this limit could trigger rate limits, potentially affecting other uses of the source API.

It's important to choose the right design pattern for your target API. Using an inappropriate pattern may lead to data integrity issues. Review all our examples carefully to select the one that best suits your target API. Keep in mind that some APIs may not support patterns for which we currently have examples.

As with other new connectors, SDK connectors have a [14-day trial period](https://fivetran.com/docs/getting-started/free-trials#newconnectorfreeuseperiod) during which your usage counts towards free [MAR](https://fivetran.com/docs/usage-based-pricing). After the 14-day trial period, your usage counts towards paid MAR. To avoid incurring charges, pause or delete any connectors you created to run these examples before the trial ends.

# Maintenance
This repository is actively maintained by Fivetran Developers. Please reach out to our [Support team](https://support.fivetran.com/hc/en-us) for any inquiries.
