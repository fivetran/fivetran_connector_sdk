# Requirements
- Python ≥3.9 and ≤3.12
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

### Hello
This is the simplest, append-only example. 

### Local
This is an emulated source, without any calls out to the internet.

### Configuration
This example shows how to use secrets.

### Hashes
This example shows how to calculate a hash of fields and use it as primary key. It is useful in scenarios where the incoming rows do not have any field suitable to be used as a primary key.

### User_profiles
This example shows the use of Pandas DataFrames to manipulate data prior to sending to Fivetran.

### Weather
This is a realistic example, using a public API, fetching data from NOAA.
</details>

<details>
<summary>
Common patterns for connectors
</summary>

<details>
<summary>
Cursors
</summary>

### Multiple_tables_with_cursors
The parent-child relationship between tables from incremental API endpoints, with the complex cursor.

### Marketstack
This code retrieves different stock tickers and the daily price for those tickers using Marketstack API. Refer to Marketstsck's [documentation](https://polygon.io/docs/stocks/getting-started)
</details>

### Pagination
This is a simple pagination example template set for the following types of paginations:
- keyset
- next_page_url
- offset_based
- page_number

### Specified_types
This example declares a schema and upserts all data types.

### Unspecified_types
This example upserts all data types without specifying a schema.

### Three_operations
This example shows how to use upsert, update and delete operations.

### Priority_first_sync_for_high_volume_initial_syncs
A priority-first sync, pfs for short, is very helpful for high volume historical syncs. It is a sync strategy that prioritises fetching the most recent data first so that fresh data is ready for you to use more quickly.
This is a simple example of how you could implement the Priority-first sync strategy in a `connector.py` file for your connection.
</details>

<details>
<summary>
Source examples
</summary>

### Records with no created_at
This example shows how to work with records where the source does not provide a `created_at` (or equivalent) field.
It is useful when it's desired to keep track of when the record was first observed.

### Multiple code files
This example shows how you can write a complex connector comprising multiple `.py` files.

### Aws dynamo db authentication
This example shows how to authenticate to AWS using the IAM role credentials and use them to sync records from DynamoDB. Boto3 package is used to create an AWS client. Refer to the [Boto3 documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html).

### Redshift
This is an example to show how to sync records from Redshift by using Connector SDK. You need to provide your Redshift credentials for this example to work.

### Key-based replication
This example shows key-based replication from database sources. Replication keys are columns that are used to identify new and updated data for replication. When you set a table to use Incremental Replication, you’ll also need to define a replication key for that table.

### Accelo API connector multithreading enabled
This module implements a connector for syncing data from the Accelo API. It handles OAuth2 authentication, rate limiting, and data synchronization for companies,
invoices, payments, prospects, jobs, and staff. This is an example of multithreading used in the extraction of data from the source to improve connector performance. Multithreading helps to make API calls in parallel to pull data faster. This is also an example of using OAuth 2.0 client credentials flow. You need to provide your Accelo OAuth credentials for this example to work.

Refer to the Multithreading Guidelines in `api_threading_utils.py`.

### Smartsheets
This is an example of how we can sync Smartsheets sheets by using Connector SDK. You need to provide your Smartsheets api_key for this example to work.

### AWS Athena
This is an example of how we can sync data from AWS Athena by using Connector SDK. We have two examples, one utilises Boto3 and another utilizes SQLAlchemy with PyAthena. 
You can use either, based on your requirements. You need to provide your AWS Athena credentials for this example to work.

</details>

# Additional considerations

We provide examples to help you effectively use Fivetran's Connector SDK. While we've tested the code provided in these examples, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples.

Note that API calls made by your Connector SDK connection may count towards your service’s API call allocation. Exceeding this limit could trigger rate limits, potentially impacting other uses of the source API.

It's important to choose the right design pattern for your target API. Using an inappropriate pattern may lead to data integrity issues. We recommend that you review all our examples carefully to select the one that best suits your target API. Keep in mind that some APIs may not support patterns for which we currently have examples.

As with other new connectors, SDK connectors have a [14-day trial period](https://fivetran.com/docs/getting-started/free-trials#newconnectorfreeuseperiod) during which your usage counts towards free [MAR](https://fivetran.com/docs/usage-based-pricing). After the 14-day trial period, your usage counts towards paid MAR. To avoid incurring charges, pause or delete any connections you created to run these examples before the trial ends.

# Maintenance
This repository is actively maintained by Fivetran Developers. Reach out to our [Support team](https://support.fivetran.com/hc/en-us) for any inquiries.
