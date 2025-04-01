<p align="center">
  <a href="https://www.fivetran.com/">
    <img src="https://cdn.prod.website-files.com/6130fa1501794ed4d11867ba/63d9599008ad50523f8ce26a_logo.svg" alt="Fivetran">
  </a>
</p>

<p align="center">
  Fivetran Connector SDK allows Real-time, efficient data replication to your destination of choice.
</p>

<p align="center">
  <a href="https://github.com/fivetran/fivetran_connector_sdk/stargazers" target="_blank"><img src="https://img.shields.io/github/stars/fivetran/fivetran_connector_sdk?style=social&label=Star"></a>
  <a href="https://github.com/fivetran/fivetran_connector_sdk/blob/main/LICENSE" target="_blank"><img src="https://img.shields.io/badge/License-MIT-blue" alt="License"></a>
  <a href="https://pypi.org/project/fivetran-connector-sdk/" target="_blank"><img src="https://img.shields.io/pypi/v/fivetran-connector-sdk" alt="PyPI Release"></a>
  <a href="https://pepy.tech/project/fivetran-connector-sdk" target="_blank"><img src="https://static.pepy.tech/badge/fivetran-connector-sdk" alt="PyPI Downloads"></a>
<a href="https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md" target="_blank"><img src="https://img.shields.io/badge/Managed-Yes-green/" alt="Managed"></a>
</p>

# Overview
Explore practical examples and helpful resources for building custom data connectors with the Fivetran [Connector SDK](https://fivetran.com/docs/connectors/connector-sdk). Learn how to develop and deploy [custom data connectors](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) in Python, and extend Fivetran’s capabilities to fit your data integration needs.

You’ll also find tips on [using AI to help you code an SDK connector](https://github.com/fivetran/fivetran_connector_sdk/tree/main/ai_and_connector_sdk/README.md) quickly.

## Why Connector SDK?
Fivetran Connector SDK allows you to code a custom data connector using Python and deploy it as an extension of Fivetran. Fivetran automatically manages running Connector SDK connections on your scheduled frequency and manages the required compute resources, eliminating the need for a third-party provider.

Connector SDK provides native support for many Fivetran features and relies on existing Fivetran technology. It also eliminates timeout and data size limitations seen in AWS Lambda.

## Requirements
- Python ≥3.9 and ≤3.12
- Operating System:
  - Windows 10 or later
  - MacOS 13 (Ventura) or later

## Getting started

See [Setup guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Examples
There are several examples available under `/examples`:

<details class="details-heading" open="open">
<summary><h3>Quickstart Examples</h3></summary>

- [hello](https://github.com/fivetran/fivetran_connector_sdk/tree/main/examples/quickstart_examples/hello) - This is the simplest, append-only example.

- [simple_three_step_cursor](https://github.com/fivetran/fivetran_connector_sdk/tree/main/examples/quickstart_examples/simple_three_step_cursor) - This is an emulated source, without any calls out to the internet.

- [configuration](https://github.com/fivetran/fivetran_connector_sdk/tree/main/examples/quickstart_examples/configuration) - This example shows how to use secrets.

- [multiple_code_files](https://github.com/fivetran/fivetran_connector_sdk/tree/main/examples/quickstart_examples/multiple_code_files) - This example shows how you can write a complex connector comprising multiple `.py` files.

- [using_pd_dataframes](https://github.com/fivetran/fivetran_connector_sdk/tree/main/examples/quickstart_examples/using_pd_dataframes) - This example shows the use of Pandas DataFrames to manipulate data prior to sending to Fivetran.

- [large_data_set](https://github.com/fivetran/fivetran_connector_sdk/tree/main/examples/quickstart_examples/large_data_set) - This example shows how to handle large data from API responses with pagination and without pagination.

- [weather](https://github.com/fivetran/fivetran_connector_sdk/tree/main/examples/quickstart_examples/weather) - This is a realistic example, using a public API, fetching data from NOAA.

- [complex_configuration_options](https://github.com/fivetran/fivetran_connector_sdk/tree/main/examples/quickstart_examples/complex_configuration_options) - This example shows how to cast configuration fields to LIST, INTEGER, BOOLEAN, and DICT for use in connector code.

- [base_64_encoding_decoding](https://github.com/fivetran/fivetran_connector_sdk/tree/main/examples/quickstart_examples/base_64_encoding_decoding) - This example shows how to use base64 encoding and decoding in your connector code.

</details>

<details class="details-heading" open="open">
<summary><h3>Common Patterns for Connectors</h3></summary>

- Authentication
  - [api_key](https://github.com/fivetran/fivetran_connector_sdk/tree/main/examples/common_patterns_for_connectors/authentication/api_key) - This is a simple example of how to work with API Key authentication for a REST API.
  - [certificate](https://github.com/fivetran/fivetran_connector_sdk/tree/main/examples/common_patterns_for_connectors/authentication/certificate) 
    - [using_base64_encoded_certificate](https://github.com/fivetran/fivetran_connector_sdk/tree/main/examples/common_patterns_for_connectors/authentication/certificate/using_base64_encoded_certificate) - It is an example of using base64-encoded strings for certificate-based authentication. The script includes functions to decode the certificate and key and use them to authenticate API requests.
  - [retrieve_from_aws](https://github.com/fivetran/fivetran_connector_sdk/tree/main/examples/common_patterns_for_connectors/authentication/certificate/retrieve_from_aws) - It is an example of how to retrieve the certificate from AWS S3 bucket and use it for certificate-based authentication.
  - [http_basic](https://github.com/fivetran/fivetran_connector_sdk/tree/main/examples/common_patterns_for_connectors/authentication/http_basic) - This is a simple example of how to work with HTTP BASIC authentication for a REST API.
  - [http_bearer](https://github.com/fivetran/fivetran_connector_sdk/tree/main/examples/common_patterns_for_connectors/authentication/http_bearer) - This is a simple example of how to work with HTTP BEARER authentication for a REST API.
  - [oauth2_with_token_refresh](https://github.com/fivetran/fivetran_connector_sdk/tree/main/examples/common_patterns_for_connectors/authentication/oauth2_with_token_refresh) - It is an example of using OAuth 2.0 client credentials flow, and the refresh of Access token from the provided refresh token. Refer to the OAuth Refresh flow in its `readme.md`.
  - [session_token](https://github.com/fivetran/fivetran_connector_sdk/tree/main/examples/common_patterns_for_connectors/authentication/session_token) - This is a simple example of how to work with Session Token authentication for a REST API.

- Cursors
  - [marketstack](https://github.com/fivetran/fivetran_connector_sdk/tree/main/examples/common_patterns_for_connectors/cursors/marketstack) - This code retrieves different stock tickers and the daily price for those tickers using Marketstack API. Refer to Marketstack's [documentation](https://polygon.io/docs/stocks/getting-started).
  - [multiple_tables_with_cursors](https://github.com/fivetran/fivetran_connector_sdk/tree/main/examples/common_patterns_for_connectors/cursors/multiple_tables_with_cursors) - The parent-child relationship between tables from incremental API endpoints, with the complex cursor.

- Export
  - [csv](https://github.com/fivetran/fivetran_connector_sdk/tree/main/examples/common_patterns_for_connectors/export/csv) - This is a simple example of how to work with .CSV file response for a REST API of export type.

- [gpg_private_keys](https://github.com/fivetran/fivetran_connector_sdk/tree/main/examples/common_patterns_for_connectors/gpg_private_keys) - This example shows how to use GPG private keys to sign data.

- [hashes](https://github.com/fivetran/fivetran_connector_sdk/tree/main/examples/common_patterns_for_connectors/hashes) - This example shows how to calculate a hash of fields to be used as primary key. This is useful in scenarios where the incoming rows do not have any field suitable to be used as a Primary Key.

- Pagination
  - [keyset](https://github.com/fivetran/fivetran_connector_sdk/tree/main/examples/common_patterns_for_connectors/pagination/keyset) - This is a simple example of how to work with key-based pagination for a REST API.
  - [next_page_url](https://github.com/fivetran/fivetran_connector_sdk/tree/main/examples/common_patterns_for_connectors/pagination/next_page_url) - This is a simple example for how to work with next-page-url pagination for a REST API.
  - [offset_based](https://github.com/fivetran/fivetran_connector_sdk/tree/main/examples/common_patterns_for_connectors/pagination/offset_based) - This is a simple example of how to work with offset-based pagination for a REST API.
  - [page_number](https://github.com/fivetran/fivetran_connector_sdk/tree/main/examples/common_patterns_for_connectors/pagination/page_number) - This is a simple example for how to work with page-number-based pagination for a REST API.

- [priority_first_sync_for_high_volume_initial_syncs](https://github.com/fivetran/fivetran_connector_sdk/tree/main/examples/common_patterns_for_connectors/priority_first_sync_for_high_volume_initial_syncs) - A priority-first sync (PFS), is very helpful for high-volume historical syncs. It is a sync strategy that prioritises fetching the most recent data first so that fresh data is ready for you to use more quickly. This is a simple example of how you could implement the priority-first sync strategy in a `connector.py` file for your connection.
- [records_with_no_created_at_timestamp](https://github.com/fivetran/fivetran_connector_sdk/tree/main/examples/common_patterns_for_connectors/records_with_no_created_at_timestamp) - This example shows how to work with records where the source does not provide a `created_at` (or equivalent) field. It is useful when it's desired to keep track of when the record was first observed.
- [schema_from_database](https://github.com/fivetran/fivetran_connector_sdk/tree/main/examples/common_patterns_for_connectors/schema_from_database) - This example shows how to extract tables (columns, data types, etc.) from a schema present in Snowflake database and use this to generate the connector schema. This approach ensures that the tables in your connector match those in your source database without having to manually define each field.
- [specified_types](https://github.com/fivetran/fivetran_connector_sdk/tree/main/examples/common_patterns_for_connectors/specified_types) - This example declares a schema and upserts all data types.
- [three_operations](https://github.com/fivetran/fivetran_connector_sdk/tree/main/examples/common_patterns_for_connectors/three_operations) - This example shows how to use upsert, update and delete operations.
- [unspecified_types](https://github.com/fivetran/fivetran_connector_sdk/tree/main/examples/common_patterns_for_connectors/unspecified_types) - This example upserts all data types without specifying a schema.
- [update_and_delete](https://github.com/fivetran/fivetran_connector_sdk/tree/main/examples/common_patterns_for_connectors/update_and_delete) - This example shows how to handle composite primary keys while using update and delete operations with a PostgreSQL database as the data source.

</details>

<details class="details-heading" open="open">
<summary><h3>Source Examples</h3></summary>

- [aws_athena/using_boto3](https://github.com/fivetran/fivetran_connector_sdk/tree/main/examples/source_examples/aws_athena/using_boto3) - This is an example of how we can sync data from AWS Athena by using Connector SDK using Boto3.
- [aws_athena/using_sqlalchemy](https://github.com/fivetran/fivetran_connector_sdk/tree/main/examples/source_examples/aws_athena/using_sqlalchemy) - This is an example of how we can sync data from AWS Athena by using Connector SDK using SQLAlchemy with PyAthena.
- [aws_dynamo_db_authentication](https://github.com/fivetran/fivetran_connector_sdk/tree/main/examples/source_examples/aws_dynamo_db_authentication) - This is an example of how we can connect and sync data from AWS DynamoDB by using Connector SDK.

- Common Patterns
  - [key_based_replication](https://github.com/fivetran/fivetran_connector_sdk/tree/main/examples/source_examples/common_patterns/key_based_replication) - This example shows key-based replication from database sources. Replication keys are columns that are used to identify new and updated data for replication. When you set a table to use Incremental Replication, you’ll also need to define a replication key for that table.

- [gcp_pub_sub](https://github.com/fivetran/fivetran_connector_sdk/tree/main/examples/source_examples/gcp_pub_sub) - This example shows how to sync data from Google Cloud Pub/Sub using the Connector SDK.
- [hubspot](https://github.com/fivetran/fivetran_connector_sdk/tree/main/examples/source_examples/hubspot) - This example shows how to connect and sync specific event type Event data from Hubspot using Connector SDK
- [newsapi](https://github.com/fivetran/fivetran_connector_sdk/tree/main/examples/source_examples/newsapi) - This is a simple example of how to sync data from NewsAPI using Connector SDK.
- [oauth2_and_accelo_api_connector_multithreading_enabled](https://github.com/fivetran/fivetran_connector_sdk/tree/main/examples/source_examples/oauth2_and_accelo_api_connector_multithreading_enabled) - This example shows how to sync data from the Accelo API. It uses OAuth 2.0 Client Credentials flow authentication, rate limiting, and multithreading, which allows to make API calls in parallel to pull data faster. You need to provide your Accelo OAuth credentials for this example to work. Refer to the Multithreading Guidelines in `api_threading_utils.py`.
- [github_traffic](https://github.com/fivetran/fivetran_connector_sdk/tree/main/examples/source_examples/github_traffic) - This example shows how to sync GitHub repositories traffic data using Connector SDK via REST API. You need to provide your GitHub Personal access token and specify the repositories you want to track.
- OData APIs
  - [odata_version_2_using_pyodata](https://github.com/fivetran/fivetran_connector_sdk/tree/main/examples/source_examples/odata_api/odata_version_2_using_pyodata) - This is an example of how to sync data from an OData API version 2 using Connector SDK.
  - [odata_version_4](https://github.com/fivetran/fivetran_connector_sdk/tree/main/examples/source_examples/odata_api/odata_version_4) - This is an example of how to sync data from an OData API version 4 using Connector SDK.
  - [odata_version_4_using_python_odata](https://github.com/fivetran/fivetran_connector_sdk/tree/main/examples/source_examples/odata_api/odata_version_4_using_python_odata) - This is an example of how to sync data from an OData API version 4 using python-odata python library.
- [redshift](https://github.com/fivetran/fivetran_connector_sdk/tree/main/examples/source_examples/redshift) - This is an example to show how to sync records from Redshift by using Connector SDK. You need to provide your Redshift credentials for this example to work.
- [s3_csv_validation](https://github.com/fivetran/fivetran_connector_sdk/tree/main/examples/source_examples/s3_csv_validation) - This is an example of how to read .CSV file from Amazon S3 and validate the data. You need to provide your AWS S3 credentials for this example to work.
- [smartsheets](https://github.com/fivetran/fivetran_connector_sdk/tree/main/examples/source_examples/smartsheets) - This is an example of how we can sync Smartsheets sheets by using Connector SDK. You need to provide your Smartsheets api_key for this example to work.
- [sql_server](https://github.com/fivetran/fivetran_connector_sdk/tree/main/examples/source_examples/sql_server) - This example uses pyodbc to connect to SQL Server for syncing data using Connector SDK. You need to provide your SQL Server credentials for this example to work.
- [toast](https://github.com/fivetran/fivetran_connector_sdk/tree/main/examples/source_examples/toast) - This is an example of how we can sync Toast data using the Connector SDK. You would need to provide your Toast credentials for this example to work.
- [veeva_vault_using_basic_auth](https://github.com/fivetran/fivetran_connector_sdk/tree/main/examples/source_examples/veeva_vault_using_basic_auth) - This example shows how to authenticate to Veeva Vault using basic authentication and sync records from Veeva Vault. You need to provide your Veeva Vault credentials for this example to work.

</details>

## Example Workflows
- [github](https://github.com/fivetran/fivetran_connector_sdk/tree/main/example_workflows/github) - This is an example of a GitHub workflow to deploy a hello connector after a push to the hello directory. It uses GitHub secrets and environment variables to create a configuration.json file that is included in the deployment.

## AI and Connector SDK
 - [Readme](https://github.com/fivetran/fivetran_connector_sdk/tree/main/ai_and_connector_sdk/README.md) - This is an introduction to using AI tools to leverage Connector SDK
 - [claude_20250228](https://github.com/fivetran/fivetran_connector_sdk/tree/main/ai_and_connector_sdk/claude_20250228/) - This example contains the code produced by Claude AI to build a custom connector using our Connector SDK. See our [blog article](https://www.fivetran.com/blog/building-a-fivetran-connector-in-1-hour-with-anthropics-claude-ai) for details.

## Support
Learn how we [support Fivetran Connector SDK](https://fivetran.com/docs/connector-sdk#support).


<details open>
<summary><h2>Private Preview features</h2></summary>

>**NOTE:** The following features are in Private Preview. Please connect with our professional services to get more information about them and enable it for your connector.
- **[Importing External Libraries and Drivers](/examples/private_preview_features/importing_external_drivers)**
  - This feature enables you to install drivers in your connector environment by writing a `installation.sh` file in the `drivers` folder, in the same directory as your connector.py file. This script will be executed at the time of deploying your connector, before your connector.py is run to sync your data.

</details>


## Additional considerations

We provide examples to help you effectively use Fivetran's Connector SDK. While we've tested the code provided in these examples, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples.

Note that API calls made by your Connector SDK connection may count towards your service’s API call allocation. Exceeding this limit could trigger rate limits, potentially impacting other uses of the source API.

It's important to choose the right design pattern for your target API. Using an inappropriate pattern may lead to data integrity issues. We recommend that you review all our examples carefully to select the one that best suits your target API. Keep in mind that some APIs may not support patterns for which we currently have examples.

As with other new connectors, SDK connectors have a [14-day trial period](https://fivetran.com/docs/getting-started/free-trials#newconnectorfreeuseperiod) during which your usage counts towards free [MAR](https://fivetran.com/docs/usage-based-pricing). After the 14-day trial period, your usage counts towards paid MAR. To avoid incurring charges, pause or delete any connections you created to run these examples before the trial ends.

## Maintenance
The `fivetran_connector_sdk` repository is actively maintained by Fivetran Developers. Reach out to our [Support team](https://support.fivetran.com/hc/en-us) for any inquiries.