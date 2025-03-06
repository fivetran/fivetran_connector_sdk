<p align="center">
  <a href="https://www.fivetran.com/"><img src="https://cdn.prod.website-files.com/6130fa1501794ed4d11867ba/63d9599008ad50523f8ce26a_logo.svg" alt="Fivetran"></a>
</p>
<p align="center">
    Fivetran’s Connector SDK allows Real-time, efficient data replication to your destination of choice.
</p>
<p align="center">
<a href="https://github.com/fivetran/fivetran_connector_sdk/stargazers/" target="_blank">
    <img src="https://img.shields.io/github/stars/fivetran/fivetran_connector_sdk?style=social&label=Star&maxAge=2592000" alt="Stars">
</a>
<a href="https://github.com/fivetran/fivetran_connector_sdk?tab=MIT-1-ov-file#readme" target="_blank">
    <img src="https://img.shields.io/badge/License-MIT-blue" alt="License">
</a>
</p>

# Overview
This repository contains a collection of example custom connectors using Fivetran's [Connector SDK](https://fivetran.com/docs/connectors/connector-sdk), demonstrating how to build [custom data connectors](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) in Python and deploy them as an extension of Fivetran.

# Why Connector SDK?
Fivetran’s Connector SDK allows you to code a custom data connector using Python and deploy it as an extension of Fivetran. Fivetran automatically manages running Connector SDK connections on your scheduled frequency and manages the required compute resources, eliminating the need for a third-party provider.

Connector SDK provides native support for many Fivetran features and relies on existing Fivetran technology. It also eliminates timeout and data size limitations seen in AWS Lambda.

# Requirements
- Python ≥3.9 and ≤3.12
- Operating System:
  - Windows 10 or later
  - MacOS 13 (Ventura) or later

# Getting started

See [Setup guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

# Examples
There are several examples available under `/examples`:

<details open>
<summary><h2>Quickstart Examples</h2></summary>

- **[hello](/examples/quickstart_examples/hello)**  
  - This is the simplest, append-only example.

- **[simple_three_step_cursor](/examples/quickstart_examples/simple_three_step_cursor)**  
  - This is an emulated source, without any calls out to the internet.

- **[configuration](/examples/quickstart_examples/configuration)**  
  - This example shows how to use secrets.

- **[multiple_code_files](/examples/quickstart_examples/multiple_code_files)**  
  - This example shows how you can write a complex connector comprising multiple `.py` files.

- **[using_pd_dataframes](/examples/quickstart_examples/using_pd_dataframes)**  
  - This example shows the use of Pandas DataFrames to manipulate data prior to sending to Fivetran.

- **[large_data_set](/examples/quickstart_examples/large_data_set)**  
  - This example shows how to handle large data from API responses with pagination and without pagination.

- **[weather](/examples/quickstart_examples/weather)**  
  - This is a realistic example, using a public API, fetching data from NOAA.

- **[complex_configuration_options](/examples/quickstart_examples/complex_configuration_options)**  
  - Shows how to cast configuration fields to list, integer, boolean, and dict for use in connector code.

</details>

<details open>
<summary><h2>Common Patterns for Connectors</h2></summary>

- **Authentication**
  - [api_key](/examples/common_patterns_for_connectors/authentication/api_key)
  - [http_basic](/examples/common_patterns_for_connectors/authentication/http_basic)
  - [http_bearer](/examples/common_patterns_for_connectors/authentication/http_bearer)
  - [oauth2_with_token_refresh](/examples/common_patterns_for_connectors/authentication/oauth2_with_token_refresh)
  - [session_token](/examples/common_patterns_for_connectors/authentication/session_token)

- **Cursors**
  - [marketstack](/examples/common_patterns_for_connectors/cursors/marketstack)
  - [multiple_tables_with_cursors](/examples/common_patterns_for_connectors/cursors/multiple_tables_with_cursors)

- **Export**
  - [csv](/examples/common_patterns_for_connectors/export/csv)

- **Hashes**
  - [hashes](/examples/common_patterns_for_connectors/hashes)

- **Pagination**
  - [keyset](/examples/common_patterns_for_connectors/pagination/keyset)
  - [next_page_url](/examples/common_patterns_for_connectors/pagination/next_page_url)
  - [offset_based](/examples/common_patterns_for_connectors/pagination/offset_based)
  - [page_number](/examples/common_patterns_for_connectors/pagination/page_number)

- **Other Patterns**
  - [priority_first_sync_for_high_volume_initial_syncs](/examples/common_patterns_for_connectors/priority_first_sync_for_high_volume_initial_syncs)
  - [records_with_no_created_at_timestamp](/examples/common_patterns_for_connectors/records_with_no_created_at_timestamp)
  - [specified_types](/examples/common_patterns_for_connectors/specified_types)
  - [three_operations](/examples/common_patterns_for_connectors/three_operations)
  - [unspecified_types](/examples/common_patterns_for_connectors/unspecified_types)

</details>

<details open>
<summary><h2>Source Examples</h2></summary>

- **AWS**
  - [aws_athena/using_boto3](/examples/source_examples/aws_athena/using_boto3)
  - [aws_athena/using_sqlalchemy](/examples/source_examples/aws_athena/using_sqlalchemy)
  - [aws_dynamo_db_authentication](/examples/source_examples/aws_dynamo_db_authentication)

- **Common Patterns**
  - [key_based_replication](/examples/source_examples/common_patterns/key_based_replication)

- **API Integrations**
  - [newsapi](/examples/source_examples/newsapi)
  - [oauth2_and_accelo_api_connector_multithreading_enabled](/examples/source_examples/oauth2_and_accelo_api_connector_multithreading_enabled)
  - [smartsheets](/examples/source_examples/smartsheets)
  - [toast](/examples/source_examples/toast)
  - [veeva_vault_using_basic_auth](/examples/source_examples/veeva_vault_using_basic_auth)

- **Database Connectors**
  - [redshift](/examples/source_examples/redshift)
  - [sql_server](/examples/source_examples/sql_server)

</details>



# Repository Structure
...


# Support
For Fivetran Connector SDK support consult the [Fivetran Documentation](https://fivetran.com/docs/connector-sdk#support).


# Additional considerations

We provide examples to help you effectively use Fivetran's Connector SDK. While we've tested the code provided in these examples, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples.

Note that API calls made by your Connector SDK connection may count towards your service’s API call allocation. Exceeding this limit could trigger rate limits, potentially impacting other uses of the source API.

It's important to choose the right design pattern for your target API. Using an inappropriate pattern may lead to data integrity issues. We recommend that you review all our examples carefully to select the one that best suits your target API. Keep in mind that some APIs may not support patterns for which we currently have examples.

As with other new connectors, SDK connectors have a [14-day trial period](https://fivetran.com/docs/getting-started/free-trials#newconnectorfreeuseperiod) during which your usage counts towards free [MAR](https://fivetran.com/docs/usage-based-pricing). After the 14-day trial period, your usage counts towards paid MAR. To avoid incurring charges, pause or delete any connections you created to run these examples before the trial ends.

# Maintenance
This repository is actively maintained by Fivetran Developers. Reach out to our [Support team](https://support.fivetran.com/hc/en-us) for any inquiries.
