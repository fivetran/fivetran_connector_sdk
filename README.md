# Requirements
- Python 3.9 or later
- Operating System:
  - Windows 10 or later
  - MacOS 13 (Ventura) or later

# Getting started
See [Quickstart guide](https://fivetran.com/docs/connectors/connector-sdk/quickstart-guide) to get started.

# Examples
There are several examples available under `/examples`:

## hello
Simplest example, append-only

## local
Emulated source, without any calls out to the internet

## multiple_tables_with_cursors
The parent-child relationship between tables from incremental API endpoints, with the complex cursor.

## pagination
Simple pagination example templates for the following types of paginations:
- keyset
- next_page_url
- offset_based
- page_number

## specified_types
Declares a schema and upserts all data types

## unspecified_types
Upserts all data types without specifying a schema

## three_operations
Shows how to use upsert, update and delete operations

## user_profiles
Shows the use of Pandas DataFrames to manipulate data prior to sending to Fivetran.

## weather
A realistic example, using a public API, fetching data from NOAA

## configuration
Shows how to use secrets
