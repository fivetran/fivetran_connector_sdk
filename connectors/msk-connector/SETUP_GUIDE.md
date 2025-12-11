# Workday HCM Connector - Setup Guide

Steps to set up and test the Connector locally.

## Prerequisites

Before you begin, ensure you have the following installed:

- **Python 3.11 or later** (Python 3.13 recommended)
- **pip** (Python package manager)

## Step 1: Extract the Package

1. Extract the ZIP file to a directory of your choice
2. Open a terminal/command prompt and navigate to the extracted directory:
   ```bash
   cd workday-connector
   ```

## Step 2: Install Dependencies

Install the required Python packages:

```bash
pip install fivetran-connector-sdk requests
```

**Note**: The connector uses Python's built-in `xml.etree.ElementTree` for XML parsing, so no additional XML library is needed.


## Step 3: Configure the Connector

Update `configuration.json` with your Workday API credentials:

```json
{
  "workday_url": "https://wd2-impl.workday.com",
  "username": "your_workday_username",
  "password": "your_workday_password",
  "tenant": "your_tenant_name",
  "effective_start_date": "2020-01-31"
}
```

**Configuration Fields:**
- `workday_url`: The Workday API URL
- `username`: Your Workday username
- `password`: Your Workday password
- `tenant`: Your Workday tenant name
- `effective_start_date`: Starting date for historical data (YYYY-MM-DD format)

## Step 4: Test the Connector

Run the connector, from the root directory:

```bash
fivetran debug --configuration configuration.json --mode sync
```

This will run the connector in `initial` sync mode, which will fetch all the data from the start date to the current date, and all the subsequent syncs will be in `incremental` mode, which will fetch the data from the last sync date to the current date.

## Step 5: Files generated

When you run the connector, the following files will be generated in the **files/** folder, if there are no errors:

- `state.json`: The state of the connector
- `warehouse.db`: The warehouse database (DuckDB)


## Project Structure

```
workday-connector/
├── connector.py              # Main connector (production version)
├── configuration.json        # Connector configuration
├── SETUP_GUIDE.md            # This file
```

Here are some helpful links for the Connector SDK:

- [Connector SDK docs](https://fivetran.com/docs/connector-sdk)
- [Tutorials (be sure to check the AI video walkthroughs)](https://fivetran.com/docs/connector-sdk/tutorials)
- [Connector SDK repo](https://github.com/fivetran/fivetran_connector_sdk)
- [Quickstart Examples](https://github.com/fivetran/fivetran_connector_sdk/tree/main/examples/quickstart_examples)
- [Common Patterns](https://github.com/fivetran/fivetran_connector_sdk/tree/main/examples/common_patterns_for_connectors)
- [Source Examples](https://github.com/fivetran/fivetran_connector_sdk/tree/main/connectors)