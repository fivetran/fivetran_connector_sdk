# Sensor Tower Connector SDK Example

This example demonstrates how to build a Fivetran Connector SDK integration for [Sensor Tower](https://sensortower.com), a market intelligence and analytics platform that provides insights into mobile apps, app store trends, and digital advertising. The connector pulls data from Sensor Tower's API for three tables: `sales_report_estimates`, `active_users`, and `retention`. You can configure the connector to track specific iOS and Android app IDs to gather the necessary information for your use case.

## Requirements

* [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)   
* Operating System:  
  * Windows 10 or later  
  * macOS 13 (Ventura) or later

## Getting started

Refer to the [Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

* Supports three Sensor Tower endpoints: `sales_report_estimates`, `active_users`, and `retention` (see `ENDPOINTS` in `connector.py`)
* Handles both iOS and Android app IDs (see `IOS_APP_IDS` and `ANDROID_APP_IDS`)
* Incremental syncs with configurable lookback window (see `update` function)
* Schema mapping and column renaming for clarity (see `key_mapping` and `process_sales_report`)
* Modular data processing for each endpoint (see `process_active_users`, `process_sales_report`, `process_retention`)

## Configuration file

The connector expects a `configuration.json` file with the following structure:

```
{
  "auth_token": "YOUR_SENSOR_TOWER_API_TOKEN"
}
```

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file

The `requirements.txt` file specifies any additional Python libraries required by the connector. For this example, no extra dependencies are required beyond the Fivetran environment defaults.

*Example content of `requirements.txt`:*

```
requests
dateutil
```

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication

The connector uses a Sensor Tower API token for authentication. You can obtain your API token from your Sensor Tower account dashboard. The token should be provided in the `configuration.json` file as the value for the `auth_token` key. (See `update` function in `connector.py`)

## Pagination

This example does not implement explicit pagination, as the Sensor Tower API endpoints used either return all requested data in a single response or are filtered by date range and country. (See `get_data` function)

## Data handling

* Data is retrieved from the Sensor Tower API using the `get_data` function.
* Each endpoint is processed by a dedicated function:
  * `process_active_users` (see lines ~90-102)
  * `process_sales_report` (see lines ~104-115)
  * `process_retention` (see lines ~117-129)
* Data is mapped and transformed as needed (e.g., column renaming for `sales_report_estimates` via `key_mapping`).
* All records are delivered to Fivetran using the `op.upsert` operation.
* The schema is defined in the `schema` function (see lines ~44-66).

## Error handling

This example does not implement custom error handling. Any HTTP or API errors will raise exceptions via the `requests` library. (See `get_data` function)

## Tables Created

* `sales_report_estimates` – Contains app sales and revenue estimates by date and country.
* `active_users` – Tracks active user counts by app, date, time period, and country.
* `retention` – Provides retention metrics by app, date, and country.

*Sample data structure:*

| App ID | Date       | Country Code | iPhone Downloads | iPhone Revenue | ... |
|--------|------------|--------------|------------------|---------------|-----|
| 123456 | 2024-05-01 | US           | 1000             | 5000          | ... |

## Additional files

This example does not include additional files beyond the main connector script and configuration.

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team. 
