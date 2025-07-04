# Microsoft InTune Managed Devices Connector SDK Example

This example demonstrates how to build a Fivetran Connector SDK integration for Microsoft InTune, using the Microsoft Graph API to retrieve managed device data. The connector pulls data from the InTune managed devices endpoint and delivers it to your Fivetran destination in a single table called `managed_devices`. You will need to provide your own Microsoft credentials for this to work (`tenant_id`, `client_id`, and `client_secret`).

## Requirements

* [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)   
* Operating System:  
  * Windows 10 or later  
  * macOS 13 (Ventura) or later

## Getting started

Refer to the [Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

* Retrieves managed device data from Microsoft InTune using the Microsoft Graph API (see `update` function)
* Handles API pagination using the `@odata.nextLink` field (see `update` function)
* Converts list values in device records to JSON strings for compatibility (see `list_to_json` function)
* Delivers data to a single table: `managed_devices`
* Uses Fivetran Connector SDK logging for status and error reporting (see `log` usage)

## Configuration file

The connector expects a `configuration.json` file with the following structure:

```
{
  "tenant_id": "YOUR_TENANT_ID",
  "client_id": "YOUR_CLIENT_ID",
  "client_secret": "YOUR_CLIENT_SECRET"
}
```

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Authentication

The connector uses OAuth2 client credentials flow to authenticate with Microsoft Graph API. You will need to provide your Azure tenant ID, client ID, and client secret in the `configuration.json` file. The connector retrieves an access token using these credentials (see `get_access_token` function in `test.py`).

## Pagination

The connector handles pagination using the `@odata.nextLink` field returned by the Microsoft Graph API. It continues to request additional pages until all managed devices are retrieved (see `update` function, lines ~38-81).

## Data handling

* Data is retrieved from the Microsoft Graph API using the `update` function (see lines ~38-81)
* List values in device records are converted to JSON strings using the `list_to_json` function (see lines ~84-89)
* Data is delivered to Fivetran using the `op.upsert` operation
* The schema is defined in the `schema` function (see lines ~13-18)

## Error handling

* Uses Fivetran Connector SDK logging for info and severe error messages (see `log` usage throughout)
* Raises exceptions for failed authentication or API errors (see `get_access_token` and `update` functions)

## Tables Created

* `managed_devices` – Contains all managed device records retrieved from Microsoft InTune.

*Sample data structure:*

| id                                   | deviceName | operatingSystem | ... |
|--------------------------------------|------------|----------------|-----|
| 12345678-90ab-cdef-1234-567890abcdef | SurfacePro | Windows        | ... |

## Additional files

This example does not include additional files beyond the main connector script and configuration.

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
