# Smartsheet API Connector Example

## Connector overview
This connector demonstrates how to sync row-level data from a Smartsheet sheet using the Fivetran Connector SDK and the Smartsheet [getSheet API endpoint](https://smartsheet.redoc.ly/tag/sheets#operation/getSheet). It retrieves rows from a single sheet, dynamically maps columns using column IDs, and emits those rows to a destination table.

This example supports:
- Incremental syncs using `rowsModifiedSince`.
- Dynamic schema based on column headers.
- Optional row-level metadata.
- Configurable authentication and sheet selection.

This example is currently configured for a single sheet with no pagination. You can extend it to handle multiple sheets and pagination via the `includeAll=true` query parameter or the `page` and `pageSize` parameters.


## Requirements
- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)   
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)


## Getting started
Refer to the [Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.


## Features
- Retrieves full sheet metadata and all rows using Smartsheet’s Sheets API.
- Maps `columnId` to column names to construct each row as a flat dictionary.
- Includes row metadata fields like `rowNumber`, `createdAt`, and `modifiedAt`.
- Uses `rowsModifiedSince` to incrementally sync only updated rows.
- Uses `op.upsert()` for each row and checkpoints with the current sync timestamp.


## Configuration file
The connector requires the following configuration parameters:

```json
{
    "smartsheet_api_token": "your_api_token",
    "smartsheet_sheet_id": "your_smartsheet_sheet_id"
}
```

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.


## Requirements file
This connector requires the following Python packages:

```
requests
```

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.


## Authentication
Authentication is handled via a Bearer token using the Authorization header.


## Pagination
Pagination is not currently implemented in this example.


## Data handling
- Each row in the Smartsheet is returned as a dictionary of cell values, mapped to the correct column titles.
- Column names are dynamically derived from the `columns` array in the API response.
- Null or empty cells are ignored.


## Error handling
- API errors are surfaced using `requests.raise_for_status()`.
- Any failure in authentication, network, or sheet access will raise an exception.
- You can extend this with `try/except` blocks and log warnings for partial errors.


## Tables created
The connector creates a `SMARTSHEET_TABLE_NAME` table:

```json
{
  "table": "smartsheet_table_name",
  "primary_key": ["id"],
  "columns": {
    "id": "STRING"
  }
}
```


## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.