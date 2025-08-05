# Complex Configuration Casting Connector Example

## Connector overview
This example demonstrates how to cast complex configuration values (e.g., comma-separated strings, JSON strings, numeric and boolean values) into appropriate Python types within a Fivetran connector. It uses the Connector SDK to validate and parse config entries for `regions`, `api_quota`, `use_bulk_api`, and `currencies`, and emits a test row to confirm the values are parsed correctly.

This pattern is useful for:
- Working with custom connector configurations passed through configuration.json.
- Dynamically handling typed settings like lists, integers, booleans, and JSON objects.


## Requirements
- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)   
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)


## Getting started
Refer to the [Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.


## Features
- Validates presence of required configuration fields
- Casts:
  - Comma-separated strings → list `regions`.
  - Numeric string → integer `api_quota`.
  - Boolean string → bool `use_bulk_api`.
  - JSON string → parsed Python list of dictionaries `currencies`.
- Uses assert statements to confirm parsing behavior.
- Emits a test message `hello world` to confirm successful processing.


## Configuration file
The connector requires the following configuration parameters:
```json
{
  "regions": "us-east-1,us-east-4,us-central-1",
  "api_quota": "12345",
  "use_bulk_api": "true",
  "currencies": "[{\"From\": \"USD\",\"To\": \"EUR\"},{\"From\": \"USD\",\"To\": \"GBP\"}]"
}
```

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.


## Requirements file
This connector does not require any Python dependencies.

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.


## Authentication
This connector does not require authentication - this is a local static example which shows paring of complex configuration options. In a production scenario, use headers or token-based authentication as necessary.


## Pagination
Not applicable - this connector emits a single static row.


## Data handling
- Configuration values are parsed and validated.
- A single record in table `CRYPTO` is sent using `op.upsert()`.


## Error handling
- The connector raises a `ValueError` if any required configuration field is missing.
- Parsing errors from invalid JSON are surfaced during `json.loads()`.
- Logs informative messages via the SDK’s Logging module.


## Tables Created
The connector creates a `CRYPTO` table:

```json
{
  "table": "crypto",
  "primary_key": ["msg"],
  "columns": {
    "msg": "STRING"
  }
}
```


## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.