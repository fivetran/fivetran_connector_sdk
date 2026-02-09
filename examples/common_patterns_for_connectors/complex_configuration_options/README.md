# Complex Configuration Options Connector Example

## Connector overview
This example demonstrates handling of complex configuration values. This example shows how to handle cases when connector requires complex configuration that is difficult or impractical to express in `configuration.json`. This may include deeply nested structures or non-string values. A separate python file can be used to define such configuration values. These configurations are static and must not contain any sensitive information. The connector needs to be redeployed for updating the static values defined in the separate python file.

This pattern is useful for:
- Working with custom connector configurations passed through configuration.json.
- Maintaining static, complex configuration structures that are defined at deployment time.
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
- Demonstrates two approaches for complex configuration handling:
  - Approach 1 - Parse string values from `configuration.json` (editable from Fivetran dashboard)
  - Approach 2 - Use native Python structures from `conf.py` (static, deployment-time configuration)
- Validates presence of required configuration fields
- Casts configuration values from strings to appropriate types:
  - Comma-separated strings → list (`regions`)
  - Numeric strings → integers (`api_quota`)
  - Boolean strings → booleans (`use_bulk_api`)
  - JSON strings → parsed Python structures (`currencies`)
- Handles deeply nested configuration structures (`complex_configuration`)
- Uses assert statements to confirm parsing behavior
- Emits a test message `hello world` to confirm successful processing


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
The configuration also accepts optional key `complex_configuration`:
```
"complex_configuration": "{\"level_1\": {\"level_2\": {\"level_3\": \"This is a complex configuration value\"}}}"
```

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

When you have complex configuration structures that is not sensitive, does not need to be changed from the Fivetran dashboard and is difficult to encode as strings in `configuration.json`, You can define them directly in `conf.py` using native Python types:

```python
API_CONFIGURATION = {
    "regions": ["us-east-1", "us-east-4", "us-central-1"],
    "api_quota": 1000,
    "use_bulk_api": True,
    "currencies": [
        {"From": "USD", "To": "EUR"},
        {"From": "USD", "To": "GBP"}
    ],
    "complex_configuration": {
        "level_1": {
            "level_2": {
                "level_3": "This is a complex configuration value"
            },
            "list_of_dicts": [
                {"name": "item1", "value": 1},
                {"name": "item2", "value": 2}
            ]
        }
    }
}
```

Note: Ensure that you do not use `conf.py` to store sensitive information. You should always use `configuration.json` to define sensitive information required by the connector.



## Requirements file
This connector does not require any Python dependencies.

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.


## Authentication
This connector does not require authentication - it is a demonstration example showing how to parse complex configuration options. In a production scenario, use headers or token-based authentication as necessary, storing credentials in `configuration.json` (never in `conf.py`).


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

## Additional files
- `conf.py` – Defines complex configuration structures using native Python types. This file demonstrates how to maintain static configuration that doesn't need to be editable from the Fivetran dashboard. The connector falls back to these values when `complex_configuration` is not present in `configuration.json`.

## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.