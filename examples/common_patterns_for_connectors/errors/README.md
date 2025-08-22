# Error Handling Patterns Connector Example

## Connector overview
This connector demonstrates common error handling patterns and best practices when developing Fivetran connectors. It simulates various error scenarios that might occur during connector execution, such as missing configuration, invalid state format, data generation errors, processing errors, and database operation errors. This example is designed to help developers understand how to implement robust error handling in their connectors.


## Requirements
- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)   
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.


## Features
- Demonstrates five different error handling scenarios
- Shows proper error logging techniques
- Implements best practices for configuration validation
- Provides examples for state management error handling
- Shows how to handle errors during data operations


## Configuration file
The connector uses the following configuration parameter:

```json
{
  "error_simulation_type": "<YOUR_ERROR_SIMULATION_TYPE>"
}
```

The error_simulation_type parameter can be set to:

- "0": No error simulation (normal operation)
- "1": Missing Configuration error
- "2": Invalid State Format error
- "3": Data Generation Error
- "4": Processing Error
- "5": Database Operation Error

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.


## Requirements file
This connector does not require any additional python packages.

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.


## Tables created
This connector creates a single table:

- `DEMO_RESPONSE_TEST` - Contains simulated data with a primary key of `key`


## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
