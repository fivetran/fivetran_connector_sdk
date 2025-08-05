# Records Without CreatedAt Field Connector Example

## Connector overview
This connector demonstrates how to handle records from a data source that does not provide a `created_at` timestamp or equivalent. In such cases, we can use the updated_at field as part of the composite primary key to uniquely identify each version of a record.

This approach ensures that:
- New rows are inserted when the record is updated in the source.
- The historical context of updates is preserved in the destination.
- The original time when a record was first seen can be inferred from the system column _fivetran_synced.

This pattern is particularly useful when building connectors for systems that overwrite records without versioning or creation tracking.


## Requirements
- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)   
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)


## Getting started
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.


## Features
- Handles sources with no `created_at` or insertion timestamp.
- Uses a composite primary key: `["id", "updated_at"]`.
- Appends a new row for every observed update to a record.
- Leverages `_fivetran_synced` system column to determine when each record was synced.


## Configuration file
This example does not require a configuration file.

In production, `configuration.json` might contain API tokens, initial cursors, or filters to narrow down API results.

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.


## Requirements file
No external dependencies beyond the SDK are required for this example.

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.


## Authentication
This connector is self-contained and does not communicate with any external API.

If adapting this for an external source, add authentication headers in your request logic and load credentials from the configuration.


## Pagination
Pagination is not implemented in this minimal example. In real-world connectors, combine pagination with checkpointed cursors for efficiency.


## Data handling
- Each record includes an `id`, `updated_at`, and additional fields such as `first_name`, `last_name`, and `designation`.
- If the same `id` is updated multiple times, a new row is added for each `updated_at` timestamp.
- The primary key includes both `id` and `updated_at`, ensuring each row is uniquely identified.

This allows tracking the evolution of a record over time in the destination.


## Error handling
- Basic logging via `log.warning()` and SDK's default exception propagation.
- Add custom error-handling logic as needed for production connectors.

## Tables created
The connector creates the `USER` table:

```
{
  "table": "user",
  "primary_key": ["id", "updated_at"],
  "columns": {
    "id": "INT",
    "updated_at": "UTC_DATETIME",
    "first_name": "STRING",
    "last_name": "STRING",
    "designation": "STRING"
  }
}
```


## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.