# Error and warning operations connector example

## Connector overview
This connector demonstrates how to use [`op.warning()`](https://fivetran.com/docs/connector-sdk/technical-reference/connector-sdk-operations#warning) for recoverable row-level problems and [`op.error()`](https://fivetran.com/docs/connector-sdk/technical-reference/connector-sdk-operations#error) for a terminal data-integrity problem in one sync run.

The source is a local `mock_weather.csv` file with columns `zipcode`, `city`, `weather`, and `date`. The connector writes to the `weather` table with `zipcode` as the primary key.

## Requirements
- [Supported Python versions](https://github.com/fivetran/connector_sdk/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connector-sdk/setup-guide) to get started.

To initialize a new Connector SDK project using this connector as a starting point, run:

```bash
fivetran init --template examples/common_patterns/error_and_warning_operations
```

## Features
- Shows two warning scenarios where sync can continue
- Shows one fatal primary-key validation error where sync stops
- Uses a small local CSV file for deterministic, repeatable behavior

## Pagination
Not applicable. This connector reads a local CSV file.

## Data handling
The connector processes rows from `mock_weather.csv` and applies these checks in order:

1. If `city` is empty, it emits warning 1 and skips the row.
2. If `date` is present but not in `YYYY-MM-DD` format, it emits warning 2 and skips the row.
3. If `zipcode` (primary key) is empty, it emits a terminal error and exits immediately.

Valid rows are upserted into `WEATHER`.

## Error handling
The connector uses:

- [`op.warning(message="...")`](https://fivetran.com/docs/connector-sdk/technical-reference/connector-sdk-operations#warning) for recoverable row-level quality issues.
  - `message` (str): non-empty warning text shown in the dashboard. In this example, it is used for empty `city` and invalid `date` format rows that are skipped.
- [`op.error(message="...", trace="...")`](https://fivetran.com/docs/connector-sdk/technical-reference/connector-sdk-operations#error) when primary key identity is missing and data can no longer be safely written.
  - `message` (str): non-empty fatal error text shown in the dashboard.
  - `trace` (str, optional): extra debug context such as failed check name, row number, and row values.

`op.error()` terminates the sync immediately.

## Tables created
The connector creates the `WEATHER` table.
```json
{
  "table": "weather",
  "primary_key": ["zipcode"],
  "columns": {
    "zipcode": "STRING",
    "city": "STRING",
    "weather": "STRING",
    "date": "NAIVE_DATE"
  }
}
```

## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
