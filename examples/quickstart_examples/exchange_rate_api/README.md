# Exchange Rate Connector Example

## Connector overview
This connector uses the Fivetran Connector SDK to sync historical exchange rates from the [Frankfurter API](https://api.frankfurter.dev). It fetches daily currency conversion rates for a configurable currency pair and loads them into a table named `EXCHANGE_RATES`.

This example is useful to:
- Understand incremental syncs with cursor-based state management.
- Learn retry logic with exponential backoff for API requests.
- See how to define an explicit schema with typed columns.

Note: This connector uses the Frankfurter API which is free and does not require an API key. See [Best Practices](https://fivetran.com/docs/connector-sdk/best-practices#declaringprimarykeys) for more.


## Requirements
- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)   
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)


## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connector-sdk/setup-guide) to get started.

To initialize a new Connector SDK project using this connector as a starting point, run:

```bash
fivetran init <project-path> --template examples/quickstart_examples/exchange_rate_api
```
`fivetran init` initializes a new Connector SDK project by setting up the project structure, configuration files, and a connector you can run immediately with `fivetran debug`.
If you do not specify a project path, Fivetran creates the project in your current directory.
For more information on `fivetran init`, refer to the [Connector SDK `init` documentation](https://fivetran.com/docs/connector-sdk/setup-guide#createyourcustomconnector).


## Features
- Implements both the `schema()` and `update()` functions.
- Fetches daily exchange rates incrementally using cursor-based state.
- Uses `op.upsert()` to insert or update exchange rate records.
- Uses `op.checkpoint()` to track the last synced date for incremental syncs.
- Includes retry logic with exponential backoff (up to 3 attempts) for API resilience.


## Configuration file
Create a `configuration.json` file in the project root with the following structure:

```json
{
    "from_currency": "USD",
    "to_currency": "EUR"
}
```

| Key             | Required | Description                                      |
|-----------------|----------|--------------------------------------------------|
| `from_currency` | Yes      | The base currency code (e.g., `USD`, `GBP`).    |
| `to_currency`   | Yes      | The target currency code (e.g., `EUR`, `JPY`).  |

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.


## Requirements file
This connector does not require any additional Python dependencies beyond the pre-installed packages.

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.


## Authentication
This connector does not require authentication. The Frankfurter API is a free, open API with no API key required.


## Pagination
Not applicable. The Frankfurter API returns all exchange rates for the requested date range in a single response.


## Data handling
- The `update()` function fetches exchange rates incrementally from the day after the last checkpoint through today.
- On the first sync, it fetches rates starting from `2020-01-01`.
- Each record is upserted with the composite primary key (`date`, `from_currency`, `to_currency`).
- State is checkpointed after each sync with the latest synced date, enabling incremental syncs on subsequent runs.


## Error handling
- API requests include retry logic with exponential backoff (up to 3 attempts).
- Individual record processing failures are logged and halt further processing to preserve data integrity.
- Missing or invalid configuration keys are validated at the start of each sync.


## Tables Created
The connector creates an `EXCHANGE_RATES` table:

```json
{
  "table": "exchange_rates",
  "primary_key": ["date", "from_currency", "to_currency"],
  "columns": {
    "date": "NAIVE_DATE",
    "from_currency": "STRING",
    "to_currency": "STRING",
    "rate": "DECIMAL"
  }
}
```


## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.