# FDA Food Enforcement Connector

## Connector overview

This connector fetches food enforcement data (recalls, enforcement actions, etc.) from the FDA's openFDA API (https://api.fda.gov/food/enforcement.json). It supports incremental syncs (using a `last_sync_time` state) and flattens nested JSON into a single table (`fda_food_enforcement`) for easy replication into your Fivetran destination. The connector is designed to work with or without an API key (with different rate limits) and is ideal for users who need to monitor FDA food recalls and enforcement actions.

## Requirements

* [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
* Operating system:
  * Windows: 10 or later (64-bit only)
  * macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  * Linux: Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started

Refer to the [Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

* Fetches food enforcement data (recalls, enforcement actions) from the FDA's openFDA API
* Supports incremental syncs (via `last_sync_time` state)
* Flattens nested JSON (using the `flatten_dict` function) so that the data is delivered as a single table (`fda_food_enforcement`)
* Configurable batch size (default 100) and rate limit pause (default 0.25s with an API key, 0.5s without)
* (Optional) `MAX_BATCHES` variable (default 10) to limit the number of batches (requests) processed in a single sync (for testing or demonstration). To process the full dataset, set `MAX_BATCHES` to `None`.
* Robust error handling (retry logic, logging, and graceful exit)

## Configuration file

Detail the configuration keys defined for your connector, which are uploaded to Fivetran from the configuration file (e.g., `config.json`).

```json
{
  "api_key": "YOUR_API_KEY (or omit for no API key – note that rate limits differ)",
  "base_url": "https://api.fda.gov/food/enforcement.json",
  "batch_size": 100, // optional, default 100
  "rate_limit_pause": 0.25 // optional, default 0.25 (with API key) or 0.5 (without API key)
}
```

Note: Ensure that the configuration file is not checked into version control to protect sensitive information.

## Requirements file

The `requirements.txt` file specifies the Python libraries required by the connector.

Example content of `requirements.txt`:

```
python_dateutil==2.8.2
pytz
```

Note: The `fivetran_connector_sdk` and `requests` packages are pre-installed in the Fivetran environment. Do not declare them in your `requirements.txt`.

## Authentication

* The connector supports two modes:
  * With an API key (passed via `api_key` in the configuration file):
    * Adds an `Authorization` header and `api_key` query parameter
    * Higher rate limits (see openFDA docs)
  * Without an API key (omit `api_key` or set `api_key` to `string`):
    * No `Authorization` header or `api_key` query parameter
    * Lower rate limits and a longer `rate_limit_pause` (0.5s)
* To obtain an API key, visit the FDA's openFDA API documentation

## Pagination

* The connector uses offset-based pagination via the `skip` and `limit` query parameters to fetch batches (e.g., 100 records per request)
* See the `update` function (lines ~100–220) for details
* The `MAX_BATCHES` variable (default 10) limits the number of batches processed in a single sync for testing. To process the full dataset, set `MAX_BATCHES` to `None`.

## Data handling

* The `update` function fetches batches (using `fetch_data`) and flattens each record (using `flatten_dict`) so that nested JSON (e.g., `openfda` fields) is converted into a flat structure
* Date strings (ending in `_date` or `_time`) are converted to ISO-formatted strings
* The `schema` function defines the primary keys (`recall_number` and `event_id`) for the `fda_food_enforcement` table. Other column types are inferred by Fivetran
* See `update` and `flatten_dict` for details

## Error handling

* The `fetch_data` function implements a retry loop (with `MAX_RETRIES` and `RETRY_DELAY`) so that transient errors (e.g., 500 Internal Server Error) are caught and retried
* If the FDA API returns a 500 error (e.g., due to an empty `report_date` filter), the connector logs a SEVERE error and exits. In production, you may choose to treat such errors as empty results or reset the state for a full sync
* See `fetch_data` (lines ~70–100) for details

## Tables Created

* One table is created and replicated by the connector:
  * `fda_food_enforcement` (primary keys: `recall_number` and `event_id`)
* To inspect the table, run `duckdb files/warehouse.db` or use the Fivetran dashboard

## Prompt used
"I need a Fivetran Connector SDK solution for @https://api.fda.gov/food/enforcement.json?. I have some notes and example queries in @notes.txt  and the fields documented in @fields.yaml . 

Have it dynamically create tables based on the endpoints available. Flatten the dictionaries and upsert the key:value pairs as the columns for the tables. Only define the PK for the schema objects, let Fivetran infer the rest.

Create a Fivetran Connector SDK solution follows Fivetran best practice outlined in @connector_sdk_agent  & @template_connector. I have the files prepared in @/FDA_food"

## Additional files

* `connector.py` – Contains the main connector logic (fetching, flattening, and yielding upsert and checkpoint operations)
* `config.json` (or `configuration.json`) – Contains the connector's configuration
* `requirements.txt` – Lists the Python libraries required by the connector

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team. 
