# Incremental Sync Strategies Example

## Connector overview

This example demonstrates multiple ways to perform incremental syncs using the Fivetran Connector SDK. It includes five common strategies:

- **Keyset Pagination**: Uses a cursor (e.g., updatedAt timestamp) to fetch new/updated records since the last sync.
- **Offset-based Pagination**: Uses an offset and page size to fetch records in batches, saving the offset as state.
- **Timestamp-based Sync**: Uses a timestamp to fetch all records updated since the last sync, saving the latest timestamp as state.
- **Step-size Sync**: Uses ID ranges to fetch records in batches when pagination/count is not supported, saving the current ID as state.
- **Replay Sync**: Uses timestamp-based sync with a buffer (goes back X hours from last timestamp) for read-replica scenarios with replication lag.

## Requirements

* [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)   
* Operating system:
  * Windows: 10 or later (64-bit only)
  * macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  * Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started

Refer to the [Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## How it works

- The connector exposes a `strategy` configuration option. Set this to `keyset`, `offset`, or `timestamp` to select the incremental sync method.
- The connector saves and updates state differently for each strategy, demonstrating best practices for incremental syncs.
- The schema is the same for all strategies and delivers a `user` table.

## Configuration

Edit `configuration.json` to set the desired strategy and parameters:

```
{
  "strategy": "keyset", // or "offset", "timestamp", "step_size", "replay"
  "base_url": "http://127.0.0.1:5001/pagination/keyset",
  "page_size": 100,
  "initial_id": 1,
  "step_size": 1000,
  "max_id": 100000,
  "buffer_hours": 2
}
```

## Usage

1. Install dependencies:
   ```
   pip install fivetran-connector-sdk requests
   ```
2. Run a mock API (see [fivetran-api-playground](https://pypi.org/project/fivetran-api-playground/)) or point to your own API.
3. Run the connector for local testing:
   ```
   python connector.py
   ```
4. Change the `strategy` in `configuration.json` to try different incremental sync methods.

## Strategies explained

- **Keyset Pagination**: Saves the last `updatedAt` value and fetches records where `updatedAt` is greater than the saved value.
- **Offset-based Pagination**: Saves the last offset and fetches the next batch of records using `offset` and `limit` parameters.
- **Timestamp-based Sync**: Saves the last processed timestamp and fetches all records updated since that timestamp.
- **Step-size Sync**: Saves the current ID and fetches records in ID ranges (e.g., IDs 1-1000, then 1001-2000). Useful when APIs don't support pagination or count.
- **Replay Sync**: Similar to timestamp-based sync but applies a buffer (e.g., 2 hours) to the last timestamp. Useful for read-replica scenarios where there might be replication lag.

## Tables Created

The connector syncs the data to table `user` in the destination.
This connector replicates data from the source's `user` to a destination table with the same name. The table has a composite primary key consisting of `id` only.

The schema of the table is as follows:

```json
{
   "table": "user",
   "primary_key": ["id"],
   "columns": {
      "id": "STRING",
      "name": "STRING",
      "email": "STRING",
      "address": "STRING",
      "company": "STRING",
      "job": "STRING",
      "updatedAt": "UTC_DATETIME",
      "createdAt": "UTC_DATETIME"
   }
}
```

## Notes
- This example is for educational purposes and uses dummy/mock data.
- See the code comments in `connector.py` for more details on each strategy.


## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
