# Elasticsearch Connector

## Connector overview

This connector syncs data from Elasticsearch and OpenSearch to your Fivetran destination using the Fivetran Connector SDK. It discovers all indices and data streams in your cluster and incrementally extracts documents, using `_seq_no` for regular indices and `@timestamp` for data streams. The connector supports Elasticsearch 7.12 through 9.x, Elastic Cloud (stateful and serverless), OpenSearch 2.4+, API key and Basic authentication, and opt-in delete detection. Distribution is detected automatically at runtime.

## Requirements

* [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
* Operating system:
  * Windows: 10 or later (64-bit only)
  * macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  * Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)
* Elasticsearch 7.12 or later (7.10–7.11 are not supported; the connector uses the `_shard_doc` sort tiebreaker introduced in 7.12), **or** OpenSearch 2.4 or later

## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connector-sdk/setup-guide) to get started.

To initialize a new Connector SDK project using this connector as a starting point, run:

```bash
fivetran init <project-path> --template connectors/elasticsearch
```

`fivetran init` initializes a new Connector SDK project by setting up the project structure, configuration files, and a connector you can run immediately with `fivetran debug`.
If you do not specify a project path, Fivetran creates the project in your current directory.
For more information on `fivetran init`, refer to the [Connector SDK `init` documentation](https://fivetran.com/docs/connector-sdk/setup-guide#createyourcustomconnector).

> Note: Ensure you have updated the `configuration.json` file with the necessary parameters before running `fivetran debug`. See the [Configuration file](#configuration-file) section for details on the required configuration parameters.

## Features

- Syncs regular Elasticsearch indices and data streams as separate destination tables
- Incremental sync using `_seq_no` for regular indices and `@timestamp` for data streams
- Supports Elasticsearch 7.12 through 9.x and Elastic Cloud Serverless
- API key and Basic Auth authentication
- Serverless-compatible index discovery via `GET /*/_mapping` (avoids blocked `_cat` endpoints)
- System index filtering (excludes `.`-prefixed indices, matching the existing connector's behavior)
- Selective index sync via the `indices` configuration field
- Opt-in delete detection via full document ID scan

## Configuration file

The connector requires the following configuration parameters:

```json
{
  "host": "https://<your-elasticsearch-host>:9200",
  "auth_method": "api_key",
  "api_key": "<your-base64-encoded-api-key>",
  "username": "<your-username>",
  "password": "<your-password>",
  "indices": "all",
  "enable_delete_detection": "false"
}
```

| Field | Required | Description |
|-------|----------|-------------|
| `host` | Yes | Elasticsearch base URL, e.g. `https://my-cluster.es.io:9200` |
| `auth_method` | Yes | `api_key` or `basic_auth` |
| `api_key` | If `auth_method=api_key` | Base64-encoded API key (the `encoded` value from the Create API key response) |
| `username` | If `auth_method=basic_auth` | Username |
| `password` | If `auth_method=basic_auth` | Password |
| `indices` | No | Comma-separated index/stream names to sync, or `all` (default) |
| `enable_delete_detection` | No | `true` to enable opt-in delete detection (default: `false`) |

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file

The connector requires the `requests` package.

```
requests
fivetran_connector_sdk
```

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication

The connector supports two authentication methods (refer to the `get_headers()` function):

**API key (recommended)** — required for Elastic Cloud Serverless:
1. In Kibana, go to Stack Management → API Keys → Create API key
2. Assign the following privileges:
   - Cluster: `monitor`
   - Indices: `read`, `view_index_metadata`, `monitor` on `*`
3. Copy the `encoded` value from the response and set it as `api_key` in `configuration.json`

**Basic auth** — not supported on Elastic Cloud Serverless:
1. Set `auth_method` to `basic_auth` in `configuration.json`
2. Provide `username` and `password`
3. Ensure the user has `monitor` cluster privilege and `read`, `view_index_metadata`, `monitor` index privileges on `*`

## Pagination

The connector uses **PIT (Point in Time) + `search_after`** for all document fetching (refer to the `pit_page()` function). This is Elasticsearch's recommended approach for deep pagination in v8 and v9, replacing the deprecated Scroll API. Key advantages:

- No persistent search context holding segment files open — lighter on the cluster
- No 2 GB per-response cap
- Works correctly across data stream backing indices
- Introduced in Elasticsearch 7.10; the `_shard_doc` tiebreaker used by this connector requires 7.12 or later

Page size is controlled by the `PAGE_SIZE` constant (default: 1,000 documents per page). The connector opens a PIT context at the start of each index sync and closes it when complete.

## Data handling

The connector handles data as follows (refer to the `update()` function):

**Regular indices** (refer to `sync_index()`):
- On initial sync, fetches all documents with `_seq_no` up to the current maximum
- On incremental sync, fetches only documents with `_seq_no` greater than the last synced value
- Checkpoints the max `_seq_no` after each sync

**Data streams** (refer to `sync_data_stream()`):
- On initial sync, fetches all documents
- On incremental sync, fetches only documents with `@timestamp` greater than the last synced timestamp
- Checkpoints the latest `@timestamp` after each sync
- `_seq_no` is not used for data streams because it resets when Elasticsearch rolls over to a new backing index

**Delete detection** (refer to `scan_all_ids()` and `sync_index()`):
- When `enable_delete_detection=true`, performs a full document ID scan after each index sync
- Compares the current ID set against the previous sync's ID set stored in connector state
- Emits `op.delete()` for any IDs that have disappeared, setting `_fivetran_deleted=true` in the destination
- Disabled by default due to memory and I/O cost for large indices
- Not supported for data streams, which are append-only by design

## Tables created

One table is created per discovered index or data stream. Table names correspond to the index or stream name. Each table includes:

- All mapped fields from the index mapping, typed according to the Elasticsearch → Fivetran type mapping below
- `_id` (STRING) — the Elasticsearch document ID, used as the primary key
- `_fivetran_deleted` (BOOLEAN) — set to `true` when delete detection marks a document as deleted
- `_fivetran_synced` (UTC_DATETIME) — the time the row was last synced

| Elasticsearch Type | Fivetran Type |
|--------------------|---------------|
| `text`, `keyword`, `constant_keyword`, `wildcard` | `STRING` |
| `integer`, `short`, `byte` | `INT` |
| `long`, `unsigned_long` | `LONG` |
| `double`, `float`, `half_float`, `scaled_float` | `DOUBLE` |
| `boolean` | `BOOLEAN` |
| `binary` | `BINARY` |
| `date`, `date_nanos` | `UTC_DATETIME` |
| `object`, `nested`, `flattened`, `join` | `JSON` |
| `alias`, `geo_*`, `dense_vector`, `*_range`, `ip` | Excluded |

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
