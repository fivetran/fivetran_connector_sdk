# Proxy Agent - Multiple Hosts Connector Example

## Connector overview
This example demonstrates how to connect to a PostgreSQL instance with the [Fivetran Proxy Agent](https://fivetran.com/docs/connectors/databases/connection-options/proxy-agent) when multiple candidate hosts are available. The connector reads the `hosts` key from `configuration.json`, connects to each configured host in order, and extracts data from every reachable host.

## Requirements
- [Supported Python versions](https://github.com/fivetran/connector_sdk/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)
- A PostgreSQL instance (or replicas) reachable from the Fivetran Proxy Agent(s)
- Fivetran Proxy Agents installed and registered in your Fivetran account

## Getting started
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connector-sdk/setup-guide) to get started.

To initialize a new Connector SDK project using this connector as a starting point, run:

```bash
fivetran init --template examples/common_patterns_for_connectors/proxy_agent/multiple_hosts
```

`fivetran init` initializes a new Connector SDK project by setting up the project structure, configuration files, and a connector.

## Deploying the connector
Deploy it to your Fivetran account using the `fivetran deploy` command:

```bash
fivetran deploy \
  --api-key <YOUR_FIVETRAN_API_KEY_BASE64> \
  --destination <YOUR_DESTINATION_NAME> \
  --connection <YOUR_CONNECTION_NAME> \
  --configuration configuration.json \
  --proxy-id <YOUR_FIVETRAN_PROXY_AGENT_ID>
```

- `--api-key`: Your base64-encoded Fivetran API key and secret pair (`echo -n "API_KEY:API_SECRET" | base64`). You can also set the `FIVETRAN_API_KEY` environment variable and omit this flag.
- `--destination`: The name of the destination in your Fivetran account where this connector will load data.
- `--connection`: The name to assign to the connection in Fivetran. Use a new name for a fresh deployment, or an existing name to update it in place.
- `--configuration`: Path to the `configuration.json` file. The `hosts` value can be provided as either a JSON array or a comma-separated string of `hostname:port` entries. A JSON array is preferred in `configuration.json`.
- `--proxy-id`: The identifier of the Fivetran Proxy Agent to associate with this connection.

Refer to the [Connector SDK `deploy` documentation](https://fivetran.com/docs/connector-sdk/setup-guide#deployyourconnectortofivetran) for the full list of options.

## Features
- PostgreSQL connectivity via `psycopg2`
- Accepts multiple `hostname:port` host entries and iterates in order
- Uses a 10-second `connect_timeout` per host to fail fast on unreachable hosts
- Full table sync using a server-side named cursor with `fetchmany()` for memory-safe streaming
- Periodic checkpointing every 1000 records

## How multi-host sync works
This connector does **not** implement failover (it does not stop after the first successful
connection). It attempts every configured host and syncs from **each one that is reachable**:

1. `validate_configuration` ensures the `hosts` value parses into at least one host entry.
2. `parse_hosts` reads the configured host entries and parses comma-separated `hostname:port` values while preserving order. In `configuration.json`, you can provide `hosts` either as a JSON array or as a comma-separated string, though a JSON array is preferred.
3. `update()` iterates over the parsed list and, for each host:
   - Calls `get_database_connection`, which attempts `psycopg2.connect(host=<current>, ...)` with a 10-second timeout.
   - On success, logs the connected host and proceeds to sync from it via `fetch_and_upsert_data`.
   - On failure, logs a warning and moves to the next host — that host is skipped for this sync.
4. If every host fails, a `ConnectionError` is raised so Fivetran can retry the sync.


## Configuration file
The connector requires the following configuration parameters:

```json
{
  "hosts": ["<PRIMARY_SOURCE_HOST>:<PORT>", "<SECONDARY_SOURCE_HOST>:<PORT>", "<TERTIARY_SOURCE_HOST>:<PORT>"],
  "db_user": "<YOUR_POSTGRES_USER>",
  "db_password": "<YOUR_POSTGRES_PASSWORD>",
  "db_name": "<YOUR_POSTGRES_DATABASE_NAME>"
}
```

- `hosts` (required): PostgreSQL addresses in `hostname:port` format. In `configuration.json`, you can provide this as a JSON array (for example, `["primary.internal:5432", "replica.internal:5433"]`). At runtime, the connector can also read the same values as a comma-separated string. The connector tries entries in the order given. If a port is omitted, `5432` is used.
- `db_user` (required): Your PostgreSQL username shared across all hosts.
- `db_password` (required): Your PostgreSQL password shared across all hosts.
- `db_name` (required): Name of the database to connect to.

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file
This connector uses `psycopg2-binary` to connect to PostgreSQL:

```
psycopg2-binary==2.9.9
```

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication
Standard PostgreSQL username/password authentication is used for all hosts. Credentials are read from `configuration.json` and passed to `psycopg2.connect()`. Each Fivetran Proxy Agent authenticates itself to Fivetran independently as part of its registration.

## Data handling
1. Connects to every reachable host in the `hosts` list (not just the first).
2. For each reachable host, opens a named server-side cursor to stream rows from the `TEST` table in batches of 1000, avoiding loading the full result set into memory.
3. Upserts each row to the `TEST` destination table.
4. Checkpoints state (with `total_rows` count) after every batch of 1000 rows.

## Error handling
- Missing configuration parameters (including an empty `hosts`) raise a `ValueError` via `validate_configuration`.
- Per-host connection failures are logged with `log.warning` and do not stop the sync as long as another host succeeds.
- If all hosts fail, the connector raises a `ConnectionError` so Fivetran can retry on the next scheduled sync.

## Tables created
| Table | Primary key | Description |
| --- | --- | --- |
| `TEST` | `id` | All rows streamed from the source `TEST` table on every reachable host. |

## Additional considerations
- This example assumes the source table is named `TEST` and has an `id` column. Adjust `__TABLE_NAME` in `connector.py` and the schema definition for your source.
- For production deployments, prefer TLS-enabled PostgreSQL connections by passing `sslmode="require"` to `psycopg2.connect()`.
- Because rows from every reachable host are upserted using only `id` as the primary key, hosts whose `TEST` tables can contain the same `id` for different underlying rows (for example, independent shards) will silently overwrite each other's data in the destination. If your hosts don't share a globally unique `id` space, extend the primary key (and upserted row data) to include a per-host discriminator, such as the hostname.

The examples provided are meant to help you get started with Fivetran's Connector SDK. While the connector has been tested, Fivetran is not responsible for any issues resulting from its use. For support, contact the Fivetran Support team.
