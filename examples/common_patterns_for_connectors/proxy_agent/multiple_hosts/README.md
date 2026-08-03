a# Proxy Agent - Multiple Hosts Connector Example

## Connector overview
This example demonstrates how to connect to a PostgreSQL instance behind a [Fivetran Proxy Agent](https://fivetran.com/docs/core-concepts/architecture/hybrid-deployment) when **multiple candidate hosts** are available. The connector reads a comma-separated list of hosts from the `hosts` key in `configuration.json` and attempts to connect to each in order, using the first host that accepts the connection.

Typical use cases:
- Multiple Fivetran Proxy Agents fronting the same PostgreSQL instance for high availability.
- A primary host with one or more read replicas that share the same credentials.
- Region-specific hosts where the connector should prefer a specific ordering.

## Requirements
- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)
- A PostgreSQL instance (or replicas) reachable from the Fivetran Proxy Agent(s)
- One or more Fivetran Proxy Agents installed and registered in your Fivetran account

## Getting started
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connector-sdk/setup-guide) to get started.

To initialize a new Connector SDK project using this connector as a starting point, run:

```bash
fivetran init <project-path> --template examples/common_patterns_for_connectors/proxy_agent/multiple_hosts
```

`fivetran init` initializes a new Connector SDK project by setting up the project structure, configuration files, and a connector you can run immediately with `fivetran debug`.

> Note: Ensure you have updated the `configuration.json` file with the necessary parameters before running `fivetran debug`. See the [Configuration file](#configuration-file) section for details on the required configuration parameters.

## Deploying the connector
Once you have tested the connector locally with `fivetran debug`, deploy it to your Fivetran account using the `fivetran deploy` command:

```bash
fivetran deploy \
  --api-key <YOUR_FIVETRAN_API_KEY_BASE64> \
  --destination <YOUR_DESTINATION_NAME> \
  --connection <YOUR_CONNECTION_NAME> \
  --configuration configuration.json \
  --proxy-id <YOUR_FIVETRAN_PROXY_AGENT_ID>
```

- `--api-key`: Your base64-encoded Fivetran API key (`echo -n "API_KEY:API_SECRET" | base64`). You can also set the `FIVETRAN_API_KEY` environment variable and omit this flag.
- `--destination`: The name of the destination in your Fivetran account where this connector will load data.
- `--connection`: The name to assign to the connection in Fivetran. Use a new name for a fresh deployment, or an existing name to update it in place.
- `--configuration`: Path to the `configuration.json` file. The `hosts` value must be a JSON array of `hostname:port` strings.
- `--proxy-id`: The identifier of the Fivetran Proxy Agent to associate with this connection. If you use multiple proxy agents for failover, pass the primary agent ID here and associate the additional agents with the connection from the Fivetran dashboard.

Refer to the [Connector SDK `deploy` documentation](https://fivetran.com/docs/connector-sdk/setup-guide#deployyourconnectortofivetran) for the full list of options.

## Features
- PostgreSQL connectivity via `psycopg2`
- Accepts a comma-separated list of hosts and iterates in order
- Uses a 10-second `connect_timeout` per host to fail fast on unreachable hosts
- Incremental sync using the `modified_at` column
- Periodic checkpointing every 1000 records

## How host failover works
1. `validate_configuration` ensures the `hosts` value is a JSON array and parses into at least one host.
2. `parse_hosts` iterates over the array, trims each entry, and splits `hostname:port`, preserving order.
3. `get_database_connection` iterates over the parsed list:
   - Attempts `psycopg2.connect(host=<current>, ...)` with a 10-second timeout.
   - On success, logs the connected host and returns the connection.
   - On failure, logs a warning and moves to the next host.
4. If every host fails, a `ConnectionError` is raised so Fivetran can retry the sync.

Because the connector always tries hosts in configured order, place your preferred host first (for example, the primary or the closest proxy agent).

## Configuration file
The connector requires the following configuration parameters:

```json
{
  "hosts": ["<PRIMARY_HOST>:<PORT>", "<SECONDARY_HOST>:<PORT>", "<TERTIARY_HOST>:<PORT>"],
  "db_user": "<YOUR_POSTGRES_USER>",
  "db_password": "<YOUR_POSTGRES_PASSWORD>",
  "db_name": "<YOUR_POSTGRES_DATABASE_NAME>"
}
```

- `hosts`: JSON array of PostgreSQL addresses in `hostname:port` format (for example, `["primary.internal:5432", "replica.internal:5433"]`). The connector tries entries in the order given. If a port is omitted, `5432` is used.
- `db_user`: PostgreSQL username shared across all hosts.
- `db_password`: PostgreSQL password shared across all hosts.
- `db_name`: Name of the database to connect to.

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
1. Reads `last_modified` from `state` (defaults to `1970-01-01T00:00:00Z` on the first sync).
2. Executes a parameterized `SELECT` on `sample_users` for rows where `modified_at > last_modified` on the connected host.
3. Upserts each row to the `sample_users` destination table.
4. Checkpoints every 1000 rows and performs a final checkpoint after all rows.

## Error handling
- Missing configuration parameters (including an empty `hosts`) raise a `ValueError` via `validate_configuration`.
- Per-host connection failures are logged with `log.warning` and do not stop the sync as long as another host succeeds.
- If all hosts fail, the connector raises a `ConnectionError` so Fivetran can retry on the next scheduled sync.

## Tables created
| Table | Primary key | Description |
| --- | --- | --- |
| `sample_users` | `id` | Rows fetched incrementally from the source `sample_users` table on the first reachable host. |

## Additional considerations
- Ensure every host in the list contains the same data (or at least the same schema for `sample_users`) so that failover does not produce inconsistent results.
- If you rely on read replicas, make sure replication lag is acceptable for your incremental cursor (`modified_at`).
- For production deployments, prefer TLS-enabled PostgreSQL connections by passing `sslmode="require"` to `psycopg2.connect()`.
- Consider extending the connector to remember the last successful host in `state` to bias future syncs, if that fits your operational model.

The examples provided are meant to help you get started with Fivetran's Connector SDK. While the connector has been tested, Fivetran is not responsible for any issues resulting from its use. For support, contact the Fivetran Support team.
