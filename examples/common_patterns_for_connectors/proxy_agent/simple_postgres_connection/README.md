# Proxy Agent - Simple Postgres Connection Connector Example

## Connector overview
This example demonstrates how to connect to a PostgreSQL instance from a Fivetran Connector SDK connector when running behind the [Fivetran Proxy Agent](https://fivetran.com/docs/core-concepts/architecture/hybrid-deployment). The connector reads PostgreSQL connection parameters from `configuration.json` where `host` is a single entry in the format `hostname:port`. It performs an incremental sync of the `sample_users` table using the `modified_at` column.

The Fivetran Proxy Agent runs inside your network and forwards traffic between Fivetran-hosted connectors and your private data sources. Because the proxy agent operates transparently at the network layer, your connector code does not need to know about it. You only need to make sure the `host` you configure is reachable through the proxy agent from the Fivetran environment.

## Requirements
- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)
- A PostgreSQL instance reachable from the Fivetran Proxy Agent
- A Fivetran Proxy Agent installed and registered in your Fivetran account (only required for production deployments)

## Getting started
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connector-sdk/setup-guide) to get started.

To initialize a new Connector SDK project using this connector as a starting point, run:

```bash
fivetran init <project-path> --template examples/common_patterns_for_connectors/proxy_agent/simple_postgres_connection
```

`fivetran init` initializes a new Connector SDK project by setting up the project structure, configuration files, and a connector you can run immediately with `fivetran debug`.
If you do not specify a project path, Fivetran creates the project in your current directory.

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
- `--configuration`: Path to the `configuration.json` file with the connector's configuration values.
- `--proxy-id`: The identifier of the Fivetran Proxy Agent to associate with this connection. Traffic to `host` will be routed through this proxy agent.

Refer to the [Connector SDK `deploy` documentation](https://fivetran.com/docs/connector-sdk/setup-guide#deployyourconnectortofivetran) for the full list of options.

## Features
- Direct PostgreSQL connectivity using `psycopg2`
- Standard host/port/user/password/database configuration
- Incremental sync using the `modified_at` column
- Periodic checkpointing every 1000 records
- Compatible with the Fivetran Proxy Agent without any code changes

## Configuration file
The connector requires the following configuration parameters:

```json
{
  "host": "<YOUR_POSTGRES_HOST>:<YOUR_POSTGRES_PORT>",
  "db_user": "<YOUR_POSTGRES_USER>",
  "db_password": "<YOUR_POSTGRES_PASSWORD>",
  "db_name": "<YOUR_POSTGRES_DATABASE_NAME>"
}
```

- `host`: PostgreSQL address in `hostname:port` format (for example, `db.internal:5432`). When using the Fivetran Proxy Agent, this should be the address of the database as it is reachable from the machine running the proxy agent. If the port is omitted, `5432` is used.
- `db_user`: PostgreSQL username.
- `db_password`: PostgreSQL password.
- `db_name`: Name of the database to connect to.

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file
This connector uses `psycopg2-binary` to connect to PostgreSQL:

```
psycopg2-binary==2.9.9
```

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication
This connector uses standard PostgreSQL username/password authentication. Credentials are read from `configuration.json` and passed directly to `psycopg2.connect()`. No additional authentication step is required for the Fivetran Proxy Agent; the proxy agent authenticates itself to Fivetran using its own registration credentials.

## Data handling
The connector processes data as follows:
1. Reads `last_modified` from `state` (defaults to `1970-01-01T00:00:00Z` on the first sync).
2. Executes a parameterized `SELECT` on `sample_users` for rows where `modified_at > last_modified`.
3. Streams rows using `psycopg2.extras.RealDictCursor` and upserts each row to the `sample_users` destination table.
4. Updates the in-memory `last_modified` cursor and checkpoints every 1000 rows.
5. Performs a final checkpoint after all rows have been processed.

## Error handling
The connector includes error handling for:
- Missing configuration parameters (raised by `validate_configuration`).
- PostgreSQL connection failures (caught and re-raised with `log.error`).
- Any exceptions during data fetch/upsert are surfaced to Fivetran so the sync can be retried.

## Tables created
| Table | Primary key | Description |
| --- | --- | --- |
| `sample_users` | `id` | Rows fetched incrementally from the source `sample_users` table using `modified_at`. |

## Additional considerations
- This example assumes the source table is named `sample_users` and has an `id` column and a `modified_at` timestamp column. Adjust the schema and SQL query for your source.
- When using the Fivetran Proxy Agent, make sure the machine running the agent has network access to the PostgreSQL host and that outbound HTTPS from the proxy agent to Fivetran is allowed.
- For production deployments, prefer TLS-enabled PostgreSQL connections by passing `sslmode="require"` (or stricter) to `psycopg2.connect()`.

The examples provided are meant to help you get started with Fivetran's Connector SDK. While the connector has been tested, Fivetran is not responsible for any issues resulting from its use. For support, contact the Fivetran Support team.
