# Proxy Agent - Simple Postgres Connection Connector Example

## Connector overview
This example demonstrates how to connect to a PostgreSQL instance from a Fivetran Connector SDK connector with the [Fivetran Proxy Agent](https://fivetran.com/docs/connectors/databases/connection-options/proxy-agent). The connector reads PostgreSQL connection parameters from `configuration.json` where `host` is a single entry in the format `hostname:port`. It performs a full sync of the `TEST` table using a server-side streaming cursor.

## Requirements
- [Supported Python versions](https://github.com/fivetran/connector_sdk/blob/main/README.md#requirements)
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
fivetran init --template examples/common_patterns/proxy_agent/simple_postgres_connection
```

`fivetran init` initializes a new Connector SDK project by setting up the project structure, configuration files, and a connector.
If you do not specify a project path, Fivetran creates the project in your current directory.

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
- `--configuration`: Path to the `configuration.json` file with the connector's configuration values.
- `--proxy-id`: The identifier of the Fivetran Proxy Agent to associate with this connection. Traffic to `host` will be routed through this proxy agent.

Refer to the [Connector SDK `deploy` documentation](https://fivetran.com/docs/connector-sdk/setup-guide#deployyourconnectortofivetran) for the full list of options.

## Features
- Direct PostgreSQL connectivity using `psycopg2`
- Standard host/port/user/password/database configuration
- Full table sync using a server-side named cursor with `fetchmany()` for memory-safe streaming
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

- `host` (required): PostgreSQL address in `hostname:port` format (for example, `db.internal:5432`). When using the Fivetran Proxy Agent, this should be the address of the database as it is reachable from the machine running the proxy agent. If the port is omitted, `5432` is used.
- `db_user` (required): Your PostgreSQL username.
- `db_password` (required): Your PostgreSQL password.
- `db_name` (required): Name of the database to connect to.

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
1. Opens a PostgreSQL connection using the configured `host`, credentials, and `sslmode=disable`.
2. Opens a named server-side cursor to stream rows from the `TEST` table in batches of 1000, avoiding loading the full result set into memory.
3. Upserts each row to the `TEST` destination table.
4. Checkpoints state (with `total_rows` count) after every batch of 1000 rows.

## Error handling
The connector includes error handling for:
- Missing configuration parameters (raised by `validate_configuration`).
- PostgreSQL connection failures (caught and re-raised with `log.error`).
- Any exceptions during data fetch/upsert are surfaced to Fivetran so the sync can be retried.

## Tables created
| Table | Primary key | Description |
| --- | --- | --- |
| `TEST` | `id` | All rows streamed from the source `TEST` table via a server-side cursor. |

## Additional considerations
- This example assumes the source table is named `TEST` and has an `id` column. Adjust `__TABLE_NAME` in `connector.py` and the schema definition for your source.
- When using the Fivetran Proxy Agent, make sure the machine running the agent has network access to the PostgreSQL host.
- For production deployments, prefer TLS-enabled PostgreSQL connections by passing `sslmode="require"` (or stricter) to `psycopg2.connect()`.

The examples provided are meant to help you get started with Fivetran's Connector SDK. While the connector has been tested, Fivetran is not responsible for any issues resulting from its use. For support, contact the Fivetran Support team.
