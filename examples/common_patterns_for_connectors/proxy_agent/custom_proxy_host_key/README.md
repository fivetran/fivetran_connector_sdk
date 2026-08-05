# Proxy Agent - Custom Proxy Host Key Connector Example

## Connector overview
This example demonstrates how to connect to a PostgreSQL instance with [Fivetran Proxy Agent](https://fivetran.com/docs/core-concepts/architecture/hybrid-deployment)

The connector logic is identical to `proxy_agent/simple_postgres_connection`, except that the PostgreSQL host is read from the `proxy_host` key in `configuration.json`. Using a custom key makes it clear which source host should be used with the Fivetran Proxy Agent. This can be helpful when your team uses naming conventions such as `proxy_host`, `on_prem_host`, or `postgres_host`.

## Requirements
- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)
- A PostgreSQL instance reachable from the Fivetran Proxy Agent
- A Fivetran Proxy Agent installed and registered in your Fivetran account

## Getting started
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connector-sdk/setup-guide) to get started.

To initialize a new Connector SDK project using this connector as a starting point, run:

```bash
fivetran init --template examples/common_patterns_for_connectors/proxy_agent/custom_proxy_host_key
```

`fivetran init` initializes a new Connector SDK project by setting up the project structure, configuration files, and a connector.


## Deploying the connector
Deploy connector to your Fivetran account using the `fivetran deploy` command:

```bash
fivetran deploy \
  --api-key <YOUR_FIVETRAN_API_KEY_BASE64> \
  --destination <YOUR_DESTINATION_NAME> \
  --connection <YOUR_CONNECTION_NAME> \
  --configuration configuration.json \
  --proxy-id <YOUR_FIVETRAN_PROXY_AGENT_ID> \
  --proxy-host-config-key proxy_host
```

- `--api-key`: Your base64-encoded Fivetran API key and secret pair (`echo -n "API_KEY:API_SECRET" | base64`). You can also set the `FIVETRAN_API_KEY` environment variable and omit this flag.
- `--destination`: The name of the destination in your Fivetran account where this connector will load data.
- `--connection`: The name to assign to the connection in Fivetran. Use a new name for a fresh deployment, or an existing name to update it in place.
- `--configuration`: Path to the `configuration.json` file with the connector's configuration values. Ensure the key matches the value defined by `__PROXY_HOST_CONFIG_KEY` in `connector.py`.
- `--proxy-id`: The identifier of the Fivetran Proxy Agent to associate with this connection.
- `--proxy-host-config-key`: The name of the configuration key in `configuration.json` that holds the source host address.

If your `configuration.json` already uses `host` or `hosts` for the source host details, you do not need to pass `--proxy-host-config-key` during deployment. This argument is optional and is only needed when you use a custom key name such as `proxy_host`. Refer to the `simple_postgres_connection` example for a standard configuration example.

Refer to the [Connector SDK `deploy` documentation](https://fivetran.com/docs/connector-sdk/setup-guide#deployyourconnectortofivetran) for the full list of options.

## Features
- PostgreSQL connectivity via `psycopg2`
- Uses a custom `proxy_host` configuration key instead of the generic `host` key
- Full table sync using a server-side named cursor with `fetchmany()` for memory-safe streaming
- Periodic checkpointing every 1000 records

## How to use a custom proxy host key
The Fivetran Proxy Agent forwards TCP traffic from Fivetran to your private data source. To route your PostgreSQL connection through the proxy agent using this example:

1. Install and register the Fivetran Proxy Agent in your network following the Fivetran documentation.
2. Identify the source PostgreSQL host and port that the proxy agent should forward traffic to inside your private network.
3. Populate `configuration.json` with that source address under the  custom key ex: `proxy_host`:
   ```json
   {
       "proxy_host": "<YOUR_POSTGRES_HOST>:<YOUR_POSTGRES_PORT>",
       "db_user": "<YOUR_POSTGRES_USER>",
       "db_password": "<YOUR_POSTGRES_PASSWORD>",
       "db_name": "<YOUR_POSTGRES_DATABASE_NAME>"
   }
   ```

At runtime the connector reads `configuration["proxy_host"]` and passes it as the `host` argument to `psycopg2.connect(...)`. The proxy agent handles the network hop into your private environment; no additional code change is required.

## Configuration file
The connector requires the following configuration parameters:

```json
{
  "proxy_host": "<YOUR_POSTGRES_HOST>:<YOUR_POSTGRES_PORT>",
  "db_user": "<YOUR_POSTGRES_USER>",
  "db_password": "<YOUR_POSTGRES_PASSWORD>",
  "db_name": "<YOUR_POSTGRES_DATABASE_NAME>"
}
```

- `proxy_host` (required): PostgreSQL address in `hostname:port` format (for example, `db.internal:5432`). When using the Fivetran Proxy Agent, this should be the address of the database as it is reachable from the machine running the proxy agent. If the port is omitted, `5432` is used.
- `db_user` (required): PostgreSQL username.
- `db_password` (required): PostgreSQL password.
- `db_name` (required): Name of the database to connect to.

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file
This connector uses `psycopg2-binary` to connect to PostgreSQL:

```
psycopg2-binary==2.9.9
```

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication
Standard PostgreSQL username/password authentication. Credentials are read from `configuration.json` and passed to `psycopg2.connect()`.

## Data handling
1. Opens a PostgreSQL connection via the proxy host address in `configuration["proxy_host"]`.
2. Opens a named server-side cursor to stream rows from the `TEST` table in batches of 1000, avoiding loading the full result set into memory.
3. Upserts each row to the `TEST` destination table.
4. Checkpoints state (with `total_rows` count) after every batch of 1000 rows.

## Error handling
- Missing configuration parameters (including `proxy_host`) raise a `ValueError` via `validate_configuration`.
- PostgreSQL connection failures are logged with `log.error` and re-raised so Fivetran can retry.

## Tables created
| Table | Primary key | Description |
| --- | --- | --- |
| `TEST` | `id` | All rows streamed from the source `TEST` table via the proxy agent. |

## Additional considerations
- This example assumes the source table is named `TEST` and has an `id` column. Adjust `__TABLE_NAME` in `connector.py` and the schema definition for your source.
- Make sure the proxy agent has network access to your PostgreSQL instance.
- Prefer TLS-enabled PostgreSQL connections by passing `sslmode="require"` to `psycopg2.connect()` for production deployments.
- Keep the custom key name consistent across your team's connectors to make configurations easier to review.
- `fivetran debug` runs locally and does not route through the Proxy Agent, so it cannot validate end-to-end Proxy Agent connectivity. To test connectivity, either deploy the connection with `--proxy-id` and run a sync, or run `fivetran debug` from within your private network where the data source is directly reachable.

The examples provided are meant to help you get started with Fivetran's Connector SDK. While the connector has been tested, Fivetran is not responsible for any issues resulting from its use. For support, contact the Fivetran Support team.
