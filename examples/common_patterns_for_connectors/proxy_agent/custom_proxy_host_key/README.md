# Proxy Agent - Custom Proxy Host Key Connector Example

## Connector overview
This example demonstrates how to connect to a PostgreSQL instance behind a [Fivetran Proxy Agent](https://fivetran.com/docs/core-concepts/architecture/hybrid-deployment) using a **custom configuration key** for the proxy host, rather than the generic `host` key.

The connector logic is identical to `proxy_agent/simple_postgres_connection`, except that the PostgreSQL host is read from the `proxy_host` key in `configuration.json`. Using a domain-specific key makes it explicit that the address points to the Fivetran Proxy Agent (and not to the underlying database directly), which can be helpful when:
- Your team uses naming conventions such as `proxy_host`, `on_prem_proxy_host`, or `fivetran_proxy_agent_host`.
- The same configuration schema is shared across multiple connectors and needs to distinguish proxied hosts from direct hosts.
- You want the configuration UI/JSON to self-document that traffic flows through the proxy agent.

If your naming convention differs, change the `__PROXY_HOST_CONFIG_KEY` constant at the top of `connector.py`.

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
fivetran init <project-path> --template examples/common_patterns_for_connectors/proxy_agent/custom_proxy_host_key
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
  --proxy-id <YOUR_FIVETRAN_PROXY_AGENT_ID> \
  --proxy-host-config-key proxy_host
```

- `--api-key`: Your base64-encoded Fivetran API key (`echo -n "API_KEY:API_SECRET" | base64`). You can also set the `FIVETRAN_API_KEY` environment variable and omit this flag.
- `--destination`: The name of the destination in your Fivetran account where this connector will load data.
- `--connection`: The name to assign to the connection in Fivetran. Use a new name for a fresh deployment, or an existing name to update it in place.
- `--configuration`: Path to the `configuration.json` file with the connector's configuration values. Ensure the key matches the value defined by `__PROXY_HOST_CONFIG_KEY` in `connector.py`.
- `--proxy-id`: The identifier of the Fivetran Proxy Agent to associate with this connection.
- `--proxy-host-config-key`: The name of the configuration key in `configuration.json` that holds the proxy host address (`proxy_host` by default). If you renamed `__PROXY_HOST_CONFIG_KEY` in `connector.py`, pass the same name here.

Refer to the [Connector SDK `deploy` documentation](https://fivetran.com/docs/connector-sdk/setup-guide#deployyourconnectortofivetran) for the full list of options.

## Features
- PostgreSQL connectivity via `psycopg2`
- Uses a custom `proxy_host` configuration key instead of the generic `host` key
- Incremental sync using the `modified_at` column
- Periodic checkpointing every 1000 records

## How to use a custom proxy host key
The Fivetran Proxy Agent forwards TCP traffic from Fivetran to your private data source. To route your PostgreSQL connection through the proxy agent using this example:

1. **Install and register the Fivetran Proxy Agent** in your network following the Fivetran documentation.
2. **Identify the address exposed by the proxy agent** for your PostgreSQL instance. This is the hostname/IP that Fivetran-hosted connectors should target.
3. **Populate `configuration.json`** with that address under the `proxy_host` key:
   ```json
   {
     "proxy_host": "fivetran-proxy.mycompany.internal:5432",
     "db_user": "fivetran_reader",
     "db_password": "<YOUR_POSTGRES_PASSWORD>",
     "db_name": "analytics"
   }
   ```
4. **(Optional) Rename the key** by editing `__PROXY_HOST_CONFIG_KEY` in `connector.py` (for example to `on_prem_proxy_host`) and updating `configuration.json` to match.
5. **Run** `fivetran debug` to validate the connection, then `fivetran deploy` when ready.

At runtime the connector reads `configuration["proxy_host"]` and passes it as the `host` argument to `psycopg2.connect(...)`. The proxy agent handles the network hop into your private environment; no additional application-level proxy configuration is required.

## Configuration file
The connector requires the following configuration parameters:

```json
{
  "proxy_host": "<YOUR_FIVETRAN_PROXY_AGENT_HOST>:<YOUR_POSTGRES_PORT>",
  "db_user": "<YOUR_POSTGRES_USER>",
  "db_password": "<YOUR_POSTGRES_PASSWORD>",
  "db_name": "<YOUR_POSTGRES_DATABASE_NAME>"
}
```

- `proxy_host`: Address exposed by the Fivetran Proxy Agent in `hostname:port` format (for example, `fivetran-proxy.mycompany.internal:5432`). If the port is omitted, `5432` is used.
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
Standard PostgreSQL username/password authentication. Credentials are read from `configuration.json` and passed to `psycopg2.connect()`. The Fivetran Proxy Agent is authenticated separately with Fivetran as part of its registration.

## Data handling
1. Reads `last_modified` from `state` (defaults to `1970-01-01T00:00:00Z` on the first sync).
2. Executes a parameterized `SELECT` on `sample_users` for rows where `modified_at > last_modified`.
3. Upserts each row to the `sample_users` destination table.
4. Checkpoints every 1000 rows and performs a final checkpoint after all rows.

## Error handling
- Missing configuration parameters (including `proxy_host`) raise a `ValueError` via `validate_configuration`.
- PostgreSQL connection failures are logged with `log.error` and re-raised so Fivetran can retry.

## Tables created
| Table | Primary key | Description |
| --- | --- | --- |
| `sample_users` | `id` | Rows fetched incrementally from the source `sample_users` table via the proxy agent. |

## Additional considerations
- Make sure the proxy agent has network access to your PostgreSQL instance and that outbound HTTPS to Fivetran is allowed from the machine running the proxy agent.
- Prefer TLS-enabled PostgreSQL connections by passing `sslmode="require"` to `psycopg2.connect()` for production deployments.
- Keep the custom key name consistent across your team's connectors to make configurations easier to review.

The examples provided are meant to help you get started with Fivetran's Connector SDK. While the connector has been tested, Fivetran is not responsible for any issues resulting from its use. For support, contact the Fivetran Support team.
