# Proxy Agent Examples

## Overview

These examples show how to connect a Connector SDK connector to a private PostgreSQL source through the Fivetran Proxy Agent.

The Proxy Agent runs inside your private network and forwards traffic between Fivetran-hosted connectors and your source system. In these examples, the connector passes the source PostgreSQL host details to `psycopg2.connect(...)`, and the Proxy Agent handles the network path to that source.

---

## Getting started

Choose a Proxy Agent example to get started:

- [simple_postgres_connection](simple_postgres_connection/): Use one PostgreSQL source with a single `host` entry in `configuration.json`.
- [multiple_hosts](multiple_hosts/): Use multiple source hosts when you want to try more than one PostgreSQL host during a sync.
- [custom_proxy_host_key](custom_proxy_host_key/): Use a custom configuration key such as `proxy_host` instead of the standard `host` key for the source host details.

## How it works

These examples follow the same basic flow:

1. Read the PostgreSQL host details and credentials from `configuration.json`.
2. Validate the configured host values before attempting to connect.
3. Open a PostgreSQL connection with `psycopg2`.
4. Read rows from the source `TEST` table using a server-side cursor.
5. Upsert rows into the destination table and checkpoint state periodically.

When the connection is deployed with `--proxy-id`, Fivetran routes the connector traffic through the Proxy Agent associated with that connection.

---

## Common notes

- The configured `host`, `hosts`, or custom host key should contain the source PostgreSQL address in `hostname:port` format.
- To test Proxy Agent connectivity, either deploy the connection with `--proxy-id` and run a sync, or run `fivetran debug` from within your private network where the source is directly reachable.
- If you use a custom configuration key for source host details, pass `--proxy-host-config-key` during deployment. If you use the standard `host` or `hosts` keys, that extra argument is not needed.
