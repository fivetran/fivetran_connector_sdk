from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Operations as op
from fivetran_connector_sdk import Logging as log

from datetime import timedelta
import json
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions, QueryOptions

def create_couchbase_client(configuration: dict):
    """
    Create a Couchbase client for on-prem or cloud-hosted Couchbase Server.
    Supports both TLS and non-TLS endpoints based on configuration.
    """
    username = configuration.get("username")
    password = configuration.get("password")
    endpoint = configuration.get("endpoint")          # e.g. "localhost", "192.168.0.100"
    bucket_name = configuration.get("bucket_name")
    use_tls_str = configuration.get("use_tls", "false").strip().lower()     # Optional flag to use TLS
    use_tls = use_tls_str == "true"
    cert_path = configuration.get("cert_path")        # Optional path to CA cert

    try:
        auth = PasswordAuthenticator(username, password)
        options = ClusterOptions(auth)

        # Use secure connection if use_tls is true
        if use_tls:
            connection_string = f"couchbases://{endpoint}"
            if not cert_path:
                raise ValueError("TLS is enabled but cert_path is not provided.")
            cluster = Cluster(connection_string, options, cert_path=cert_path)
        else:
            connection_string = f"couchbase://{endpoint}"
            cluster = Cluster(connection_string, options)

        cluster.wait_until_ready(timedelta(seconds=5))
        cluster_bucket = cluster.bucket(bucket_name)
        log.info(f"Connected to bucket '{bucket_name}' at '{connection_string}' successfully.")
        return cluster_bucket

    except Exception as e:
        raise RuntimeError(f"Failed to connect to Couchbase: {e}")


def execute_query_and_upsert(client, scope, collection, query, table_name, state):
    client_scope = client.scope(scope)
    count = 0
    checkpoint_interval = 1000

    try:
        row_iter = client_scope.query(query, QueryOptions(metrics=False))
        for row in row_iter:
            row_data = row.get(collection)
            yield op.upsert(table=table_name, data=row_data)
            count += 1

            if count % checkpoint_interval == 0:
                yield op.checkpoint(state)

    except Exception as e:
        raise RuntimeError(f"Failed to fetch and upsert data: {e}")

    log.info(f"Upserted {count} records into {table_name} successfully.")


def schema(configuration: dict):
    for key in ["endpoint", "username", "password", "bucket_name", "scope", "collection"]:
        if key not in configuration:
            raise ValueError(f"Missing required configuration value: {key}")

    return [
        {
            "table": "airline_table",
            "primary_key": ["id"],
            "columns": {
                "id": "INT",
                "name": "STRING",
                "country": "STRING",
                "type": "STRING",
                "callsign": "STRING",
                "iata": "STRING",
                "icao": "STRING"
            },
        }
    ]


def update(configuration, state):
    client = create_couchbase_client(configuration)
    scope = configuration.get("scope")
    collection = configuration.get("collection")
    table_name = "airline_table"
    sql_query = f"SELECT * FROM `{collection}`"

    yield from execute_query_and_upsert(
        client=client,
        scope=scope,
        collection=collection,
        query=sql_query,
        table_name=table_name,
        state=state,
    )

    yield op.checkpoint(state)


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    with open("configuration.json", "r") as f:
        configuration = json.load(f)
    connector.debug(configuration=configuration)
