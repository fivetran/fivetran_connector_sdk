"""JanusGraph Connector for Fivetran Connector SDK.

This connector integrates JanusGraph graph database with Fivetran by extracting vertices, edges,
and their properties using the Gremlin Server API. It supports incremental sync with checkpointing
based on updated_at timestamps and provides schema discovery via JanusGraph management queries.

See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

# For reading configuration from a JSON file
import json

# For time-based operations and sleep during retries
import time

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# For handling Gremlin queries and responses
from gremlin_python.driver import client, serializer
from gremlin_python.driver.protocol import GremlinServerError

# Constants for pagination and checkpointing
__BATCH_SIZE = 1000
__MAX_RETRIES = 5
__RETRY_DELAY_SECONDS = 2

# Table names
__TABLE_VERTICES = "vertices"
__TABLE_EDGES = "edges"
__TABLE_VERTEX_PROPERTIES = "vertex_properties"
__TABLE_EDGE_PROPERTIES = "edge_properties"


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.

    This function is called at the start of the update method to ensure that the connector
    has all necessary configuration values for connecting to JanusGraph.

    Args:
        configuration: a dictionary that holds the configuration settings for the connector.

    Raises:
        ValueError: if any required configuration parameter is missing.
    """
    required_configs = ["gremlin_server_url", "traversal_source"]
    for key in required_configs:
        if key not in configuration:
            raise ValueError(f"Missing required configuration value: {key}")


def create_gremlin_client(configuration: dict):
    """
    Create and return a Gremlin client connection to JanusGraph.

    Args:
        configuration: a dictionary containing the Gremlin server connection details.

    Returns:
        A Gremlin client instance configured to connect to the specified server.
    """
    gremlin_server_url = configuration.get("gremlin_server_url")
    traversal_source = configuration.get("traversal_source", "g")

    # Create Gremlin client with JSON serialization
    gremlin_client = client.Client(
        gremlin_server_url,
        traversal_source,
        message_serializer=serializer.GraphSONSerializersV3d0(),
    )

    return gremlin_client


def execute_gremlin_query_with_retry(gremlin_client, query: str, bindings: dict = None):
    """
    Execute a Gremlin query with retry logic for transient failures.

    Args:
        gremlin_client: The Gremlin client instance.
        query: The Gremlin query string to execute.
        bindings: Optional dictionary of query parameter bindings.

    Returns:
        Query result set.

    Raises:
        RuntimeError: if the query fails after all retry attempts.
    """
    for attempt in range(__MAX_RETRIES):
        try:
            query_bindings = bindings if bindings is not None else {}
            result_set = gremlin_client.submit(query, query_bindings)
            return result_set.all().result()
        except (GremlinServerError, ConnectionError, TimeoutError) as e:
            if attempt == __MAX_RETRIES - 1:
                raise RuntimeError(
                    f"Gremlin query failed after {__MAX_RETRIES} attempts: {str(e)}"
                )

            sleep_time = min(60, __RETRY_DELAY_SECONDS**attempt)
            log.warning(
                f"Query failed, retry {attempt + 1}/{__MAX_RETRIES} after {sleep_time}s: {str(e)}"
            )
            time.sleep(sleep_time)


def get_vertex_labels(gremlin_client):
    """
    Retrieve all vertex labels from JanusGraph schema.

    Args:
        gremlin_client: The Gremlin client instance.

    Returns:
        List of vertex label names.
    """
    query = "mgmt = graph.openManagement(); mgmt.getVertexLabels().collect{it.name()}"
    try:
        result = execute_gremlin_query_with_retry(gremlin_client, query)
        return result if result else []
    except Exception as e:
        log.warning(f"Failed to retrieve vertex labels, using fallback: {str(e)}")
        # Fallback: get labels from actual vertices
        fallback_query = "g.V().label().dedup().toList()"
        result = execute_gremlin_query_with_retry(gremlin_client, fallback_query)
        return result if result else []


def get_edge_labels(gremlin_client):
    """
    Retrieve all edge labels from JanusGraph schema.

    Args:
        gremlin_client: The Gremlin client instance.

    Returns:
        List of edge label names.
    """
    query = (
        "mgmt = graph.openManagement(); mgmt.getRelationTypes(EdgeLabel.class).collect{it.name()}"
    )
    try:
        result = execute_gremlin_query_with_retry(gremlin_client, query)
        return result if result else []
    except Exception as e:
        log.warning(f"Failed to retrieve edge labels, using fallback: {str(e)}")
        # Fallback: get labels from actual edges
        fallback_query = "g.E().label().dedup().toList()"
        result = execute_gremlin_query_with_retry(gremlin_client, fallback_query)
        return result if result else []


def get_property_keys(gremlin_client):
    """
    Retrieve all property keys from JanusGraph schema.

    Args:
        gremlin_client: The Gremlin client instance.

    Returns:
        List of property key names.
    """
    query = "mgmt = graph.openManagement(); mgmt.getRelationTypes(PropertyKey.class).collect{it.name()}"
    try:
        result = execute_gremlin_query_with_retry(gremlin_client, query)
        return result if result else []
    except Exception as e:
        log.warning(f"Failed to retrieve property keys: {str(e)}")
        return []


def sync_vertices(gremlin_client, state: dict, has_updated_at: bool):
    """
    Sync vertices from JanusGraph with incremental support.

    This function fetches vertices in batches using pagination (range() step) and supports
    incremental sync based on updated_at property if available.

    Args:
        gremlin_client: The Gremlin client instance.
        state: The state dictionary containing last sync information.
        has_updated_at: Boolean indicating if vertices have updated_at property.

    Returns:
        The latest timestamp from synced vertices.
    """
    log.info("Starting vertices sync")

    last_updated_at = state.get("vertices_last_updated_at")
    latest_timestamp = last_updated_at
    offset = 0
    total_synced = 0

    while True:
        # Build query with optional incremental filter
        if has_updated_at and last_updated_at:
            query = f"""
            g.V().has('updated_at', gt('{last_updated_at}'))
             .order().by('updated_at')
             .range({offset}, {offset + __BATCH_SIZE})
             .project('id', 'label', 'properties')
             .by(id())
             .by(label())
             .by(valueMap(true))
            """
        else:
            query = f"""
            g.V().range({offset}, {offset + __BATCH_SIZE})
             .project('id', 'label', 'properties')
             .by(id())
             .by(label())
             .by(valueMap(true))
            """

        results = execute_gremlin_query_with_retry(gremlin_client, query)

        if not results:
            break

        for vertex in results:
            vertex_id = str(vertex.get("id"))
            vertex_label = vertex.get("label")
            properties = vertex.get("properties", {})

            # Flatten properties - handle both single values and lists
            flattened_props = {}
            for key, value in properties.items():
                # Ensure key is a string
                str_key = str(key)
                if isinstance(value, list):
                    # Take first value if list
                    flattened_props[str_key] = value[0] if value else None
                else:
                    flattened_props[str_key] = value

            vertex_record = {
                "vertex_id": vertex_id,
                "vertex_label": vertex_label,
                **flattened_props,
            }

            # The 'upsert' operation is used to insert or update data in the destination table.
            # The first argument is the name of the destination table.
            # The second argument is a dictionary containing the record to be upserted.
            op.upsert(table=__TABLE_VERTICES, data=vertex_record)

            # Sync vertex properties to separate table if they contain complex structures
            sync_vertex_properties(vertex_id, properties)

            # Track latest timestamp
            if has_updated_at and "updated_at" in flattened_props:
                updated_at = flattened_props["updated_at"]
                if latest_timestamp is None or updated_at > latest_timestamp:
                    latest_timestamp = updated_at

            total_synced += 1

        log.info(f"Synced {len(results)} vertices (total: {total_synced})")

        # Checkpoint after each batch
        state["vertices_last_updated_at"] = latest_timestamp
        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        op.checkpoint(state)

        # Check if we received a full batch
        if len(results) < __BATCH_SIZE:
            break

        offset += __BATCH_SIZE

    log.info(f"Completed vertices sync. Total synced: {total_synced}")
    return latest_timestamp


def sync_vertex_properties(vertex_id: str, properties: dict):
    """
    Sync vertex properties that have multiple values to separate table.

    Args:
        vertex_id: The vertex ID.
        properties: Dictionary of vertex properties.
    """
    for property_key, property_value in properties.items():
        if isinstance(property_value, list) and len(property_value) > 1:
            # Multi-valued property - create separate records
            for idx, value in enumerate(property_value):
                property_record = {
                    "vertex_id": vertex_id,
                    "property_key": property_key,
                    "property_value": str(value),
                    "property_index": idx,
                }

                # The 'upsert' operation is used to insert or update data in the destination table.
                # The first argument is the name of the destination table.
                # The second argument is a dictionary containing the record to be upserted.
                op.upsert(table=__TABLE_VERTEX_PROPERTIES, data=property_record)


def sync_edges(gremlin_client, state: dict, has_updated_at: bool):
    """
    Sync edges from JanusGraph with incremental support.

    This function fetches edges in batches using pagination (range() step) and supports
    incremental sync based on updated_at property if available.

    Args:
        gremlin_client: The Gremlin client instance.
        state: The state dictionary containing last sync information.
        has_updated_at: Boolean indicating if edges have updated_at property.

    Returns:
        The latest timestamp from synced edges.
    """
    log.info("Starting edges sync")

    last_updated_at = state.get("edges_last_updated_at")
    latest_timestamp = last_updated_at
    offset = 0
    total_synced = 0

    while True:
        # Build query with optional incremental filter
        if has_updated_at and last_updated_at:
            query = f"""
            g.E().has('updated_at', gt('{last_updated_at}'))
             .order().by('updated_at')
             .range({offset}, {offset + __BATCH_SIZE})
             .project('id', 'label', 'inV', 'outV', 'properties')
             .by(id())
             .by(label())
             .by(inV().id())
             .by(outV().id())
             .by(valueMap(true))
            """
        else:
            query = f"""
            g.E().range({offset}, {offset + __BATCH_SIZE})
             .project('id', 'label', 'inV', 'outV', 'properties')
             .by(id())
             .by(label())
             .by(inV().id())
             .by(outV().id())
             .by(valueMap(true))
            """

        results = execute_gremlin_query_with_retry(gremlin_client, query)

        if not results:
            break

        for edge in results:
            edge_id = str(edge.get("id"))
            edge_label = edge.get("label")
            in_vertex_id = str(edge.get("inV"))
            out_vertex_id = str(edge.get("outV"))
            properties = edge.get("properties", {})

            # Flatten properties
            flattened_props = {}
            for key, value in properties.items():
                # Ensure key is a string
                str_key = str(key)
                if isinstance(value, list):
                    flattened_props[str_key] = value[0] if value else None
                else:
                    flattened_props[str_key] = value

            edge_record = {
                "edge_id": edge_id,
                "edge_label": edge_label,
                "in_vertex_id": in_vertex_id,
                "out_vertex_id": out_vertex_id,
                **flattened_props,
            }

            # The 'upsert' operation is used to insert or update data in the destination table.
            # The first argument is the name of the destination table.
            # The second argument is a dictionary containing the record to be upserted.
            op.upsert(table=__TABLE_EDGES, data=edge_record)

            # Sync edge properties to separate table if they contain complex structures
            sync_edge_properties(edge_id, properties)

            # Track latest timestamp
            if has_updated_at and "updated_at" in flattened_props:
                updated_at = flattened_props["updated_at"]
                if latest_timestamp is None or updated_at > latest_timestamp:
                    latest_timestamp = updated_at

            total_synced += 1

        log.info(f"Synced {len(results)} edges (total: {total_synced})")

        # Checkpoint after each batch
        state["edges_last_updated_at"] = latest_timestamp
        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        op.checkpoint(state)

        # Check if we received a full batch
        if len(results) < __BATCH_SIZE:
            break

        offset += __BATCH_SIZE

    log.info(f"Completed edges sync. Total synced: {total_synced}")
    return latest_timestamp


def sync_edge_properties(edge_id: str, properties: dict):
    """
    Sync edge properties that have multiple values to separate table.

    Args:
        edge_id: The edge ID.
        properties: Dictionary of edge properties.
    """
    for property_key, property_value in properties.items():
        if isinstance(property_value, list) and len(property_value) > 1:
            # Multi-valued property - create separate records
            for idx, value in enumerate(property_value):
                property_record = {
                    "edge_id": edge_id,
                    "property_key": property_key,
                    "property_value": str(value),
                    "property_index": idx,
                }

                # The 'upsert' operation is used to insert or update data in the destination table.
                # The first argument is the name of the destination table.
                # The second argument is a dictionary containing the record to be upserted.
                op.upsert(table=__TABLE_EDGE_PROPERTIES, data=property_record)


def check_updated_at_property(gremlin_client):
    """
    Check if vertices and edges have updated_at property for incremental sync.

    Args:
        gremlin_client: The Gremlin client instance.

    Returns:
        Tuple of (vertices_have_updated_at, edges_have_updated_at).
    """
    # Check vertices
    vertex_query = "g.V().limit(1).has('updated_at').count()"
    try:
        vertex_result = execute_gremlin_query_with_retry(gremlin_client, vertex_query)
        vertices_have_updated_at = vertex_result[0] > 0 if vertex_result else False
    except Exception:
        vertices_have_updated_at = False

    # Check edges
    edge_query = "g.E().limit(1).has('updated_at').count()"
    try:
        edge_result = execute_gremlin_query_with_retry(gremlin_client, edge_query)
        edges_have_updated_at = edge_result[0] > 0 if edge_result else False
    except Exception:
        edges_have_updated_at = False

    return vertices_have_updated_at, edges_have_updated_at


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.

    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema

    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    return [
        {
            "table": __TABLE_VERTICES,
            "primary_key": ["vertex_id"],
            "columns": {"vertex_id": "STRING", "vertex_label": "STRING"},
        },
        {
            "table": __TABLE_EDGES,
            "primary_key": ["edge_id"],
            "columns": {
                "edge_id": "STRING",
                "edge_label": "STRING",
                "in_vertex_id": "STRING",
                "out_vertex_id": "STRING",
            },
        },
        {
            "table": __TABLE_VERTEX_PROPERTIES,
            "primary_key": ["vertex_id", "property_key", "property_index"],
            "columns": {
                "vertex_id": "STRING",
                "property_key": "STRING",
                "property_value": "STRING",
                "property_index": "INT",
            },
        },
        {
            "table": __TABLE_EDGE_PROPERTIES,
            "primary_key": ["edge_id", "property_key", "property_index"],
            "columns": {
                "edge_id": "STRING",
                "property_key": "STRING",
                "property_value": "STRING",
                "property_index": "INT",
            },
        },
    ]


def update(configuration: dict, state: dict):
    """
    Define the update function, which is a required function, and is called by Fivetran during each sync.

    See the technical reference documentation for more details on the update function
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update

    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
        state: a dictionary that holds the state of the connector.
    """
    log.warning("Example: GRAPH_DATABASE : JANUSGRAPH")

    # Validate the configuration to ensure it contains all required values
    validate_configuration(configuration=configuration)

    # Create Gremlin client
    gremlin_client = None

    try:
        gremlin_client = create_gremlin_client(configuration)

        log.info("Connected to JanusGraph Gremlin Server")

        # Check if data has updated_at property for incremental sync
        vertices_have_updated_at, edges_have_updated_at = check_updated_at_property(gremlin_client)

        if vertices_have_updated_at:
            log.info("Vertices have 'updated_at' property - incremental sync enabled")
        else:
            log.info("Vertices do not have 'updated_at' property - performing full sync")

        if edges_have_updated_at:
            log.info("Edges have 'updated_at' property - incremental sync enabled")
        else:
            log.info("Edges do not have 'updated_at' property - performing full sync")

        # Sync vertices
        vertices_timestamp = sync_vertices(gremlin_client, state, vertices_have_updated_at)

        # Sync edges
        edges_timestamp = sync_edges(gremlin_client, state, edges_have_updated_at)

        # Update final state
        if vertices_timestamp:
            state["vertices_last_updated_at"] = vertices_timestamp
        if edges_timestamp:
            state["edges_last_updated_at"] = edges_timestamp

        # Final checkpoint
        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        op.checkpoint(state)

        log.info("Sync completed successfully")

    except Exception as e:
        log.severe(f"Failed to sync JanusGraph data: {str(e)}")
        raise RuntimeError(f"Failed to sync data: {str(e)}")

    finally:
        # Clean up client connection
        if gremlin_client:
            try:
                gremlin_client.close()
            except Exception:
                pass


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents
    with open("configuration.json", "r") as f:
        configuration = json.load(f)

    # Test the connector locally
    connector.debug(configuration=configuration)
