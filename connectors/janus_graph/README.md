# JanusGraph Connector Example

## Connector overview

This connector integrates JanusGraph graph database with Fivetran's data pipeline using the Gremlin Server API. JanusGraph is a scalable graph database optimized for storing and querying large graphs with billions of vertices and edges distributed across a multi-machine cluster.

The connector extracts vertices, edges, and their properties from JanusGraph and delivers them to your data warehouse in a flattened, analytics-ready format. It supports both full and incremental synchronization based on `updated_at` timestamps when available. This enables graph relationship analytics, operational observability, and compliance tracking for enterprises leveraging JanusGraph for knowledge graphs, recommendation engines, fraud detection, and network analysis.

Key capabilities include:
- Schema discovery via JanusGraph management API queries
- Incremental sync with automatic checkpoint management
- Pagination support for large graph datasets using Gremlin `range()` step
- Flattened property tables for multi-valued properties
- Relationship tracking between vertices via edges with source and target IDs

## Requirements

- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
    - Windows: 10 or later (64-bit only)
    - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
    - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

- Incremental synchronization - Automatically detects and uses `updated_at` property on vertices and edges for efficient incremental syncs. Falls back to full sync if the property is not present.
- Pagination support - Handles large graph datasets efficiently using Gremlin `range()` step with configurable batch size (default 1000 records per batch).
- Schema discovery - Automatically discovers vertex labels, edge labels, and property keys from JanusGraph management API.
- Multi-valued property handling - Creates separate property tables for vertices and edges with multi-valued properties, maintaining property order with indexes.
- Relationship preservation - Captures graph structure by storing `in_vertex_id` and `out_vertex_id` in the edges table for relationship analytics.
- Retry logic - Implements exponential backoff retry mechanism for transient Gremlin server failures.
- Checkpoint management - Automatically checkpoints state after each batch to enable resume capability on interruptions.

## Configuration file

The connector requires the following configuration parameters in the `configuration.json` file:

```json
{
  "gremlin_server_url": "<YOUR_GREMLIN_SERVER_URL>",
  "traversal_source": "<YOUR_TRAVERSAL_SOURCE_NAME>"
}
```

Configuration parameters:
- `gremlin_server_url` - The WebSocket URL of your Gremlin Server (e.g., `ws://localhost:8182/gremlin`)
- `traversal_source` - The graph traversal source name, typically `g` (default traversal source in JanusGraph)

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file

The connector requires the `gremlinpython` package to communicate with the Gremlin Server API:

```
gremlinpython
```

The `gremlinpython` library provides:
- WebSocket-based client for Gremlin Server connections
- GraphSON serialization for query requests and responses
- Support for graph traversal queries and management operations

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication

This connector uses WebSocket connections to the Gremlin Server API. By default, JanusGraph's Gremlin Server runs without authentication in development mode.

For production deployments:

1. Configure authentication on your JanusGraph Gremlin Server by editing `gremlin-server.yaml`:
    - Add authentication handler (e.g., Simple Authentication)
    - Configure username and password credentials

2. Extend the `configuration.json` to include authentication credentials:
   ```json
   {
     "gremlin_server_url": "wss://your-server:8182/gremlin",
     "traversal_source": "g",
     "username": "<YOUR_USERNAME>",
     "password": "<YOUR_PASSWORD>"
   }
   ```

3. Modify the `create_gremlin_client()` function in [connector.py:61-73](connector.py#L61-L73) to pass authentication credentials when creating the client connection.

## Pagination

The connector implements pagination using Gremlin's `range()` step to handle large graph datasets without loading all data into memory at once (refer to `sync_vertices()` function at [connector.py:180-280](connector.py#L180-L280) and `sync_edges()` function at [connector.py:318-418](connector.py#L318-L418)).

Pagination implementation:
- Batch size is controlled by the `__BATCH_SIZE` constant (default: 1000 records)
- Uses `range(offset, offset + batch_size)` in Gremlin queries
- Automatically detects end of data when fewer records than batch size are returned
- Checkpoints state after each batch for resume capability
- Offset increments by batch size for next iteration

For incremental syncs, pagination combines with timestamp filtering:
```gremlin
g.V().has('updated_at', gt('2024-01-15T10:00:00Z'))
  .order().by('updated_at')
  .range(0, 1000)
```

For full syncs without `updated_at` property:
```gremlin
g.V().range(0, 1000)
```

## Data handling

The connector transforms JanusGraph graph data into four relational tables optimized for warehouse analytics:

**Data transformation approach:**
- Flattens single-valued properties directly into vertex and edge tables as columns
- Creates separate property tables for multi-valued properties to avoid data loss
- Converts all vertex and edge IDs to strings for consistent identifier handling
- Preserves property ordering using `property_index` for list-type properties
- Automatically infers column data types from property values

**Incremental sync logic** (refer to `check_updated_at_property()` function at [connector.py:458-481](connector.py#L458-L481)):
- On first sync, checks if vertices/edges have `updated_at` property
- If present, uses timestamp-based filtering for subsequent syncs: `g.V().has('updated_at', gt(last_checkpoint))`
- State tracks separate timestamps: `vertices_last_updated_at` and `edges_last_updated_at`
- Falls back to full sync if `updated_at` property is not available

**Schema discovery** (refer to functions at [connector.py:115-178](connector.py#L115-L178)):
- Queries JanusGraph management API for vertex labels, edge labels, and property keys
- Uses fallback queries to discover labels from actual data if management API fails
- Minimal schema definition with primary keys and core columns
- Additional properties are auto-discovered by Fivetran from data

## Error handling

The connector implements comprehensive error handling with retry logic for transient failures:

**Retry mechanism** (refer to `execute_gremlin_query_with_retry()` function at [connector.py:76-112](connector.py#L76-L112)):
- Retries Gremlin queries up to 5 times (controlled by `__MAX_RETRIES` constant)
- Uses exponential backoff: sleep time = min(60, 2^attempt) seconds
- Catches specific exceptions: `GremlinServerError`, `ConnectionError`, `TimeoutError`
- Logs each retry attempt with warning level for monitoring
- Raises `RuntimeError` after exhausting all retry attempts

**Error scenarios handled:**
- Connection failures to Gremlin Server (network issues, server restarts)
- Query timeout errors for long-running graph traversals
- Server-side errors during query execution
- Schema discovery failures with automatic fallback to data-driven discovery

**Fail-fast behavior:**
- Configuration validation fails immediately on missing required parameters
- Final exception raised after all retries exhausted to alert Fivetran
- Client connection cleanup in `finally` block to prevent resource leaks

## Tables created

The connector creates four tables in your destination warehouse:

| Table | Primary Key | Description | Key Columns |
|-------|-------------|-------------|-------------|
| **vertices** | `vertex_id` | Stores all vertices (nodes) from the JanusGraph database with their properties. | `vertex_id` (STRING), `vertex_label` (STRING), plus dynamic property columns |
| **edges** | `edge_id` | Stores all edges (relationships) from the JanusGraph database with their properties. | `edge_id` (STRING), `edge_label` (STRING), `in_vertex_id` (STRING), `out_vertex_id` (STRING), plus dynamic property columns |
| **vertex_properties** | `[vertex_id, property_key, property_index]` | Stores multi-valued properties for vertices (only created when vertices have properties with multiple values). | `vertex_id` (STRING), `property_key` (STRING), `property_value` (STRING), `property_index` (INT) |
| **edge_properties** | `[edge_id, property_key, property_index]` | Stores multi-valued properties for edges (only created when edges have properties with multiple values). | `edge_id` (STRING), `property_key` (STRING), `property_value` (STRING), `property_index` (INT) |

### Detailed schema

#### vertices table

| Column | Data Type | Description |
|--------|-----------|-------------|
| `vertex_id` | STRING | Unique identifier for the vertex (Primary Key) |
| `vertex_label` | STRING | Label/type of the vertex (e.g., "account", "account_holder", "device") |
| Additional columns | Various | Dynamic columns for vertex properties (e.g., `name`, `email`, `balance`, `risk_score`, `updated_at`) |

#### edges table

| Column | Data Type | Description |
|--------|-----------|-------------|
| `edge_id` | STRING | Unique identifier for the edge (Primary Key) |
| `edge_label` | STRING | Label/type of the edge (e.g., "OWNS", "TRANSFERRED_TO", "ACCESSED_FROM") |
| `in_vertex_id` | STRING | ID of the destination/target vertex |
| `out_vertex_id` | STRING | ID of the source/origin vertex |
| Additional columns | Various | Dynamic columns for edge properties (e.g., `amount`, `since`, `updated_at`) |

#### vertex_properties table

| Column | Data Type | Description |
|--------|-----------|-------------|
| `vertex_id` | STRING | Reference to the parent vertex (Composite Primary Key) |
| `property_key` | STRING | Name of the property (Composite Primary Key) |
| `property_value` | STRING | Single value from the property list |
| `property_index` | INT | Position in the original property list, 0-based (Composite Primary Key) |

#### edge_properties table

| Column | Data Type | Description |
|--------|-----------|-------------|
| `edge_id` | STRING | Reference to the parent edge (Composite Primary Key) |
| `property_key` | STRING | Name of the property (Composite Primary Key) |
| `property_value` | STRING | Single value from the property list |
| `property_index` | INT | Position in the original property list, 0-based (Composite Primary Key) |

**Schema definition:** Refer to the `schema()` function at [connector.py:484-525](connector.py#L484-L525).

## Additional considerations

### Testing with Docker

To test this connector locally, spin up a JanusGraph instance using Docker:

```bash
docker run -d --name janusgraph -p 8182:8182 janusgraph/janusgraph:latest
```

The Gremlin Server will be available at `ws://localhost:8182/gremlin`.

### Adding sample data with timestamps

For testing incremental sync, add sample vertices and edges with `updated_at` properties:

```groovy
# Connect via Gremlin Console and run:
g.addV('person').property('name', 'Alice').property('updated_at', '2024-01-15T10:00:00Z')
g.addV('person').property('name', 'Bob').property('updated_at', '2024-01-15T11:00:00Z')
g.V().has('name', 'Alice').addE('knows').to(g.V().has('name', 'Bob')).property('updated_at', '2024-01-15T12:00:00Z')
```

### Testing the connector

Use Fivetran CLI commands to test the connector:

```bash
# Test sync
fivetran debug

# Reset state for fresh sync
fivetran reset
```

### Performance tuning

Adjust the `__BATCH_SIZE` constant in [connector.py:31](connector.py#L31) based on your graph size:
- Small graphs (< 100K vertices): 5000 records per batch
- Medium graphs (100K - 1M vertices): 1000 records per batch (default)
- Large graphs (> 1M vertices): 500 records per batch

### Graph relationship queries

To analyze relationships in your warehouse, join the `edges` table with `vertices`:

```sql
SELECT
  v1.vertex_label as source_type,
  e.edge_label as relationship,
  v2.vertex_label as target_type,
  COUNT(*) as relationship_count
FROM edges e
JOIN vertices v1 ON e.out_vertex_id = v1.vertex_id
JOIN vertices v2 ON e.in_vertex_id = v2.vertex_id
GROUP BY 1, 2, 3
```

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.