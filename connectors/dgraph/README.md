# Dgraph E-Commerce Connector Example

## Connector overview

This connector syncs e-commerce product catalog data from Dgraph graph databases into data warehouses via the GraphQL API. It demonstrates a production-ready implementation that preserves graph relationships in relational format, enabling advanced analytics on product catalogs, recommendations, and customer reviews.

The connector showcases best practices for syncing graph data:
- True incremental sync using DateTime filters (createdAt/updatedAt fields)
- Graph structure preservation via a separate relationships/edges table
- Per-table state checkpointing for efficient resumption
- E-commerce use case: products, categories, attributes, reviews, and their relationships

## Requirements

- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

- True incremental sync using timestamp-based filtering on updatedAt/createdAt fields
- Graph relationship preservation through dedicated edges/relationships table
- Per-table state tracking with independent last_updated timestamps
- Pagination with configurable batch size (default: 100 records per page)
- Checkpointing every 500 records for reliable sync resumption
- GraphQL schema metadata extraction and tracking
- Automatic retry logic with exponential backoff (up to 5 retries)
- Authentication via X-Auth-Token header
- Support for both Dgraph GraphQL endpoint and admin API

## Configuration file

The connector requires the following configuration parameters:

```json
{
  "dgraph_url": "<YOUR_DGRAPH_GRAPHQL_ENDPOINT_URL>",
  "api_key": "<YOUR_DGRAPH_API_KEY_OR_AUTH_TOKEN>"
}
```

Configuration parameters:
- `dgraph_url` (required) - The base URL of your Dgraph instance (e.g., `http://localhost:8080` or `https://your-instance.dgraph.io`). Must start with `http://` or `https://`
- `api_key` (required) - Authentication token for accessing the Dgraph API

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file

This connector uses only the Python standard library and SDK-provided packages. No additional dependencies are required beyond what is pre-installed in the Fivetran environment.

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication

This connector uses API key authentication via the `X-Auth-Token` HTTP header. The authentication token is passed with every GraphQL request to both the `/graphql` and `/admin` endpoints.

Configuration:
1. Obtain an API key or auth token from your Dgraph instance
2. Add the token to the `api_key` field in your `configuration.json` file
3. The connector automatically handles authentication in all GraphQL requests

For local testing with Docker, authentication may be disabled by default in your Dgraph instance.

## Pagination

The connector implements offset-based pagination using Dgraph's GraphQL `first` and `offset` parameters. Refer to the sync functions (lines 239-355 for categories, 357-451 for attributes, 454-591 for products, 594-711 for reviews).

Pagination logic:
- Each query fetches up to 100 records per page (configurable via `__PAGE_SIZE` constant on line 43)
- The `offset` parameter advances by the number of records returned in each batch
- Pagination continues until fewer records than the page size are retrieved
- State is checkpointed after every 500 records (configurable via `__CHECKPOINT_INTERVAL` on line 44)
- Per-table timestamp tracking enables true incremental sync

This approach prevents memory overflow when syncing large datasets and ensures reliable recovery from interruptions.

## Data handling

The connector implements true incremental sync with per-table state management. Refer to the `update` function (lines 118-182) and individual sync functions:

Sync operations:
1. **Schema Metadata Sync** (`sync_schema_metadata`, lines 185-236) - Extracts GraphQL type definitions from admin API, performs full sync each time (small dataset)
2. **Categories Sync** (`sync_categories`, lines 239-355) - Syncs product categories with incremental filter on updatedAt field
3. **Attributes Sync** (`sync_attributes`, lines 357-451) - Syncs product attributes with incremental filter on updatedAt field
4. **Products Sync** (`sync_products`, lines 454-591) - Syncs products with relationships, using incremental filter on updatedAt field
5. **Reviews Sync** (`sync_reviews`, lines 594-711) - Syncs customer reviews with incremental filter on createdAt field

Graph structure preservation:
- All relationships are captured in a separate `relationships` table via the `create_edge_record` function (lines 714-736)
- Edge types include: BELONGS_TO_CATEGORY, RELATED_TO, HAS_ATTRIBUTE, REVIEWS_PRODUCT, WRITTEN_BY_USER, HAS_PARENT
- Each edge has a composite ID: `{source_id}_{relationship_type}_{target_id}`
- This enables graph reconstruction and relationship analysis in SQL

Data transformation:
- DateTime values are preserved in ISO 8601 format with UTC timezone
- All records include `synced_at` timestamps for audit tracking
- Related entities are tracked via count fields (e.g., `attributes_count`, `related_products_count`)
- Incremental sync uses `ge` (greater than or equal) filter on timestamp fields

## Error handling

The connector implements comprehensive error handling with retry logic. Refer to the `execute_graphql_query` function (lines 739-816).

Error handling strategies:
- **Authentication errors (401, 403)**: Fail immediately without retry, log as severe error
- **Bad requests (400)**: Fail immediately with detailed error message, no retry
- **Server errors (5xx)**: Retry up to 5 times with exponential backoff (base 2 seconds, max 60 seconds)
- **Network errors (timeout, connection)**: Retry with exponential backoff, configurable timeout (30 seconds default)
- **GraphQL errors**: Log warnings but continue processing if data is present
- **Unexpected exceptions**: Log severe error and re-raise with context

Configuration validation (`validate_configuration`, lines 62-79):
- Ensures required fields (dgraph_url, api_key) are present
- Validates URL format (must start with http:// or https://)
- Raises ValueError with descriptive message for invalid configuration

All errors are logged using the SDK logging framework with appropriate severity levels (info, warning, severe).

## Tables created

The connector creates the following tables in the destination warehouse:

| Table | Primary Key | Description |
|-------|-------------|-------------|
| `products` | `product_id` | Product catalog with SKU, name, price, and inventory status |
| `categories` | `category_id` | Product categories with hierarchical parent relationships |
| `attributes` | `attribute_id` | Product attributes for faceted search (brand, size, color, specs) |
| `reviews` | `review_id` | Customer reviews with ratings and comments |
| `relationships` | `edge_id` | Graph edges preserving all product relationships and recommendations |
| `schema_metadata` | `type_name` | GraphQL schema type definitions with field counts |

### Detailed column schemas

#### products

| Column | Type | Description |
|--------|------|-------------|
| `product_id` | STRING | Unique product identifier (primary key) |
| `sku` | STRING | Stock keeping unit |
| `name` | STRING | Product name |
| `description` | STRING | Product description |
| `price` | FLOAT | Product price |
| `in_stock` | BOOLEAN | Inventory availability |
| `category_id` | STRING | Foreign key to categories table |
| `attributes_count` | INT | Number of associated attributes |
| `related_products_count` | INT | Number of related products (recommendations) |
| `created_at` | UTC_DATETIME | Product creation timestamp |
| `updated_at` | UTC_DATETIME | Last update timestamp (used for incremental sync) |
| `synced_at` | UTC_DATETIME | Sync timestamp from connector |

#### categories

| Column | Type | Description |
|--------|------|-------------|
| `category_id` | STRING | Unique category identifier (primary key) |
| `name` | STRING | Category name |
| `description` | STRING | Category description |
| `parent_category_id` | STRING | Parent category for hierarchical taxonomy |
| `created_at` | UTC_DATETIME | Category creation timestamp |
| `updated_at` | UTC_DATETIME | Last update timestamp (used for incremental sync) |
| `synced_at` | UTC_DATETIME | Sync timestamp from connector |

#### attributes

| Column | Type | Description |
|--------|------|-------------|
| `attribute_id` | STRING | Unique attribute identifier (primary key) |
| `name` | STRING | Attribute name (e.g., Brand, Size, Color) |
| `value` | STRING | Attribute value (e.g., Apple, Large, Red) |
| `unit` | STRING | Unit of measurement (e.g., GB, inches, oz) |
| `created_at` | UTC_DATETIME | Attribute creation timestamp |
| `updated_at` | UTC_DATETIME | Last update timestamp (used for incremental sync) |
| `synced_at` | UTC_DATETIME | Sync timestamp from connector |

#### reviews

| Column | Type | Description |
|--------|------|-------------|
| `review_id` | STRING | Unique review identifier (primary key) |
| `rating` | INT | Star rating (1-5) |
| `comment` | STRING | Review text/comment |
| `product_id` | STRING | Foreign key to products table |
| `author_id` | STRING | Reviewer user ID |
| `author_username` | STRING | Reviewer username |
| `created_at` | UTC_DATETIME | Review creation timestamp (used for incremental sync) |
| `synced_at` | UTC_DATETIME | Sync timestamp from connector |

#### relationships

This table preserves the graph structure by storing all edges/relationships between entities.

| Column | Type | Description |
|--------|------|-------------|
| `edge_id` | STRING | Unique edge identifier: `{source_id}_{relationship_type}_{target_id}` (primary key) |
| `source_id` | STRING | Source node ID |
| `source_type` | STRING | Source node type (Product, Category, Review, etc.) |
| `target_id` | STRING | Target node ID |
| `target_type` | STRING | Target node type (Product, Category, Attribute, User, etc.) |
| `relationship_type` | STRING | Relationship type (BELONGS_TO_CATEGORY, RELATED_TO, HAS_ATTRIBUTE, REVIEWS_PRODUCT, WRITTEN_BY_USER, HAS_PARENT) |
| `created_at` | UTC_DATETIME | Edge creation timestamp |

Example relationship types:
- `BELONGS_TO_CATEGORY`: Product → Category
- `RELATED_TO`: Product → Product (recommendations)
- `HAS_ATTRIBUTE`: Product → Attribute
- `REVIEWS_PRODUCT`: Review → Product
- `WRITTEN_BY_USER`: Review → User
- `HAS_PARENT`: Category → Category (hierarchy)

#### schema_metadata

| Column | Type | Description |
|--------|------|-------------|
| `type_name` | STRING | GraphQL type name (primary key) |
| `field_count` | INT | Number of fields defined in the type |
| `extracted_at` | UTC_DATETIME | Schema extraction timestamp |

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
