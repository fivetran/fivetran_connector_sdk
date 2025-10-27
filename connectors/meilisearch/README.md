# MeiliSearch Connector Example

## Connector overview
This connector demonstrates how to fetch index metadata and document data from [MeiliSearch](https://www.meilisearch.com/) and upsert it into your destination using the Fivetran Connector SDK. MeiliSearch is a fast, typo-tolerant search engine API commonly used for powering search functionality in e-commerce platforms, SaaS applications, and content management systems. The connector synchronizes all indexes and their documents from your MeiliSearch instance, enabling analytics on search data, product catalogs, user reviews, and search query performance.

## Requirements
- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features
- Synchronizes all indexes and their metadata from MeiliSearch API
- Fetches all documents from each index with automatic flattening of nested structures
- Supports both cloud-hosted and self-hosted MeiliSearch instances
- Implements offset-based pagination for efficient data retrieval
- Comprehensive error handling with exponential backoff retry logic for API requests
- Periodic checkpointing to ensure reliable sync resumption

## Configuration file
The connector requires the following configuration parameters:

```json
{
  "api_url": "<YOUR_MEILISEARCH_API_URL>",
  "api_key": "<YOUR_MEILISEARCH_API_KEY>"
}
```

### Configuration parameters

- `api_url` (required): The base URL of your MeiliSearch instance (e.g., `https://ms-xxx.meilisearch.io` for MeiliSearch Cloud or `http://localhost:7700` for self-hosted)
- `api_key` (required): Your MeiliSearch API key for authentication (master key or search API key with read permissions)

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file
The connector uses the `requests` library for HTTP communication, which is pre-installed in the Fivetran environment.

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication
The connector uses bearer token authentication with MeiliSearch API keys. To obtain your API key:

1. For MeiliSearch Cloud, log in to your MeiliSearch Cloud account and navigate to the API Keys section.
2. For self-hosted MeiliSearch, access your instance configuration where the master key is defined.
3. Copy the API key (master key or a search API key with appropriate permissions).
4. Add the API key to your `configuration.json` file as the `api_key` parameter.

The API key is included in the `Authorization` header as a bearer token for all API requests (refer to the `build_request_headers` function).

## Pagination
The connector implements MeiliSearch's offset-based pagination system for both indexes and documents. It processes data in configurable batches of 100 records per request (controlled by the `__PAGINATION_LIMIT` constant). The pagination logic calculates when all records have been fetched by comparing the current offset plus fetched count against the total available records. Refer to the `sync_indexes` function and `sync_documents_for_index` function for implementation details.

## Data handling
The connector processes data from two main MeiliSearch API endpoints. The `/indexes` endpoint provides index metadata including UID, creation timestamp, update timestamp, and primary key configuration. The `/indexes/{index_uid}/documents/fetch` endpoint retrieves documents from each index using POST requests. All nested JSON structures within documents are automatically flattened to create optimal table schemas for your destination (refer to the `flatten_document` function). Documents are enriched with `_index_uid` and `_document_id` fields to maintain relationships and ensure proper primary key handling.

## Error handling
The connector implements comprehensive error handling strategies with exponential backoff retry logic for transient failures. The `make_api_request` function handles HTTP timeouts with a 30-second timeout threshold, rate limiting with detection and automatic retry, and server errors with configurable retry attempts. Retries use exponential backoff starting at 1 second and doubling with each attempt up to a maximum of 5 retries. All errors are logged with appropriate severity levels without exposing sensitive information.

## Tables created
The connector creates the following tables in your destination:

### index table

| Column | Data Type | Description |
|--------|-----------|-------------|
| uid | STRING | Unique identifier for the index (primary key) |
| created_at | STRING | Timestamp when the index was created |
| updated_at | STRING | Timestamp when the index was last updated |
| primary_key | STRING | Configured primary key field name for the index |

The `index` table contains metadata about each MeiliSearch index including its unique identifier, creation timestamp, last update timestamp, and configured primary key field name.

### document table

| Column | Data Type | Description |
|--------|-----------|-------------|
| _index_uid | STRING | Unique identifier of the index (part of composite primary key) |
| _document_id | STRING | Document identifier (part of composite primary key) |

The `document` table contains all documents from all indexes with a composite primary key of index UID and document ID. Additional columns are dynamically created based on the document structure, with nested objects flattened using underscore separators and arrays converted to JSON strings. For example, a product document might contain columns like `name`, `description`, `price`, `category`, `brand`, `rating`, and `tags`.

## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
