# SAP Hybris Commerce Cloud Connector Example

## Connector overview
This connector extracts order data from SAP Hybris Commerce Cloud (formerly Hybris) using OAuth2 client credentials authentication. It provides comprehensive order data synchronization including main order details, payment transactions, line item entries, product bundles, and promotional results.

The connector implements incremental sync using cursor-based state management, processing orders in paginated batches and flattening complex nested JSON structures into multiple relational tables suitable for data warehousing and analytics.


## Requirements
- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)   
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)
- SAP Hybris Commerce Cloud API access with OAuth2 client credentials
- API permissions to read order data

## Getting started
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features
- OAuth2 client credentials authentication for secure API access
- Incremental sync using cursor-based state management (timestamp)
- Paginated data retrieval to handle large order volumes
- Comprehensive order data extraction including:
  - Main order information (order number, dates, totals, customer details)
  - Payment transactions (transaction IDs, amounts, payment methods)
  - Order line items/entries (products, quantities, prices)
  - Bundle product entries (bundled items within line items)
  - Promotion and discount results (applied promotions, discount amounts)
- Automatic flattening of nested JSON structures into relational format
- Run history tracking (pagination metadata, sync timestamps)
- Robust error handling with logging at multiple severity levels
- Automatic retry logic with exponential backoff for all API calls
- Page-level checkpointing for fault tolerance and resumability
- Individual order error isolation (failed orders don't stop the sync)

## Configuration file
The following configuration keys must be defined in `configuration.json`:

```json
{
  "prod_client_id": "<YOUR_HYBRIS_PROD_CLIENT_ID>",
  "prod_client_secret": "<YOUR_HYBRIS_PROD_CLIENT_SECRET>",
  "prod_api_url": "<YOUR_HYBRIS_PROD_API_URL>",
  "prod_token_url": "<YOUR_HYBRIS_PROD_TOKEN_URL>",
  "orders_api_endpoint": "<YOUR_HYBRIS_API_ENDPOINT>"
}
```

Configuration parameters:
- `prod_client_id` (required) - OAuth2 client ID for production Hybris API authentication
- `prod_client_secret` (required) - OAuth2 client secret for authentication
- `prod_api_url` (required) - Base URL for your Hybris Commerce Cloud API (e.g., `https://api.hybris.example.com`)
- `prod_token_url` (required) - OAuth2 token endpoint URL (e.g., `https://api.hybris.example.com/authorizationserver/oauth/token`)
- `orders_api_endpoint` (required) - Orders API endpoint path with query parameters (e.g., `/occ/v2/electronics/orders?fields=FULL`)

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file

This connector does not require any external Python dependencies beyond the pre-installed packages.

The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment and are sufficient for this connector's operation.

Note: To avoid dependency conflicts, do not declare `fivetran_connector_sdk` or `requests` in your `requirements.txt` file.


## Authentication

This connector uses OAuth2 client credentials grant flow for authentication:

1. Obtain OAuth2 credentials from your SAP Hybris Commerce Cloud administrator:
   - Client ID
   - Client secret
   - Token endpoint URL
2. Add your OAuth2 credentials to the `configuration.json`.
3. Authentication flow (handled automatically by the connector - see the `get_oauth_token()` function):
   - Connector sends POST request to token endpoint with client credentials
   - Hybris API responds with access token
   - Access token is used in Authorization header for all subsequent API requests
   - Tokens are refreshed on each sync run
4. Required API permissions:
   - Read access to the orders endpoint
   - Access to order fields including: `orders`, `entries`, `paymentTransactions`, `allPromotionResults`, and `bundleEntries`


## Pagination
The connector implements offset-based pagination to handle large order datasets (see the `update()` function, and `fetch_orders_page()` function):

- Pagination method: Page number-based (offset pagination)
- Page processing: Each page is fetched, processed, and checkpointed before moving to the next
- Pagination metadata: Extracted from API response (`paginationData` object)
  - `currentPage` - Current page number (0-indexed)
  - `pageSize` - Number of orders per page
  - `totalNumberOfResults` - Total orders matching the query
  - `numberOfPages` - Total pages available
- Loop control: Continues until all pages are processed (`while current_page < num_pages`)
- Request construction: Page parameter appended to URL (e.g., `&page=0`, `&page=1`)
- Memory efficiency: Processes one page at a time, preventing memory overflow with large datasets

## Data handling
The connector transforms nested Hybris API responses into a flattened relational structure suitable for data warehousing:

### Data transformation (see the `flatten_dict()` function)
- Nested dictionaries: Recursively flattened with underscore-separated keys
  - Example: `{"user": {"name": "John"}}` becomes `{"user_name": "John"}`
- Lists/arrays: JSON-serialized as strings for storage
- Null/empty values: Replaced with `'N/A'` placeholder for consistency
- Date filters: URL-encoded for API compatibility (see `build_date_filters()`)

### Data processing flow (see the `process_single_order()` and related functions)
1. Extract order number as primary key
2. Flatten main order object into `orders_raw` table
3. Process related entities in separate functions:
   - `process_payment_transactions()` - Payment transaction records
   - `process_order_entries()` - Line items and bundle entries
   - `process_promotion_results()` - Promotion/discount records
4. Generate composite keys for child records (e.g., `orderKey_transactionId`)
5. Upsert records to destination tables

### State management (see the `update()` function)
- Cursor format: Timestamp string `"YYYY-MM-DD HH:MM:SS"`
- Default lookback: 30 days from current date if no state exists
- State update: Updated to current sync time after each page
- Checkpoint frequency: After processing each page of orders

## Error handling
The connector implements multi-level error handling for robustness (see `update()` function):

### Authentication errors
- Catches OAuth2 token acquisition failures
- Logs at `log.severe()` level with error details
- Re-raises exception to stop sync (cannot proceed without authentication)

### API request errors (see the `fetch_orders_page()` and `_make_api_request_with_retry()`)
- Catches `requests.exceptions.RequestException` for network/HTTP errors
- Uses `response.raise_for_status()` to catch 4xx/5xx HTTP status codes
- Logs failed page numbers and error details
- Re-raises exception with context

### Retry logic with exponential backoff (see `_make_api_request_with_retry()`)
- Automatically retries failed API requests up to 3 times (configurable via `__MAX_RETRIES`)
- Implements exponential backoff: 1s, 2s, 4s delays (capped at 15s)
- Retries on transient errors:
  - Connection errors (network issues)
  - Timeout errors
  - HTTP 429 (rate limiting), 500, 502, 503, 504 (server errors)
- Immediately fails on non-retryable errors (4xx client errors except 429)
- Logs retry attempts with detailed error information
- Applies to both OAuth token acquisition and order data fetching

### Individual order errors (see the `update()` function)
- Try-catch block around `process_single_order()` calls
- Logs warning with order number and error details
- Continues processing remaining orders (isolated failure handling)
- Ensures one failed order doesn't stop the entire sync

### Configuration validation (see `validate_configuration()`)
- Validates presence of all required configuration keys before API calls
- Raises `ValueError` with clear message listing missing keys
- Prevents runtime errors from missing credentials

### Empty response handling
- Checks for `paginationData` in API response
- Logs warning and returns gracefully if no data available
- Handles empty order lists on individual pages

## Tables created

This connector creates six related tables optimized for order analytics:

### ORDERS_RUN_HISTORY
Tracks connector execution metadata for monitoring and troubleshooting.
- Primary key: `run_key` (sync timestamp)
- Columns: `page_size`, `totalNumberOfPages`, `totalNumberOfResults`

### ORDERS_RAW
Main orders table containing flattened order details.
- Primary key: `order_key` (order number)
- Contains: All order fields from API response, flattened with underscore notation
- Example columns: `order_num`, `code`, `created`, `totalPrice`, `user_name`, `user_uid`, `status`, etc.

### ORDERS_PAYMENT_TRANSACTIONS
Payment transaction details for each order.
- Primary key: `order_key` (composite: `orderNumber_transactionId`)
- Contains: Flattened payment transaction data prefixed with `paymentTransactions_`
- Relationship: Many-to-one with `orders_raw` via `order_num`

### ORDERS_ENTRIES
Individual line items (products/services) within each order.
- Primary key: `order_key` (composite: `orderNumber_orderLineNumber`)
- Contains: Flattened entry data prefixed with `entries_`
- Relationship: Many-to-one with `orders_raw` via `order_num`

### ORDERS_ENTRIES_BUNDLE_ENTRIES
Bundle product details within line items (products sold as bundles).
- Primary key: `order_key` (composite: `orderNumber_entryLine_bundleLineNumber`)
- Contains: Flattened bundle data prefixed with `bundleEntries_`
- Additional column: `entry_line` (parent line item reference)
- Relationship: Many-to-one with `orders_entries` via `order_num` and `entry_line`

### ORDERS_ALL_PROMOTION_RESULTS
Promotions and discounts applied to orders.
- Primary key: `order_key` (composite: `orderNumber_promotionName`)
- Contains: Flattened promotion data prefixed with `promotion_`
- Relationship: Many-to-one with `orders_raw` via `order_num`

Note: All child tables include an `order_num` column for joining back to the main `orders_raw` table.

## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
