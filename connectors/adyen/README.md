# Adyen Connector Example

## Connector overview

This connector example demonstrates how to integrate with Adyen's payment processing platform to sync payment data, modifications, and webhook events. The connector fetches transaction data, refunds, captures, cancellations, and webhook notifications from Adyen's APIs and syncs them to your data warehouse using the Fivetran Connector SDK.

The connector implements memory-efficient streaming patterns to handle large volumes of payment data while maintaining low cognitive complexity through modular helper functions. It supports incremental syncing based on creation timestamps and provides robust error handling for rate limiting and API failures.

Key features include support for multiple data types (payments, modifications, webhook events), configurable pagination, retry logic with exponential backoff, and comprehensive data validation. The connector follows Adyen's API authentication patterns and handles both test and live environments.

## Requirements

- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started

To set up and run this connector, follow the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide).

## Features

- Payment data sync: Fetches payment transactions with comprehensive details including amount, status, payment method, and risk information (refer to `get_payments_data` function)
- Modification tracking: Syncs payment modifications including captures, refunds, and cancellations with proper relationship mapping (refer to `get_modifications_data` function)
- Webhook events: Captures webhook notifications for real-time payment status updates (refer to `get_webhooks_data` function)
- Incremental sync: Supports time-based incremental syncing to fetch only new data since last sync
- Memory efficiency: Uses generator-based streaming to process large datasets without memory accumulation
- Error handling: Implements comprehensive retry logic with exponential backoff for rate limiting and network issues (refer to `execute_api_request` function)
- Configuration validation: Automatic configuration validation provided by the Fivetran SDK
- Data quality: Automatic field mapping and type conversion with JSON serialization for complex objects

## Configuration file

```json
{
  "api_key": "<YOUR_ADYEN_API_KEY>",
  "merchant_account": "<YOUR_MERCHANT_ACCOUNT>",
  "environment": "<YOUR_ENVIRONMENT>",
  "enable_payments": "<YOUR_ENABLE_PAYMENTS>",
  "enable_modifications": "<YOUR_ENABLE_MODIFICATIONS>",
  "enable_webhooks": "<YOUR_ENABLE_WEBHOOKS>",
  "sync_frequency_hours": "<YOUR_SYNC_FREQUENCY_HOURS>",
  "initial_sync_days": "<YOUR_INITIAL_SYNC_DAYS>",
  "max_records_per_page": "<YOUR_MAX_RECORDS_PER_PAGE>",
  "request_timeout_seconds": "<YOUR_REQUEST_TIMEOUT_SECONDS>",
  "retry_attempts": "<YOUR_RETRY_ATTEMPTS>",
  "enable_incremental_sync": "<YOUR_INCREMENTAL_SYNC>",
  "enable_debug_logging": "<YOUR_ENABLE_DEBUG_LOGGING>"
}
```

### Configuration parameters

- api_key (required): Your Adyen API key for authentication. Get this from your Adyen Customer Area under Developers > API credentials
- merchant_account (required): The merchant account identifier provided by Adyen
- environment: API environment to use - "test" for testing or "live" for production (default: "test")
- enable_payments: Enable syncing payment transaction data (default: "true")
- enable_modifications: Enable syncing payment modifications like refunds and captures (default: "true")
- enable_webhooks: Enable syncing webhook event notifications (default: "true")
- sync_frequency_hours: How often to run incremental syncs in hours (default: "4")
- initial_sync_days: Number of days of historical data to fetch on first sync (default: "90")
- max_records_per_page: Maximum records to fetch per API request (default: "100", range: 1-1000)
- request_timeout_seconds: HTTP request timeout in seconds (default: "30", range: 1-300)
- retry_attempts: Number of retry attempts for failed requests (default: "3", range: 1-10)
- enable_incremental_sync: Enable incremental syncing based on timestamps (default: "true")
- enable_debug_logging: Enable verbose debug logging for troubleshooting (default: "false")

## Requirements file

This connector uses the following dependencies that are automatically provided by the Fivetran environment:

- `requests`: For making HTTP requests to Adyen APIs
- `datetime`: For handling timestamps and date ranges
- `json`: For parsing JSON responses and serializing complex objects
- `typing`: For type hints and improved code readability

The Fivetran environment provides these dependencies automatically, so you don't need to install them separately when deploying the connector.

## Authentication

### Setting up Adyen API credentials

1. Access your Adyen Customer Area:
   - Log in to your Adyen Customer Area
   - Navigate to **Developers > API credentials**

2. Create or select API credentials:
   - Create new API credentials or select existing ones
   - Ensure the credentials have appropriate permissions for:
     - Merchant Reporting API (for payment data)
     - Management API (for webhook configurations)
     - Checkout API (for payment details)

3. Generate API key:
   - Generate an API key from your API credentials
   - Copy the API key - this will be your `api_key` configuration value

4. Find your merchant account:
   - Your merchant account identifier is shown in the Customer Area
   - This identifier is required for the `merchant_account` configuration

5. Environment setup:
   - Use "test" environment for development and testing
   - Use "live" environment for production data
   - Different environments require different API credentials

### Authentication method

The connector uses API key authentication via the `X-API-Key` header. All requests are authenticated using your Adyen API key, which provides access to your merchant account data based on the permissions configured in your Adyen Customer Area.

## Pagination

The connector implements cursor-based pagination to efficiently handle large datasets from Adyen's APIs. The pagination strategy varies by data type:

- Payment Data: Uses date-based filtering with configurable page sizes (refer to `get_payments_data` function)
- Modifications: Fetches modifications within date ranges with automatic pagination
- Webhooks: Processes webhook events in chronological order with size limits

The `max_records_per_page` configuration parameter controls the page size for all API requests. The connector automatically handles pagination by continuing to fetch data until all records within the specified date range are retrieved.

## Data handling

### Memory efficiency

The connector uses generator-based streaming patterns to prevent memory accumulation when processing large datasets (refer to data fetching functions). Each record is processed individually and immediately upserted to the destination, ensuring constant memory usage regardless of dataset size.

### Field mapping

- Payment Data: Maps Adyen payment objects to flat table structure with JSON serialization for complex fields like `paymentMethod` and `additionalData`
- Modification Data: Tracks relationship between modifications and original payments via `original_reference` field
- Webhook Events: Captures event metadata with proper typing for boolean fields like `success`

All monetary amounts are stored in minor units (cents) as returned by Adyen's APIs. Timestamps are normalized to UTC format for consistency across time zones.

### Incremental sync

The connector supports incremental syncing using creation timestamps. After each successful sync, the connector checkpoints the current timestamp and uses it as the starting point for the next sync. This ensures only new or updated data is fetched, improving performance and reducing API usage.

## Error handling

The connector implements comprehensive error handling patterns with separated concerns for maintainability:

- Rate limiting: Handles HTTP 429 responses with automatic retry using exponential backoff and jitter (refer to `__handle_rate_limit` function)
- Network errors: Implements retry logic for transient network issues with configurable attempts (refer to `__handle_request_error` function)
- Authentication errors: Provides clear error messages for invalid API credentials
- Data validation: Automatic configuration parameter validation provided by the Fivetran SDK
- API timeouts: Configurable request timeouts with automatic retry for timeout errors

The error handling logic is modularized into focused helper functions to maintain low cognitive complexity while providing robust error recovery capabilities.

## Tables created

The connector creates the following tables in your data warehouse. Column types are automatically inferred by Fivetran.

### payments

Primary key: `psp_reference`

Sample columns:
- `psp_reference`: Adyen's unique payment reference
- `merchant_account`: Merchant account identifier
- `payment_method`: JSON object containing payment method details
- `amount_value`: Payment amount in minor units (cents)
- `amount_currency`: Payment currency code
- `status`: Payment status (Authorised, Refused, etc.)
- `creation_date`: Payment creation timestamp
- `merchant_reference`: Merchant's reference for the payment
- `shopper_reference`: Reference for the shopper/customer
- `country_code`: Country code for the payment
- `shopper_ip`: IP address of the shopper
- `risk_score`: Adyen's risk assessment score
- `auth_code`: Authorization code from processor
- `additional_data`: JSON object with additional payment metadata

### modifications

Primary key: `psp_reference`

Sample columns:
- `psp_reference`: Unique reference for the modification
- `original_reference`: Reference to the original payment
- `merchant_account`: Merchant account identifier
- `modification_type`: Type of modification (Capture, Refund, Cancel)
- `status`: Modification status
- `amount_value`: Modification amount in minor units
- `amount_currency`: Currency of the modification
- `creation_date`: Modification creation timestamp
- `reason`: Reason for the modification (for refunds)
- `additional_data`: JSON object with additional modification metadata

### webhook_events

Primary key: `psp_reference`, `event_code`, `event_date`

Sample columns:
- `psp_reference`: Payment reference the event relates to
- `merchant_account`: Merchant account identifier
- `event_code`: Type of webhook event (AUTHORISATION, CAPTURE, etc.)
- `event_date`: When the event occurred
- `success`: Boolean indicating if the event was successful
- `payment_method`: Payment method used
- `amount_value`: Event amount in minor units
- `amount_currency`: Event currency
- `original_reference`: Reference to original payment (for modifications)
- `merchant_reference`: Merchant's reference
- `additional_data`: JSON object with additional event metadata

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.