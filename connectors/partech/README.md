# Partech (Punchh) POS API Connector Example

## Connector overview

This connector demonstrates how to sync location configuration and program metadata from the Partech (Punchh) Point of Sale (POS) API. Partech provides loyalty and engagement solutions for restaurants and retail businesses. The connector fetches location settings, program rules, redeemables (rewards), and redemption priority configurations, making this data available in your destination for analytics and reporting purposes.

## Requirements

- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

- Syncs location configuration data including business details, reward modes, and display settings
- Retrieves program metadata including loyalty program rules and point conversion settings
- Extracts redeemable rewards with point requirements
- Captures processing priority rules for multiple redemption types
- Implements exponential backoff retry logic for transient API errors
- Flattens nested JSON structures into relational tables
- Checkpoints after each endpoint sync for fault tolerance

## Configuration file

The connector requires the following configuration parameters:

```json
{
  "base_url": "<YOUR_PUNCHH_SERVER_URL>",
  "location_key": "<YOUR_LOCATION_API_KEY>",
  "business_key": "<YOUR_BUSINESS_KEY>"
}
```

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

| Parameter | Description | Required |
|-----------|-------------|----------|
| `base_url` | The base URL of your Partech (Punchh) server. This can be your production server (e.g., `https://yourserver.punchh.com`) or the mock server for testing (`https://stoplight.io/mocks/punchh-api/dev-portal-pos/50283893`) | Yes |
| `location_key` | Your location-specific API key obtained from Partech | Yes |
| `business_key` | Your business-wide API key obtained from Partech | Yes |

## Authentication

The connector uses custom token-based authentication. To obtain your credentials:

1. Log in to your Partech (Punchh) account.

2. Navigate to the API settings section for your business.

3. Retrieve your location key and business key.

4. Add both keys to the `configuration.json` file.

The authentication header format used by the connector is:

```
Authorization: Token token=<location_key>, btoken=<business_key>
```

## Data handling

The connector processes data using the following approach. Refer to the `sync_location_configuration`, `sync_program_meta`, `flatten_multiple_redemptions`, `upsert_program_meta`, `upsert_redeemables`, and `upsert_processing_priority` functions in `connector.py`:

- Location configuration data is synced as-is since it contains flat key-value pairs
- Single nested objects like `multiple_redemptions` are flattened into the main program_meta table with prefixed column names
- Arrays (such as `auto_redemption_discounts`) are converted to comma-separated strings and stored in the program_meta table; all array elements must be string-convertible (i.e., support conversion via `str()`)
- Arrays of objects like `redeemables` and `processing_priority_by_acquisition_type` are extracted into separate child tables with composite primary keys
- Child tables include the parent foreign key `program_type` as part of their composite primary key to maintain referential integrity

## Error handling

The connector implements robust error handling. Refer to the `make_api_request`, `handle_http_error`, and `handle_request_error` functions in `connector.py`:

- Client errors (4xx) fail immediately without retry as they indicate configuration or authentication issues
- Server errors (5xx) trigger up to 3 retry attempts with exponential backoff (1s, 2s, 4s delays)
- Network errors are retried with the same exponential backoff strategy
- All errors are logged with appropriate severity levels before being raised
- The connector checkpoints after each successful endpoint sync, allowing recovery from the last successful state

## Tables created

The connector creates the following tables in your destination:

### location_configuration

Contains location-specific settings and display configurations.

| Column | Primary Key |
|--------|-------------|
| `location_id` | Yes |
| `location_name` | No |
| `business_name` | No |
| `banked_rewards_mode` | No |
| `header` | No |
| `log_level` | No |
| `points_unlock_mode` | No |
| `print_barcodes` | No |
| `send_to_datasink` | No |
| `short_key` | No |
| `trailer_1` through `trailer_5` | No |
| `update_interval` | No |
| `visits_mode` | No |
| `multiple_redemption_on_location` | No |

### program_meta

Contains loyalty program rules and settings with flattened multiple redemption configurations.

| Column | Primary Key |
|--------|-------------|
| `program_type` | Yes |
| `minimum_payable_price` | No |
| `maximum_discountable_quantity` | No |
| `points_conversion_type` | No |
| `visits_per_card` | No |
| `card_redemption_value` | No |
| `minimum_visit_amount` | No |
| `minimum_visit_hours` | No |
| `minimum_age_to_signup` | No |
| `earning_unit` | No |
| `currency_earned` | No |
| `points_conversion_threshold` | No |
| `redemption_expiry_minutes` | No |
| `pending_points` | No |
| `pending_points_duration` | No |
| `configurable_default_time_eod` | No |
| `autocreate_user_phone` | No |
| `coupon_prefix` | No |
| `auto_redemption_discounts` | No |
| `processing_priority_by_discount_type` | No |
| `exclude_interoperability_strategy_between` | No |
| `multiple_redemptions_enabled` | No |

Note: Columns prefixed with `multiple_redemptions_` are flattened from the nested `multiple_redemptions` object.

### redeemable

Contains individual reward items that can be redeemed by customers.

| Column | Primary Key |
|--------|-------------|
| `program_type` | Yes (FK from program_meta) |
| `redeemable_id` | Yes |
| `name` | No |
| `description` | No |
| `redeemable_image_url` | No |
| `redeemable_properties` | No |
| `meta_data` | No |
| `points_required_to_redeem` | No |

### processing_priority_by_acquisition_type

Contains priority rules for different discount acquisition types during redemption.

| Column | Primary Key |
|--------|-------------|
| `program_type` | Yes (FK from program_meta) |
| `code` | Yes |
| `priority` | No |
| `multiplication_factor` | No |

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
