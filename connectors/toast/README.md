# Toast Connector Example

## Connector overview

This connector extracts and syncs data from the [Toast POS API](https://doc.toasttab.com/) into a destination warehouse. Toast is a restaurant management platform providing point-of-sale, labor, menu, and operational data.

## Requirements

- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

- Syncs data from multiple Toast endpoints, including orders, employees, shifts, menus, and more
- Automatically handles nested JSON structures and normalizes them into relational tables
- Includes incremental sync via time-based windowing and state checkpointing
- Graceful handling of rate limits, authentication, and API errors
- Supports voids, deletions, and nested child entities
- Uses Fernet encryption for token security in state

## Configuration file

```json
{
  "clientId": "<YOUR_CLIENT_ID>",
  "clientSecret": "<YOUR_CLIENT_SECRET>",
  "userAccessType": "TOAST_MACHINE_CLIENT",
  "domain": "<YOUR_DOMAIN>",
  "initialSyncStart": "<ISO_FORMAT_TIMESTAMP>",
  "key": "<BASE_64_ENCODED_FERNET_KEY>"
}
```

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Authentication

The connector generates and caches a Toast access token using the `clientId`, `clientSecret`, and `userAccessType` credentials. The token is encrypted using the provided Fernet `key` and stored in state for reuse across syncs. For more information on Fernet encryption, see the [Fernet documentation](https://cryptography.io/en/latest/fernet/). Refer to `make_headers(configuration, base_url, state, key)`.

## Pagination

The connector syncs data in 30-day time windows, iterating from `initialSyncStart` to the current sync time. State is checkpointed after each window to allow resumption in case of interruption. Refer to `def sync_items(base_url, headers, ts_from, ts_to, start_timestamp, state)` and `def set_timeranges(state, configuration, start_timestamp)`.

## Data handling

The connector flattens nested JSON objects and serializes list fields using `flatten_dict(parent_row, dict_field, prefix)`, `extract_fields(fields, row)`, and `stringify_lists(d)`. Records are written to the destination using `op.upsert()`, and deleted records are emitted using `op.delete()`. State is updated after each 30-day window to enable incremental sync. Refer to `def sync_items(base_url, headers, ts_from, ts_to, start_timestamp, state)`.


## Error handling

Refer to `get_api_response(endpoint_path, headers, **kwargs)`.

- 401 Unauthorized – Retries up to three times before logging a severe error and skipping the endpoint.
- 403 Forbidden – Skips the endpoint.
- 429 Too Many Requests – Backs off on rate limit response before retrying.
- 400 and 409 errors – Skips with logging.

## Tables created

The entity-relationship diagram (ERD) below shows how tables are linked in the Toast schema.

 <img src="https://raw.githubusercontent.com/fivetran/fivetran_connector_sdk/main/connectors/toast/Toast_ERD.png" alt="Fivetran Toast Connector ERD" width="100%">


| Category | Table | Primary Key |
|---|---|---|
| Core | `restaurant` | `id` |
| Labor | `job` | `id` |
| Labor | `shift` | `id` |
| Labor | `employee` | `id` |
| Labor | `employee_job_reference` | `id`, `employee_id` |
| Labor | `employee_wage_override` | `id`, `employee_id` |
| Labor | `time_entry` | `id` |
| Labor | `break` | `id` |
| Cash | `cash_deposit` | `id` |
| Cash | `cash_entry` | `id` |
| Config | `alternate_payment_types` | `id` |
| Config | `dining_option` | `id` |
| Config | `discounts` | `id` |
| Config | `menu` | `id` |
| Config | `menu_group` | `id` |
| Config | `menu_item` | `id` |
| Config | `restaurant_service` | `id` |
| Config | `revenue_center` | `id` |
| Config | `sale_category` | `id` |
| Config | `service_area` | `id` |
| Config | `tables` | `id` |
| Orders | `orders` | `id` |
| Orders | `orders_check` | `id` |
| Orders | `orders_check_applied_discount` | `id` |
| Orders | `orders_check_applied_discount_combo_item` | `id` |
| Orders | `orders_check_applied_discount_trigger` | `orders_check_applied_discount_id` |
| Orders | `orders_check_applied_service_charge` | `id`, `orders_check_id` |
| Orders | `orders_check_payment` | `orders_check_id`, `payment_id`, `orders_guid` |
| Orders | `orders_check_selection` | `id`, `orders_check_id` |
| Orders | `orders_check_selection_applied_discount` | `id` |
| Orders | `orders_check_selection_applied_discount_trigger` | `orders_check_selection_applied_discount_id` |
| Orders | `orders_check_selection_applied_tax` | `id`, `orders_check_selection_id` |
| Orders | `orders_check_selection_modifier` | `id`, `orders_check_selection_id` |
| Orders | `orders_pricing_feature` | `orders_id` |
| Orders | `payment` | `id` |

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
