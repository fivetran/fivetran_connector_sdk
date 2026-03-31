# Rillet Connector Example

## Connector overview
This connector fetches core accounting data from the Rillet API and syncs it into Fivetran destination tables.
It supports incremental updates using cursor-based pagination and checkpointing.  
Supported resources: accounts, subsidiaries, products, customers, contracts, invoices, invoice payments, credit memos, vendors, vendor credits, bills, charges, reimbursements, journal entries, bank accounts, bank transactions, tax rates, and fields.

## Requirements
- Python 3.10+ (3.12 supported)
- Preinstalled dependencies (provided by Fivetran runtime): `fivetran_connector_sdk`, `requests`

## Getting started
1. Create an API key in the Rillet dashboard under Organization Settings > API Access.
2. Copy `connectors/rillet/configuration.json` and populate `api_key`.
3. Run:
   - `fivetran debug --connector connectors/rillet/connector.py`

## Features
- Cursor-based pagination (`pagination.next_cursor`) across Rillet endpoints.
- Incremental sync using `updated.gt` and `updated_at` timestamps.
- Retry logic with exponential backoff for transient errors and 429 rate-limited responses.
- Data integrity with `op.upsert` and checkpoint state after each batch.
- Separate destination tables for each resource.

## Data handling
- `schema()` defines the table names and primary keys.
- `update()` loops over configured collections and fetches data from Rillet endpoints.
- Each record is upserted using `op.upsert`.
- State keys such as `accounts_cursor`, `accounts_last_updated_at` store progress.

## Error handling
- `validate_configuration()` checks that `api_key` is present and non-empty.
- Requests use retry/backoff for 429/5xx and fail fast on 4xx.
- `RuntimeError` wraps unexpected execution problems and stops sync for Fivetran.

## Tables created
- account (primary key: id)
- subsidiary (primary key: id)
- product (primary key: id)
- customer (primary key: id)
- contract (primary key: id)
- invoice (primary key: id)
- invoice_payment (primary key: id)
- credit_memo (primary key: id)
- vendor (primary key: id)
- vendor_credit (primary key: id)
- bill (primary key: id)
- charge (primary key: id)
- reimbursement (primary key: id)
- journal_entry (primary key: id)
- bank_account (primary key: id)
- bank_transaction (primary key: id)
- tax_rate (primary key: id)
- field (primary key: id)

## Additional considerations
- `base_url` defaults to `https://api.rillet.com` if omitted.
- Set `api_version` in config for API stability (defaults to 3).
- Webhook docs at https://docs.api.rillet.com/docs/webhooks (not used in connector pull sync).
- For very large datasets, you may further shard by organization subset query parameters.
