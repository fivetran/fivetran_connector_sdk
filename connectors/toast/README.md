# Toast Fivetran Connector

This is a custom [Fivetran connector](https://fivetran.com/docs/connectors/connector-sdk) implementation to extract and sync data from the [Toast POS API](https://doc.toasttab.com/) into a destination warehouse. Toast is a restaurant management platform providing point-of-sale, labor, menu, and operational data.

For full implementation details, see the [Toast connector example code](https://github.com/fivetran/fivetran_connector_sdk/edit/main/examples/source_examples/toast/).

---

## Features

- Syncs data from multiple Toast endpoints, including orders, employees, shifts, menus, and more
- Automatically handles nested JSON structures and normalizes them into relational tables
- Includes incremental sync via time-based windowing and state checkpointing
- Graceful handling of rate limits, authentication, and API errors
- Supports voids, deletions, and nested child entities
- Uses Fernet encryption for token security in state

---

## Requirements

- Toast API credentials: `clientId`, `clientSecret`, `userAccessType`
- Domain to connect to (e.g., `api.toasttab.com`)
- A Fernet key (`key`) for encrypting access tokens
- `initialSyncStart` ISO timestamp to define the start of sync window

Example `configuration.json`:

```json
{
  "clientId": "your_client_id",
  "clientSecret": "your_client_secret",
  "userAccessType": "TOAST_MACHINE_CLIENT",
  "domain": "ws-api.toasttab.com",
  "initialSyncStart": "2023-01-01T00:00:00.000Z",
  "key": "your_base64_encoded_fernet_key"
}
```

---

## How it works


The connector performs the following actions for each key aspect:
- Authentication: Generates and caches a Toast token using the provided credentials
- Sync Loop: Runs in 30-day time chunks, paginating through endpoints
- Data Normalization: Flattens nested objects and lists using `flatten_dict`, `extract_fields`, and `stringify_lists`
- Upserts and Deletes: Emits operations using `op.upsert()` and `op.delete()`
- Checkpointing: Updates state after each window to resume seamlessly

---

## Data coverage

The entity-relationship diagram (ERD) below shows how tables are linked in the Toast schema.

 <img src="https://raw.githubusercontent.com/fivetran/fivetran_connector_sdk/main/connectors/toast/Toast_ERD.png" alt="Fivetran Toast Connector ERD" width="100%">

### Core tables
- `restaurant`
- `job`, `employee`, `shift`, `break`, `time_entry`
- `orders`, `orders_check`, `payment`

### Configuration
- `menu`, `menu_item`, `menu_group`, `discounts`, `tables`, etc.

### Nested children
- `orders_check_payment`, `orders_check_selection`, `orders_check_selection_modifier`, etc.

### Cash management
- `cash_entry`, `cash_deposit`

---


## Error handling

- Retries on 401 Unauthorized (max 3)
- Skips 403 Forbidden
- Backs off on 429 Too Many Requests
- Skips on 400 and 409 errors with logging

---

## Logging

Uses Fivetran SDK logging levels (`info`, `fine`, `warning`, `severe`) for detailed sync visibility.

---

## Resources

- [Fivetran Connector SDK Docs](https://fivetran.com/docs/connectors/connector-sdk)
- [Toast API Reference](https://doc.toasttab.com/)
- [Fernet Encryption](https://cryptography.io/en/latest/fernet/)

