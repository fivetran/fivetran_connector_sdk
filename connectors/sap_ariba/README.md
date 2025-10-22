# SAP Ariba Purchase Orders Connector (Custom Fivetran SDK)

## Connector overview

This connector retrieves **Purchase Order (PO)** and **Purchase Order Item** data from the **SAP Ariba API** and loads it into your Fivetran destination using the **Fivetran Connector SDK**.

It supports incremental syncing by maintaining timestamps in its internal state and emits structured, schema-defined records into two destination tables:

- `orders` — Contains high-level purchase order metadata.
- `items` — Contains line-item level data for each order.

The connector uses the **SAP Ariba Sandbox API** for demonstration purposes and can be configured for production endpoints.

---

## Requirements

* [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
* Operating System:
    * Windows 10 or later
    * macOS 13 (Ventura) or later
    * Linux: Ubuntu 20.04+, Debian 10+, or Amazon Linux 2+ (arm64 or x86_64)
* SAP Ariba API Key (or Sandbox access)
* Outbound internet access to `sandbox.api.sap.com` (or your production Ariba endpoint)

---

## Features

* Fetches **purchase orders** and **line items** from the Ariba API
* Performs **incremental syncs** using internal timestamp checkpoints
* Automatically handles API pagination via `$skip` and `$top` parameters
* Converts Ariba timestamps to ISO 8601 UTC format
* Retries automatically on network or rate limit errors
* Performs schema-aware upserts for reliable idempotent loads

---

## Configuration file

Example `configuration.json`:

```json
{
  "APIKey": "YOUR_API_KEY"
}
```

**Notes**
- The Ariba Sandbox API requires an `APIKey` header for authentication.
- You can replace the sandbox base URL (`https://sandbox.api.sap.com/ariba/api/purchase-orders/v1/sandbox/`) with your production endpoint.

---

## Requirements file

Minimal dependencies:

```text
fivetran-connector-sdk
requests
```

These are pre-installed in the Fivetran environment.

---

## Authentication

The connector authenticates to SAP Ariba via an API key passed in headers:

```
APIKey: <YOUR_API_KEY>
Accept: application/json
DataServiceVersion: 2.0
```

---

## Data handling and flow

1. Builds schema for two tables:
    - **orders** (top-level purchase order data)
    - **items** (line-item level detail)
2. Fetches data via paginated REST calls (`$top`, `$skip`)
3. Filters and normalizes response JSON into structured rows
4. Converts date strings (e.g., `"16 May 2019 1:41:26 AM"`) into ISO timestamps
5. Emits:
    - `op.Upsert(table, data)` for every record
    - `op.Checkpoint(state)` periodically for reliable recovery
6. Updates the `last_updated_at` field in connector state after each successful batch.

---

## Incremental sync logic

| Setting | Description |
|----------|-------------|
| `__LAST_UPDATED_AT` | Tracks last processed timestamp per table |
| `__CHECKPOINT_INTERVAL` | Checkpoints every 1,000 processed records |
| `__EPOCH_START_DATE` | Default starting point (`1970-01-01T00:00:00Z`) |

### Example Flow

1. Start from `state[table]["last_updated_at"]` or epoch start.
2. Fetch all new records using `$skip`/`$top` pagination.
3. For each record, assign:
    - `row_id` = running count
    - `last_updated_at` = sync start timestamp
4. Upsert into destination.
5. After every 1,000 rows → `op.checkpoint(state)`
6. Save `last_updated_at` after final page.

---

## Tables Created

### orders
**Primary key:** `payload_id`, `revision`, `row_id`

| Field | Type | Description |
|--------|------|-------------|
| `documentNumber` | STRING | Purchase order number |
| `orderDate` | UTC_DATETIME | Order creation date |
| `supplierName` | STRING | Supplier organization name |
| `supplierANID` | STRING | Supplier Ariba Network ID |
| `buyerANID` | STRING | Buyer Ariba Network ID |
| `customerName` | STRING | Customer name |
| `systemId` | STRING | Ariba system identifier |
| `payloadId` | STRING | Unique payload ID |
| `revision` | STRING | Revision identifier |
| `endpointId` | STRING | Endpoint identifier |
| `created` | UTC_DATETIME | Creation date |
| `status` | STRING | Order status |
| `documentStatus` | STRING | Document state (Confirmed/Unconfirmed) |
| `amount` | DOUBLE | Total purchase order value |
| `numberOfInvoices` | INT | Count of associated invoices |
| `invoiced_amount` | DOUBLE | Invoiced amount |
| `company_code` | STRING | Company identifier |
| `row_id` | INT | Sequential row counter |

---

### items
**Primary key:** `document_number`, `line_number`, `row_id`

| Field | Type | Description |
|--------|------|-------------|
| `documentNumber` | STRING | Parent PO number |
| `lineNumber` | INT | Line item number |
| `quantity` | DOUBLE | Ordered quantity |
| `unitOfMeasure` | STRING | Unit of measure |
| `supplierPart` | STRING | Supplier part number |
| `buyerPartId` | STRING | Buyer’s part number |
| `manufacturerPartId` | STRING | Manufacturer part ID |
| `description` | STRING | Item description |
| `itemShipToName` | STRING | Shipping contact name |
| `itemShipToStreet` | STRING | Street address |
| `itemShipToCity` | STRING | City |
| `itemShipToState` | STRING | State |
| `itemShipToPostalCode` | STRING | Postal code |
| `itemShipToCountry` | STRING | Country |
| `isoCountryCode` | STRING | ISO 2-letter country code |
| `itemShipToCode` | STRING | Ship-to code |
| `itemLocation` | STRING | Delivery location |
| `requestedDeliveryDate` | UTC_DATETIME | Requested delivery date |
| `requestedShipmentDate` | UTC_DATETIME | Requested ship date |
| `row_id` | INT | Sequential row counter |

---

## Error handling

| Error | Description | Resolution |
|--------|-------------|-------------|
| 400–499 | Client errors (bad API key, malformed request) | Check API key or request parameters |
| 429 | Rate limit reached | Retries with exponential backoff |
| 500–599 | Server errors | Retries automatically with delay |
| Network errors | Connection failures or timeouts | Retries up to 3 times with delay |
| Data parsing | Invalid JSON or date format | Logged; record skipped |

All errors and warnings are logged via `fivetran_connector_sdk.Logging`.

---

## Example logs

```
Starting sync from SAP Ariba API...
Total records to process for table orders: 500
Processed 1000 records for items
Checkpoint created
Sync completed successfully.
```

---

## Local testing

Run the connector locally for debugging:

```bash
fivetran debug --configuration configuration.json
```

### Example debug summary
```
Operation       | Calls
----------------+------------
Upserts         | 859
Updates         | 0
Deletes         | 0
Truncates       | 0
SchemaChanges   | 2
Checkpoints     | 3
```

---

## Deployment

Deploy the connector to Fivetran:

```bash
fivetran deploy --destination <DESTINATION_NAME>                 --connection ariba_purchase_orders                 --configuration configuration.json
```

---

## Additional considerations

* The Ariba Sandbox API is rate-limited — adjust retry delay (`__RETRY_DELAY`) if needed.
* In production, update `__BASE_URL` to point to your tenant’s Ariba API endpoint.
* All timestamps are normalized to UTC (`UTC_DATETIME`).
* Ensure your API key has access to Purchase Order and Item endpoints.
* The connector supports pagination and incremental syncs out of the box.

---