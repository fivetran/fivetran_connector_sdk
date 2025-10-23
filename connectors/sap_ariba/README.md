# SAP Ariba Purchase Orders Connector Example

## Connector overview
The SAP Ariba Purchase Orders connector demonstrates how to use the Fivetran Connector SDK to extract **purchase order (PO)** and **line-item** data from the [SAP Ariba API](https://api.sap.com/api/api_purchase_orders/overview). It loads the data into a Fivetran destination for analytics and reporting.

The connector supports **incremental syncing** by maintaining timestamps in its internal state. It fetches new or modified purchase orders and items and marks processed rows with checkpoints for reliable continuation.

It maintains two tables:
- `ORDER` – contains header-level purchase order metadata.
- `ITEM` – contains detailed line-item records for each order.

This example uses the **SAP Ariba Sandbox API** but can easily be configured for production environments.

## Requirements
- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
    - Windows: 10 or later (64-bit only)
    - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
    - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)
- SAP Ariba API key (sandbox or production)
- Outbound internet access to `sandbox.api.sap.com` (or your production Ariba host)

## Getting started
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) for setup instructions, including installing dependencies, configuring authentication, and running local tests.

## Features
- Fetches purchase orders and line items from the SAP Ariba API.
- Performs **incremental syncs** using internal timestamp checkpoints.
- Supports pagination with `$skip` and `$top` query parameters.
- Converts timestamps to ISO 8601 UTC format.
- Retries automatically on network failures or rate limit errors.
- Performs schema-aware upserts for idempotent loading into Fivetran destinations.

## Configuration file
The connector reads configuration settings from `configuration.json`, which defines the authentication parameters for SAP Ariba.

```
{
  "APIKey": "YOUR_SAP_ARIBA_API_KEY"
}
```

| Key | Required | Description |
|-----|-----------|-------------|
| `APIKey` | Yes | Your SAP Ariba API key for authentication. |

Note: The connector defaults to the Ariba Sandbox API endpoint (`https://sandbox.api.sap.com/ariba/api/purchase-orders/v1/sandbox/`). Replace it with your production base URL if applicable.

## Requirements file
The `requirements.txt` file specifies Python dependencies required by the connector.

```
fivetran-connector-sdk
requests
```

Note: Both `fivetran_connector_sdk` and `requests` are pre-installed in the Fivetran environment. This file is only required for local testing.

## Authentication
The connector authenticates using a single API key passed in the request headers.  
Each request includes:

```
APIKey: <YOUR_API_KEY>
Accept: application/json
DataServiceVersion: 2.0
```

No OAuth or password-based authentication is needed.

## Pagination
Pagination is handled in the data extraction functions using the `$skip` and `$top` query parameters.  
The connector iterates through API pages until it receives an empty response, ensuring complete data retrieval.

Example:
```
GET /purchase-orders?$top=100&$skip=200
```

## Data handling
The connector uses the `update(configuration, state)` function to:
1. Fetch data from the `/purchase-orders` and related endpoints.
2. Parse JSON responses and flatten nested structures into relational rows.
3. Convert Ariba timestamp fields (e.g., `"16 May 2019 1:41:26 AM"`) to ISO UTC.
4. Emit `Upsert` operations for every record using `op.upsert()`.
5. Save sync progress with checkpoints via `op.checkpoint(state)`.

The connector also defines schemas and primary keys for both the `orders` and `items` tables through the `schema(configuration)` function.

## Error handling
Refer to the HTTP request logic in the connector implementation.

The connector implements the following error handling:
- `400–499`: Client-side errors (e.g., invalid key or malformed request) – connector logs and exits.
- `429`: API rate limit exceeded – connector retries automatically using exponential backoff.
- `500–599`: Server errors – connector retries after a short delay.
- Network issues: Automatically retried up to 3 times.
- Invalid JSON or date parsing errors: Skipped gracefully and logged for review.

All logs and exceptions are managed using the `fivetran_connector_sdk.Logging` interface.

## Tables created
The connector creates two destination tables.

### ORDER
Primary key: `payload_id`, `revision`, `row_id`

| Column | Type | Description |
|--------|------|-------------|
| `documentNumber` | STRING | Unique purchase order number. |
| `orderDate` | UTC_DATETIME | Date when the order was created. |
| `supplierName` | STRING | Name of the supplier. |
| `supplierANID` | STRING | Supplier Ariba Network ID. |
| `buyerANID` | STRING | Buyer Ariba Network ID. |
| `customerName` | STRING | Name of the customer. |
| `systemId` | STRING | Ariba system identifier. |
| `payloadId` | STRING | Unique payload identifier. |
| `revision` | STRING | Revision number or version. |
| `endpointId` | STRING | Endpoint identifier. |
| `created` | UTC_DATETIME | Creation timestamp. |
| `status` | STRING | Status of the order. |
| `documentStatus` | STRING | Confirmation state (Confirmed/Unconfirmed). |
| `amount` | DOUBLE | Total purchase order amount. |
| `numberOfInvoices` | INT | Number of invoices linked to the order. |
| `invoiced_amount` | DOUBLE | Total invoiced amount. |
| `company_code` | STRING | Internal company code. |
| `row_id` | INT | Sequential ID used for uniqueness. |

### ITEM
Primary key: `document_number`, `line_number`, `row_id`

| Column | Type | Description |
|--------|------|-------------|
| `documentNumber` | STRING | Associated purchase order number. |
| `lineNumber` | INT | Line item number. |
| `quantity` | DOUBLE | Ordered quantity. |
| `unitOfMeasure` | STRING | Measurement unit for the item. |
| `supplierPart` | STRING | Supplier’s part identifier. |
| `buyerPartId` | STRING | Buyer’s part identifier. |
| `manufacturerPartId` | STRING | Manufacturer’s part number. |
| `description` | STRING | Description of the item. |
| `itemShipToName` | STRING | Recipient name for shipment. |
| `itemShipToStreet` | STRING | Street address of recipient. |
| `itemShipToCity` | STRING | City of destination. |
| `itemShipToState` | STRING | State or province. |
| `itemShipToPostalCode` | STRING | Postal code. |
| `itemShipToCountry` | STRING | Country name. |
| `isoCountryCode` | STRING | ISO 2-letter country code. |
| `itemShipToCode` | STRING | Internal ship-to code. |
| `itemLocation` | STRING | Delivery location name. |
| `requestedDeliveryDate` | UTC_DATETIME | Requested delivery date. |
| `requestedShipmentDate` | UTC_DATETIME | Requested shipment date. |
| `row_id` | INT | Sequential ID used for uniqueness. |

## Additional files
- `sap_ariba_connector.py` – Main connector implementation, including schema and update logic.
- `helpers.py` – Handles date parsing and pagination utilities.
- `constants.py` – Stores default API URLs, retry intervals, and state key names.

## Additional considerations
The example is intended for demonstration and educational purposes using the Fivetran Connector SDK.  
When adapting this connector for production:
- Replace the Sandbox API URL with your tenant’s production endpoint.
- Increase retry delay (`__RETRY_DELAY`) if you encounter rate limits.
- Confirm that your API key has permissions for both Purchase Order and Item endpoints.
- All timestamps are normalized to UTC (`UTC_DATETIME`).
- The connector is idempotent and safe for repeated runs.

For questions or issues, contact Fivetran Support.
