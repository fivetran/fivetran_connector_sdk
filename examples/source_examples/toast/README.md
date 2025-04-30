# Toast Connector Example

This example demonstrates how to use the Fivetran Connector SDK to integrate with the Toast platform API, enabling synchronization of restaurant data including orders, labor, configuration, and cash management information.

## Requirements

- Python 3.7 or later
- Operating System:
  - Windows 10 or later
  - macOS 13 or later
- Toast Developer Account
- Valid Toast API credentials:
  - Client ID
  - Client Secret
  - User Access Type
  - Domain

## Getting Started

Follow the [Fivetran Connector SDK setup guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

- Comprehensive Toast API integration
- Multi-endpoint data synchronization:
  - Configuration data
  - Labor information
  - Cash management
  - Order details
- Automatic schema discovery
- Efficient batch processing
- State checkpointing for reliable resumption
- Robust error handling and logging

## Configuration File

The connector requires the following parameters in the configuration file (`configuration.json`):

```json
{
    "clientId": "<YOUR_CLIENT_ID>",
    "clientSecret": "<YOUR_CLIENT_SECRET>",
    "userAccessType": "<YOUR_ACCESS_TYPE>",
    "domain": "<YOUR_DOMAIN>"
}
```

## Authentication

The connector uses OAuth2 client credentials flow:
1. Obtain client credentials from Toast Developer Portal
2. Configure the credentials in your configuration file
3. The connector automatically handles token management and renewal
4. Keep credentials secure and never commit them to version control

## Data Handling

### Endpoints and Data Types
The connector syncs data from multiple Toast API endpoints:

1. **Configuration Data**
   - Restaurant settings
   - Menu items
   - Pricing information

2. **Labor Data**
   - Employee information
   - Shift details
   - Time entries

3. **Cash Management**
   - Cash drawer activities
   - Payments
   - Reconciliation data

4. **Orders**
   - Order details
   - Item modifications
   - Payment information

### Processing Features
- Incremental updates based on timestamps
- Batch processing for efficient data transfer
- Automatic schema mapping
- Data type conversion and validation

### Error Handling
- API credential validation
- Connection error management
- Rate limit compliance
- Comprehensive error logging
- Automatic retries with backoff

## Additional Considerations for Production Use

1. **API Rate Limits**
   - Monitor API usage
   - Implement appropriate request delays
   - Handle rate limit errors gracefully

2. **Data Volume**
   - Optimize batch sizes
   - Configure appropriate sync intervals
   - Monitor memory usage

3. **Error Handling**
   - Implement comprehensive error reporting
   - Set up alerts for critical failures
   - Maintain detailed logs

4. **Monitoring**
   - Track sync statistics
   - Monitor API response times
   - Set up performance metrics

5. **Security**
   - Rotate credentials regularly
   - Use environment variables
   - Implement audit logging
   - Follow security best practices

6. **Restaurant-Specific Considerations**
   - Handle timezone differences
   - Account for business hours
   - Consider peak operation times
   - Plan for menu updates

## Disclaimer

This example is provided for learning purposes and should be thoroughly tested and modified before use in a production environment. Fivetran is not responsible for any consequences resulting from the use of this example.

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

## How It Works

1. **Authentication**: Generates and caches a Toast token using the provided credentials.
2. **Sync Loop**: Runs in 30-day time chunks, paginating through endpoints.
3. **Data Normalization**: Flattens nested objects and lists using `flatten_dict`, `extract_fields`, and `stringify_lists`.
4. **Upserts and Deletes**: Emits operations using `op.upsert()` and `op.delete()`.
5. **Checkpointing**: Updates state after each window to resume seamlessly.

---

## Data Coverage

### Core Tables
- `restaurant`
- `job`, `employee`, `shift`, `break`, `time_entry`
- `orders`, `orders_check`, `payment`

### Configuration
- `menu`, `menu_item`, `menu_group`, `discounts`, `tables`, etc.

### Nested Children
- `orders_check_payment`, `orders_check_selection`, `orders_check_selection_modifier`, etc.

### Cash Management
- `cash_entry`, `cash_deposit`

---


## Error Handling

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

