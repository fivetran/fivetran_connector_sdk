# Crypto.com Connector

## Connector overview

The Crypto.com Connector is a comprehensive data integration solution that extracts cryptocurrency trading data, wallet information, and market data from the Crypto.com Exchange API v1. This connector provides real-time and historical data for cryptocurrency portfolio tracking, trading analysis, and market monitoring.

The connector fetches data from multiple Crypto.com Exchange API v1 endpoints including:
- **Exchange Information**: All available trading pairs, symbols, and market rules
- **Market Data**: 24-hour price change statistics and ticker information
- **Account Data**: Wallet balances, account permissions, and trading capabilities
- **Trading History**: Complete trade history with detailed transaction information
- **Order History**: All orders placed on the exchange with incremental sync
- **Positions**: Current trading positions and margin information
- **Open Orders**: Currently active orders on the exchange

This connector is designed for cryptocurrency traders, portfolio managers, and financial analysts who need comprehensive data from Crypto.com for analysis, reporting, and decision-making.

## Requirements

- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)   
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

- **Comprehensive Data Coverage**: Extracts data from 8 major Crypto.com API endpoints
- **Incremental Sync**: Order history uses incremental sync to fetch only new data since last sync
- **Optimized Performance**: Single API calls for open orders and order history (no per-instrument loops)
- **Rate Limit Handling**: Built-in retry logic with exponential backoff
- **Data Type Compliance**: Automatic conversion of complex data types for Fivetran SDK compatibility
- **Robust Error Handling**: Comprehensive error handling and logging
- **Time-based Sync Control**: Configurable sync intervals for different endpoint types

## Configuration file

The connector requires the following configuration parameters in `configuration.json`:

```json
{
  "api_key": "YOUR_CRYPTO_COM_API_KEY",
  "api_secret": "YOUR_CRYPTO_COM_API_SECRET",
  "log": "true",
  "hours_between_syncs": "12.0"
}
```

### Configuration Parameters

| Parameter | Description | Required | Default |
|-----------|-------------|----------|---------|
| `api_key` | Your Crypto.com API key | Yes | - |
| `api_secret` | Your Crypto.com API secret | Yes | - |
| `log` | Enable file logging (true/false) | No | "false" |
| `hours_between_syncs` | Hours between syncs for public endpoints | No | "12.0" |

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file

The `requirements.txt` file specifies the Python libraries required by the connector:

```
requests==2.32.4
```

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication

The connector uses API key authentication with HMAC SHA256 signatures according to the [Crypto.com Exchange API v1 specification](https://exchange-docs.crypto.com/exchange/v1/rest-ws/index.html#common-api-reference).

### Obtaining API Credentials

1. Create a Crypto.com Exchange account
2. Navigate to API Management in your account settings
3. Generate new API credentials with appropriate permissions:
   - **Read permissions**: Required for all data extraction
   - **Trade permissions**: Optional, only needed for order management
4. Copy your API key and secret
5. Configure the connector with your credentials

The signature is created using: `HMAC-SHA256(api_secret, method + id + api_key + paramsString + nonce)`

## Pagination

The connector handles pagination efficiently:

- **Public Endpoints**: Fetched once per sync with time-based intervals
- **Private Endpoints**: Always synced on every run
- **Order History**: Uses incremental sync with time-based pagination
- **Rate Limiting**: Implements 100ms delays between requests and batch processing

## Data handling

The connector processes and transforms data as follows:

- **Schema Mapping**: Maps API response fields to standardized table schemas
- **Data Type Conversion**: Automatically converts lists and dictionaries to JSON strings for Fivetran SDK compatibility
- **Incremental Sync**: Order history uses state-based incremental sync to fetch only new data
- **Time Zone Handling**: All timestamps are handled in UTC
- **Data Cleaning**: Complex nested data structures are preserved as JSON strings

## Error handling

The connector implements comprehensive error handling strategies:

- **Retry Logic**: Exponential backoff for transient errors (up to 3 attempts)
- **Rate Limit Handling**: Automatic retry with increasing delays for rate limit errors
- **Authentication Errors**: Clear error messages for invalid credentials
- **Data Validation**: Type checking and conversion for Fivetran SDK compatibility
- **Logging**: Detailed logging at INFO, WARNING, and SEVERE levels
- **Graceful Degradation**: Continues processing other endpoints if one fails

## Tables created

The connector creates the following tables in your destination:

### Public Data Tables
- **instrument**: Trading pairs and market information
- **ticker**: Real-time market data and price statistics
- **trade**: Recent trade execution data
- **book**: Order book data (bids/asks)
- **candlestick**: Historical OHLCV price data
### Private Data Tables
- **user_balance**: Account wallet balances and permissions
- **open_order**: Currently active orders
- **order_history**: Complete order history with incremental sync
- **user_position**: Current trading positions (available but not actively synced)

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.

### Performance Optimizations

- **Single API Calls**: Open orders and order history use single API calls instead of per-instrument loops
- **Batch Processing**: Public endpoints are processed in batches with rate limiting
- **Incremental Sync**: Order history only fetches new data since last sync
- **Efficient State Management**: Proper checkpointing to resume from last successful sync

### Known Limitations

- **API Rate Limits**: Crypto.com has rate limits that may affect sync speed
- **Data Volume**: Large accounts may require longer sync times
- **API Permissions**: Some data may not be available depending on account permissions
- **Time Range**: Order history defaults to 60 days for initial sync, then incremental

### Troubleshooting

- **No Data**: Check API credentials and permissions
- **Rate Limit Errors**: The connector automatically retries with exponential backoff
- **Sync Failures**: Check logs for detailed error messages
- **Missing Data**: Verify account has trading activity and sufficient permissions
