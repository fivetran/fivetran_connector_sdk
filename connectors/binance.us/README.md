# Binance.US Connector

## Connector overview

The Binance.US connector is a comprehensive data integration solution that extracts cryptocurrency trading data, wallet information, and market data using the Binance.US API. This connector provides real-time and historical data for cryptocurrency portfolio tracking, trading analysis, and market monitoring.

The connector fetches data from multiple Binance.US API endpoints including:
- Exchange information (refer to the `fetch_exchange_info()` function)
- Market data (refer to the `fetch_ticker_prices()` function)  
- Account data (refer to the `fetch_account_info()` and `fetch_wallet_details()` functions)
- Trading history (refer to the `fetch_trade_history()` function)
- Order history (refer to the `fetch_order_history()` function)
- Server time (refer to the `fetch_server_time()` function)

This connector is designed for cryptocurrency traders, portfolio managers, and financial analysts who need comprehensive data from Binance.US for analysis, reporting, and decision-making.

Note: Binance.US is only available to US residents.

## Requirements

- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)   
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

- Comprehensive data coverage: Fetches exchange info, market data, account balances, trade history, and order history (refer to `update()` function, lines 425-617)
- Incremental sync support: Uses timestamp-based incremental syncing to efficiently process only new data (refer to state management, lines 430-437)
- Rate limit handling: Implements exponential backoff retry logic to handle API rate limits gracefully (refer to `make_authenticated_request()` and `make_public_request()` functions, lines 84-227)
- Authentication support: Secure API key and secret management for authenticated endpoints (refer to `validate_configuration()` and authentication setup, lines 27-38, 441-450)
- Error handling: Robust error handling with detailed logging for troubleshooting (refer to error handling in API request functions)
- Real-time market data: 24-hour ticker statistics and price change information
- Portfolio tracking: Complete wallet balance tracking across all supported assets including held coins detection

## Configuration file

The connector requires the following configuration parameters in the `configuration.json` file:

```json
{
  "api_key": "YOUR_BINANCE_US_API_KEY",
  "api_secret": "YOUR_BINANCE_US_API_SECRET"
}
```

Configuration parameters:

- api_key (required): Your Binance.US API key for authentication.
- api_secret (required): Your Binance.US API secret for request signing.

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file

The `requirements.txt` file specifies the Python libraries required by the connector. This connector uses only standard library modules and the pre-installed `requests` library, so no additional dependencies are required.

Example content of `requirements.txt`:

```
# No additional dependencies required
# Uses standard library and pre-installed requests
```

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication

The Binance.US connector uses API key authentication with HMAC-SHA256 signature verification. To obtain your API credentials:

1. Log in to your [Binance.US account](https://www.binance.us/login).
2. Go to **Account** > **API Management** and enable API access.
3. Generate and make a note of the new API key and secret.
4. Configure the API key with appropriate permissions:
   - Read info: Required for account and balance information
   - Enable trading: Required for trade and order history (optional)
   - Enable withdrawals: Not required for this connector
5. (Optional) Set IP restrictions for enhanced security.
6. Add your API key and secret to the `configuration.json` file.

Security best practices:

- Never share your API credentials.
- Use IP restrictions when possible.
- Regularly rotate your API keys.
- Monitor API usage for unusual activity.

## Pagination

The connector implements intelligent pagination and incremental syncing:

- Trade history: Fetches trades in batches of 1000 records per symbol, using timestamp-based pagination
- Order history: Retrieves orders in batches of 1000 records with timestamp filtering
- Incremental sync: Uses checkpoint state to track the last sync time for each data type
- Rate limit management: Implements exponential backoff with up to 3 retry attempts
- Symbol limiting: Limits trade history fetching to the first 10 symbols with balances to avoid rate limits

## Data handling

The connector processes and transforms data as follows:

- **Schema Mapping**: Maps Binance US API responses to standardized table schemas
- **Data Types**: Preserves original data types while ensuring compatibility
- **Timestamp Handling**: Converts Unix timestamps to ISO format for consistency
- **Balance Calculations**: Computes total balances from free and locked amounts
- **Symbol Filtering**: Only processes symbols with non-zero balances for efficiency
- **Error Recovery**: Continues processing other symbols if one fails

**Data Processing Flow:**
1. Fetch public market data (exchange info, ticker prices)
2. Authenticate and fetch account information
3. Process account balances and identify active symbols
4. Fetch trade history for symbols with balances
5. Fetch order history with timestamp filtering
6. Update checkpoint state for incremental sync

## Error handling

The connector implements comprehensive error handling strategies (refer to error handling in API request functions, lines 84-227):

- Retry logic: Exponential backoff retry for transient failures (429, 5xx status codes)
- Request timeouts: 30-second timeout for all API requests
- Symbol-level error isolation: If one symbol fails, processing continues with others
- Detailed logging: Comprehensive logging at INFO, WARNING, and SEVERE levels
- Graceful degradation: Continues operation even if some data sources fail
- State preservation: Maintains checkpoint state even during partial failures

Error categories:
- Authentication errors: Invalid API credentials or permissions
- Rate limit errors: API rate limit exceeded (handled with backoff)
- Network errors: Connection timeouts or network issues
- Data errors: Malformed API responses or missing required fields

## Tables created

The connector creates the following tables in your destination (refer to `schema()` function, lines 406-422):

### EXCHANGE_INFO
Contains information about all trading pairs and symbols available on Binance US (populated by `fetch_exchange_info()`, line 231).
- Primary Key: symbol, run_id
- Key Fields: symbol, baseAsset, quoteAsset, status, orderTypes, filters

### TICKER_PRICE  
Contains 24-hour price change statistics for all trading pairs (populated by `fetch_ticker_prices()`, line 259).
- Primary Key: symbol, run_id
- Key Fields: symbol, priceChange, priceChangePercent, weightedAvgPrice, prevClosePrice, lastPrice

### ACCOUNT_BALANCE
Contains wallet balance information for all assets in your account (populated by `fetch_account_info()`, line 281).
- Primary Key: asset, run_id
- Key Fields: asset, free, locked, total, accountType, canTrade, canWithdraw, canDeposit

### WALLET_DETAILS
Contains wallet details and asset status for all coins (populated by `fetch_wallet_details()`, line 297). This table tracks free, locked, frozen, and withdrawing balances for all supported assets on Binance US.
- Primary Key: coin, run_id
- Key Fields: coin, free, locked, frozen (renamed from freeze to avoid SQL keyword conflict), withdrawing, depositAllEnable, withdrawAllEnable, networkList

### TRADE
Contains detailed information about all trades executed on your account (populated by `fetch_trade_history()`, line 319).
- Primary Key: id, run_id
- Key Fields: id, orderId, orderListId, price, qty, quoteQty, commission, commissionAsset, time, isBuyer

### ORDER
Contains information about all orders placed on your account (populated by `fetch_order_history()`, line 361).
- Primary Key: orderId, run_id
- Key Fields: orderId, symbol, orderListId, clientOrderId, price, origQty, executedQty, status, timeInForce, type

### SERVER_TIME
Contains the current server time from Binance US for synchronization purposes (populated by `fetch_server_time()`, line 241).
- Primary Key: serverTime, run_id
- Key Fields: serverTime

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.

Important notes:
- This connector requires valid Binance US API credentials with appropriate permissions
- Rate limits apply to all API endpoints - the connector implements backoff strategies
- The connector processes data incrementally to minimize API usage
- Some endpoints may require additional permissions based on your account type
- Monitor your API usage to avoid hitting rate limits in production environments
- Binance US is only available to US residents and has different trading pairs than international Binance
