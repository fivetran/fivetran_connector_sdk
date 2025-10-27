"""
Binance US Connector for Fivetran SDK
This connector fetches cryptocurrency data, wallet balances, trading history, and market information from Binance US API.
See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

import json
import time
import hmac
import hashlib
import urllib.parse
import uuid
from typing import Dict, List, Optional

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

# Constants for API configuration and retry logic
__MAX_RETRIES = 3
__BASE_DELAY = 1
__BINANCE_BASE_URL = "https://api.binance.us"


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing.
    """
    required_configs = ["api_key", "api_secret"]
    for key in required_configs:
        if key not in configuration:
            raise ValueError(f"Missing required configuration value: {key}")


def create_signature(query_string: str, api_secret: str) -> str:
    """
    Create HMAC SHA256 signature for authenticated requests.
    Args:
        query_string: The query string to sign
        api_secret: The API secret key
    Returns:
        The signature string
    """
    return hmac.new(
        api_secret.encode("utf-8"), query_string.encode("utf-8"), hashlib.sha256
    ).hexdigest()


def transform_data_for_fivetran(data: Dict) -> Dict:
    """
    Transform data to ensure all values are Fivetran-compatible types.
    Converts lists to JSON strings and handles other data type conversions.
    Args:
        data: The data dictionary to transform
    Returns:
        Transformed data dictionary with Fivetran-compatible types
    """
    transformed = {}

    for key, value in data.items():
        if isinstance(value, list):
            # Convert lists to JSON strings
            transformed[key] = json.dumps(value)
        elif isinstance(value, dict):
            # Convert nested dictionaries to JSON strings
            transformed[key] = json.dumps(value)
        elif isinstance(value, (int, float, str, bool)) or value is None:
            # Keep primitive types as-is
            transformed[key] = value
        else:
            # Convert any other type to string
            transformed[key] = str(value)

    return transformed


def make_authenticated_request(
    endpoint: str, api_key: str, api_secret: str, params: Optional[Dict] = None
) -> Dict:
    """
    Make an authenticated request to Binance US API with retry logic.
    Args:
        endpoint: The API endpoint to call
        api_key: The API key for authentication
        api_secret: The API secret for signing requests
        params: Optional query parameters
    Returns:
        JSON response from the API
    """
    url = f"{__BINANCE_BASE_URL}{endpoint}"

    if params is None:
        params = {}

    # Add timestamp for authenticated requests
    params["timestamp"] = int(time.time() * 1000)

    # Create query string and signature
    query_string = urllib.parse.urlencode(params)
    signature = create_signature(query_string, api_secret)
    params["signature"] = signature

    headers = {"X-MBX-APIKEY": api_key, "Content-Type": "application/json"}

    for attempt in range(__MAX_RETRIES):
        try:
            import requests

            response = requests.get(url, params=params, headers=headers, timeout=30)

            if response.status_code == 200:
                return response.json()
            elif response.status_code == 451:
                # Geographic restriction error
                log.severe(
                    "Binance US API is not available in your current location due to geographic restrictions."
                )
                log.severe("Please try one of the following solutions:")
                log.severe("1. Use a VPN to connect from the US")
                log.severe(
                    "2. Contact Binance US support if you believe this is an error"
                )
                raise RuntimeError(
                    "Geographic restriction: Binance US API not available in your location. Please use VPN."
                )
            elif response.status_code in [429, 500, 502, 503, 504]:
                if attempt < __MAX_RETRIES - 1:
                    delay = __BASE_DELAY * (2**attempt)
                    log.warning(
                        f"Request failed with status {response.status_code}, retrying in {delay} seconds (attempt {attempt + 1}/{__MAX_RETRIES})"
                    )
                    time.sleep(delay)
                    continue
                else:
                    log.severe(
                        f"Failed to fetch data after {__MAX_RETRIES} attempts. Last status: {response.status_code} - {response.text}"
                    )
                    raise RuntimeError(
                        f"API returned {response.status_code} after {__MAX_RETRIES} attempts: {response.text}"
                    )
            else:
                log.severe(
                    f"API request failed with status {response.status_code}: {response.text}"
                )
                raise RuntimeError(
                    f"API request failed: {response.status_code} - {response.text}"
                )

        except requests.exceptions.RequestException as e:
            if attempt < __MAX_RETRIES - 1:
                delay = __BASE_DELAY * (2**attempt)
                log.warning(
                    f"Request exception occurred, retrying in {delay} seconds: {str(e)}"
                )
                time.sleep(delay)
                continue
            else:
                log.severe(f"Request failed after {__MAX_RETRIES} attempts: {str(e)}")
                raise RuntimeError(f"Request failed: {str(e)}")

    # This should never be reached, but added for type safety
    raise RuntimeError("Unexpected error in make_authenticated_request")


def make_public_request(endpoint: str, params: Optional[Dict] = None) -> Dict:
    """
    Make a public (unauthenticated) request to Binance US API.
    Args:
        endpoint: The API endpoint to call
        params: Optional query parameters
    Returns:
        JSON response from the API
    """
    url = f"{__BINANCE_BASE_URL}{endpoint}"

    for attempt in range(__MAX_RETRIES):
        try:
            import requests

            response = requests.get(url, params=params, timeout=30)

            if response.status_code == 200:
                return response.json()
            elif response.status_code == 451:
                # Geographic restriction error
                log.severe(
                    "Binance US API is not available in your current location due to geographic restrictions."
                )
                log.severe("Please try one of the following solutions:")
                log.severe("1. Use a VPN to connect from the US")
                log.severe(
                    "2. Contact Binance US support if you believe this is an error"
                )
                raise RuntimeError(
                    "Geographic restriction: Binance US API not available in your location. Please use VPN."
                )
            elif response.status_code in [429, 500, 502, 503, 504]:
                if attempt < __MAX_RETRIES - 1:
                    delay = __BASE_DELAY * (2**attempt)
                    log.warning(
                        f"Request failed with status {response.status_code}, retrying in {delay} seconds (attempt {attempt + 1}/{__MAX_RETRIES})"
                    )
                    time.sleep(delay)
                    continue
                else:
                    log.severe(
                        f"Failed to fetch data after {__MAX_RETRIES} attempts. Last status: {response.status_code} - {response.text}"
                    )
                    raise RuntimeError(
                        f"API returned {response.status_code} after {__MAX_RETRIES} attempts: {response.text}"
                    )
            else:
                log.severe(
                    f"API request failed with status {response.status_code}: {response.text}"
                )
                raise RuntimeError(
                    f"API request failed: {response.status_code} - {response.text}"
                )

        except requests.exceptions.RequestException as e:
            if attempt < __MAX_RETRIES - 1:
                delay = __BASE_DELAY * (2**attempt)
                log.warning(
                    f"Request exception occurred, retrying in {delay} seconds: {str(e)}"
                )
                time.sleep(delay)
                continue
            else:
                log.severe(f"Request failed after {__MAX_RETRIES} attempts: {str(e)}")
                raise RuntimeError(f"Request failed: {str(e)}")

    # This should never be reached, but added for type safety
    raise RuntimeError("Unexpected error in make_public_request")


def fetch_exchange_info() -> Dict:
    """
    Fetch exchange information including all trading pairs and symbols from Binance US.
    Args:
        run_id: Unique identifier for this sync run
    Returns:
        Exchange information data with run_id
    """
    log.info("Fetching exchange information from Binance US")
    response = make_public_request("/api/v3/exchangeInfo")
    return response


def fetch_server_time(run_id: str) -> Dict:
    """
    Fetch server time from Binance US.
    Args:
        run_id: Unique identifier for this sync run
    Returns:
        Server time data with run_id
    """
    log.info("Fetching server time from Binance US")
    response = make_public_request("/api/v3/time")
    response["run_id"] = run_id
    return response


def fetch_ticker_prices(run_id: str) -> List[Dict]:
    """
    Fetch 24hr ticker price change statistics for all symbols from Binance US.
    Args:
        run_id: Unique identifier for this sync run
    Returns:
        List of ticker price data with run_id
    """
    log.info("Fetching ticker prices from Binance US")
    response = make_public_request("/api/v3/ticker/24hr")

    # Ensure we always return a list and add run_id to each record
    if isinstance(response, list):
        log.info(f"Fetched {len(response)} ticker price records")
        for ticker in response:
            ticker["run_id"] = run_id
        return response
    else:
        log.warning(f"Expected list but got {type(response)}, wrapping in list")
        response["run_id"] = run_id
        return [response]


def fetch_account_info(api_key: str, api_secret: str, run_id: str) -> Dict:
    """
    Fetch account information including balances from Binance US.
    Args:
        api_key: The API key for authentication
        api_secret: The API secret for signing requests
        run_id: Unique identifier for this sync run
    Returns:
        Account information data with run_id
    """
    log.info("Fetching account information from Binance US")
    response = make_authenticated_request("/api/v3/account", api_key, api_secret)
    response["run_id"] = run_id
    return response


def fetch_wallet_details(api_key: str, api_secret: str, run_id: str) -> List[Dict]:
    """
    Fetch wallet details and asset status from Binance US.
    Args:
        api_key: The API key for authentication
        api_secret: The API secret for signing requests
        run_id: Unique identifier for this sync run
    Returns:
        List of wallet details and asset status data with run_id
    """
    log.info("Fetching wallet details and asset status from Binance US")
    response = make_authenticated_request(
        "/sapi/v1/capital/config/getall", api_key, api_secret
    )

    # Add run_id to each wallet detail record
    if isinstance(response, list):
        for wallet_detail in response:
            wallet_detail["run_id"] = run_id
        return response
    else:
        return [response]


def fetch_trade_history(
    api_key: str,
    api_secret: str,
    symbol: str,
    run_id: str,
    start_time: Optional[int] = None,
    end_time: Optional[int] = None,
    limit: int = 1000,
) -> List[Dict]:
    """
    Fetch trade history for a specific symbol.
    Args:
        api_key: The API key for authentication
        api_secret: The API secret for signing requests
        symbol: The trading symbol (e.g., 'BTCUSDT')
        run_id: Unique identifier for this sync run
        start_time: Start time in milliseconds (optional)
        end_time: End time in milliseconds (optional)
        limit: Maximum number of trades to fetch
    Returns:
        List of trade history data with run_id
    """
    log.info(f"Fetching trade history for symbol: {symbol}")
    params = {"symbol": symbol, "limit": limit}

    if start_time:
        params["startTime"] = start_time
    if end_time:
        params["endTime"] = end_time

    response = make_authenticated_request(
        "/api/v3/myTrades", api_key, api_secret, params
    )
    log.fine(json.dumps(response, indent=2))

    # Add run_id to each trade record
    if isinstance(response, list):
        for trade in response:
            trade["run_id"] = run_id
        return response
    else:
        return [response]


def fetch_order_history(
    api_key: str,
    api_secret: str,
    symbol: str,
    run_id: str,
    start_time: Optional[int] = None,
    end_time: Optional[int] = None,
    limit: int = 1000,
) -> List[Dict]:
    """
    Fetch order history for a specific symbol.
    Args:
        api_key: The API key for authentication
        api_secret: The API secret for signing requests
        symbol: The trading symbol (required for Binance US)
        run_id: Unique identifier for this sync run
        start_time: Start time in milliseconds (optional)
        end_time: End time in milliseconds (optional)
        limit: Maximum number of orders to fetch
    Returns:
        List of order history data with run_id
    """
    log.info(f"Fetching order history for symbol: {symbol}")
    params = {"symbol": symbol, "limit": limit}

    if start_time:
        params["startTime"] = start_time
    if end_time:
        params["endTime"] = end_time

    response = make_authenticated_request(
        "/api/v3/allOrders", api_key, api_secret, params
    )

    # Log the response
    log.fine(json.dumps(response, indent=2))

    # Add run_id to each order record
    if isinstance(response, list):
        for order in response:
            order["run_id"] = run_id
        return response
    else:
        response["run_id"] = run_id
        return [response]


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    return [
        {"table": "exchange_info", "primary_key": ["symbol", "run_id"]},
        {"table": "ticker_price", "primary_key": ["symbol", "run_id"]},
        {"table": "account_balance", "primary_key": ["asset", "run_id"]},
        {"table": "wallet_details", "primary_key": ["coin", "run_id"]},
        {"table": "trade", "primary_key": ["id", "run_id"]},
        {"table": "order", "primary_key": ["orderId", "run_id"]},
        {"table": "server_time", "primary_key": ["serverTime", "run_id"]},
    ]


def update(configuration: dict, state: dict):
    """
    Define the update function, which is a required function, and is called by Fivetran during each sync.
    See the technical reference documentation for more details on the update function
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    Args:
        configuration: A dictionary containing connection details
        state: A dictionary containing state information from previous runs
        The state dictionary is empty for the first sync or for any full re-sync
    """
    log.info("Starting Binance US connector sync")

    # Generate a unique run ID for this sync
    run_id = str(uuid.uuid4())
    log.info(f"Generated run ID: {run_id}")

    # Validate the configuration to ensure it contains all required values
    validate_configuration(configuration=configuration)

    # Extract configuration parameters
    api_key = configuration.get("api_key")
    api_secret = configuration.get("api_secret")

    # Validate API credentials
    if not api_key or not api_secret:
        raise ValueError("API key and secret are required for authenticated requests")

    # Get state variables for incremental sync
    last_trade_sync = state.get("last_trade_sync", {})
    last_order_sync = state.get("last_order_sync", {})

    try:
        # Fetch server time
        server_time_data = fetch_server_time(run_id)
        op.upsert(
            table="server_time", data=transform_data_for_fivetran(server_time_data)
        )

        # Checkpoint after server time
        op.checkpoint(state=state)
        log.info("Checkpoint: Server time data synced")

        # Fetch exchange information (public data)
        exchange_info = fetch_exchange_info()
        for symbol_info in exchange_info.get("symbols", []):
            symbol_info["run_id"] = run_id
            op.upsert(
                table="exchange_info", data=transform_data_for_fivetran(symbol_info)
            )

        # Checkpoint after exchange info
        op.checkpoint(state=state)
        log.info("Checkpoint: Exchange info data synced")

        # Fetch ticker prices (public data)
        ticker_prices = fetch_ticker_prices(run_id)
        log.info(f"Processing {len(ticker_prices)} ticker price records")
        for i, ticker in enumerate(ticker_prices):
            try:
                op.upsert(
                    table="ticker_price", data=transform_data_for_fivetran(ticker)
                )
                if i < 5:  # Log first 5 for debugging
                    log.fine(
                        f"Upserted ticker {i+1}: {ticker.get('symbol', 'unknown')}"
                    )
            except Exception as e:
                log.warning(f"Failed to upsert ticker {i+1}: {str(e)}")
                continue

        # Checkpoint after ticker prices
        op.checkpoint(state=state)
        log.info("Checkpoint: Ticker price data synced")

        # Fetch account information (authenticated)
        account_info = fetch_account_info(api_key, api_secret, run_id)

        # Process account balances
        for balance in account_info.get("balances", []):
            if float(balance.get("free", 0)) > 0 or float(balance.get("locked", 0)) > 0:
                balance_record = {
                    "asset": balance.get("asset"),
                    "free": balance.get("free"),
                    "locked": balance.get("locked"),
                    "total": str(
                        float(balance.get("free", 0)) + float(balance.get("locked", 0))
                    ),
                    "accountType": account_info.get("accountType"),
                    "canTrade": account_info.get("canTrade"),
                    "canWithdraw": account_info.get("canWithdraw"),
                    "canDeposit": account_info.get("canDeposit"),
                    "updateTime": account_info.get("updateTime"),
                    "run_id": run_id,
                }
                op.upsert(
                    table="account_balance",
                    data=transform_data_for_fivetran(balance_record),
                )

        # Checkpoint after account balances
        op.checkpoint(state=state)
        log.info("Checkpoint: Account balance data synced")

        # Fetch wallet details and asset status
        wallet_details_data = fetch_wallet_details(api_key, api_secret, run_id)
        log.info(f"Processing {len(wallet_details_data)} wallet detail records")
        for wallet_detail in wallet_details_data:
            try:
                # Rename 'freeze' field to 'frozen' to avoid SQL keyword conflict
                if "freeze" in wallet_detail:
                    wallet_detail["frozen"] = wallet_detail.pop("freeze")
                op.upsert(
                    table="wallet_details",
                    data=transform_data_for_fivetran(wallet_detail),
                )
            except Exception as e:
                log.warning(
                    f"Failed to upsert wallet detail for {wallet_detail.get('coin', 'unknown')}: {str(e)}"
                )
                continue

        # Checkpoint after wallet details
        op.checkpoint(state=state)
        log.info("Checkpoint: Wallet details data synced")

        # Extract held coins from wallet details (free, locked, frozen, or withdrawing > 0)
        held_coins = []
        for wallet_detail in wallet_details_data:
            held_total = float(wallet_detail.get("free", 0)) + float(
                wallet_detail.get("locked", 0)
            )
            held_total += float(wallet_detail.get("frozen", 0)) + float(
                wallet_detail.get("withdrawing", 0)
            )
            if held_total > 0:
                held_coins.append(wallet_detail.get("coin"))
        log.info(f"Found {len(held_coins)} held coins: {held_coins}")

        # Fetch trade history for all symbols with held coins
        symbols_with_balance = held_coins

        # Get common trading pairs for these assets
        common_pairs = []
        for symbol_info in exchange_info.get("symbols", []):
            base_asset = symbol_info.get("baseAsset")
            quote_asset = symbol_info.get("quoteAsset")
            base_quote_asset = [base_asset, quote_asset]
            if any(val in symbols_with_balance for val in base_quote_asset):
                common_pairs.append(symbol_info.get("symbol"))

        # Fetch trade history for each symbol
        for symbol in common_pairs:  # Limit to first 10 symbols to avoid rate limits
            try:
                # Fetch trade history with proper timestamp filtering
                start_time = last_trade_sync.get(symbol)
                trades = fetch_trade_history(
                    api_key,
                    api_secret,
                    symbol,
                    run_id,
                    start_time=start_time,
                    limit=1000,
                )
                log.fine(json.dumps(trades, indent=2))

                for trade in trades:
                    op.upsert(table="trade", data=transform_data_for_fivetran(trade))

                # Update last sync time for this symbol
                if trades:
                    last_trade_sync[symbol] = max(
                        int(trade.get("time", 0)) for trade in trades
                    )

                # Checkpoint after each symbol's trade history
                op.checkpoint(state=state)
                log.info(f"Checkpoint: Trade history for {symbol} synced")

            except Exception as e:
                log.warning(f"Failed to fetch trade history for {symbol}: {str(e)}")
                continue

        # Fetch order history for each symbol with balances
        try:
            for (
                symbol
            ) in common_pairs:  # Limit to first 10 symbols to avoid rate limits
                try:
                    # Fetch order history with proper timestamp filtering
                    start_time = last_order_sync.get(symbol)
                    orders = fetch_order_history(
                        api_key,
                        api_secret,
                        symbol,
                        run_id,
                        start_time=start_time,
                        limit=1000,
                    )

                    for order in orders:
                        op.upsert(
                            table="order", data=transform_data_for_fivetran(order)
                        )

                    # Update last sync time for this symbol
                    if orders:
                        last_order_sync[symbol] = max(
                            int(order.get("time", 0)) for order in orders
                        )

                    # Checkpoint after each symbol's order history
                    op.checkpoint(state=state)
                    log.info(f"Checkpoint: Order history for {symbol} synced")

                except Exception as e:
                    log.warning(f"Failed to fetch order history for {symbol}: {str(e)}")
                    continue

        except Exception as e:
            log.warning(f"Failed to fetch order history: {str(e)}")

        # Update state with current sync time
        current_time = int(time.time() * 1000)
        new_state = {
            "last_sync_time": current_time,
            "last_trade_sync": last_trade_sync,
            "last_order_sync": last_order_sync,
            "last_run_id": run_id,
        }

        # Save the progress by checkpointing the state
        op.checkpoint(state=new_state)
        log.info("Binance US connector sync completed successfully")

    except Exception as e:
        log.severe(f"Failed to sync Binance US data: {str(e)}")
        raise RuntimeError(f"Failed to sync Binance US data: {str(e)}")


# Initialize the connector with the defined update and schema functions
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module
if __name__ == "__main__":
    # Open the configuration.json file and load its contents
    with open("configuration.json", "r") as f:
        configuration = json.load(f)

    # Test the connector locally
    connector.debug(configuration=configuration)
