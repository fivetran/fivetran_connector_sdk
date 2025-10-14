"""
Crypto.com Connector for Fivetran SDK
This connector demonstrates how to fetch cryptocurrency data from
Crypto.com Exchange API v1.
See the Technical Reference documentation
(https://fivetran.com/docs/connectors/connector-sdk/technical
        -reference#update)
and the Best Practices documentation
(https://fivetran.com/docs/connectors/connector-sdk/best-prac
        tices) for details
"""

# For reading configuration from a JSON file
import json

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and
# checkpoint()
from fivetran_connector_sdk import Operations as op

# Additional imports for API functionality
import requests
import time
import hmac
import hashlib
import logging
from typing import Dict, List, Optional, Any

# Set the logging level to enable logging
log.LOG_LEVEL = log.Level.INFO

# Constants for API configuration and retry logic
# Maximum number of retry attempts for API requests
__MAX_RETRIES = 3
# Base delay in seconds for API request retries
__BASE_DELAY = 1
# Base URL for Crypto.com API
__CRYPTO_COM_BASE_URL = "https://api.crypto.com/exchange/v1"

# Rate limiting constants based on Crypto.com API documentation
# 100ms delay between requests to respect rate limits
__RATE_LIMIT_DELAY = 0.1
# 1 second delay between batches of requests
__BATCH_DELAY = 1.0
# Process requests in batches to manage rate limits
__BATCH_SIZE = 10

# Checkpoint constants
# 10 minutes in seconds for periodic checkpoints
__CHECKPOINT_INTERVAL = 600

# Global variable to track if file logging is enabled
_file_logger = None


def setup_file_logging(configuration: dict):
    """
    Set up file logging if the 'log' configuration key is present and set to
    true.
    Defaults to false if the key is not present.
    Args:
        configuration: Configuration dictionary
    """
    global _file_logger

    if configuration.get("log", "false").lower() == "true":
        # Create a file logger
        _file_logger = logging.getLogger("crypto_connector_file")
        _file_logger.setLevel(logging.INFO)

        # Remove any existing handlers to avoid duplicates
        for handler in _file_logger.handlers[:]:
            _file_logger.removeHandler(handler)

        # Create file handler
        file_handler = logging.FileHandler("crypto_connector.log")
        file_handler.setLevel(logging.INFO)

        # Create formatter
        formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s"
        )
        file_handler.setFormatter(formatter)

        # Add handler to logger
        _file_logger.addHandler(file_handler)

        log.info(
            "File logging enabled - logs will be written to "
            "crypto_connector.log"
        )


def log_message(level: str, message: str):
    """
    Log a message to both Fivetran SDK logger and file logger (if enabled).
    Args:
        level: Log level (info, warning, severe)
        message: Log message
    """
    # Log to Fivetran SDK logger
    if level == "info":
        log.info(message)
    elif level == "warning":
        log.warning(message)
    elif level == "severe":
        log.severe(message)

    # Log to file if file logging is enabled
    if _file_logger:
        if level == "info":
            _file_logger.info(message)
        elif level == "warning":
            _file_logger.warning(message)
        elif level == "severe":
            _file_logger.error(message)


def apply_rate_limit():
    """
    Apply rate limiting delay between API requests to respect Crypto.com API
    limits.
    Based on the API documentation, we need to respect rate limits to avoid
    TOO_MANY_REQUESTS errors.
    """
    time.sleep(__RATE_LIMIT_DELAY)


def process_in_batches(
    items: List[Any],
    batch_size: int = __BATCH_SIZE,
    operation_name: str = "processing",
):
    """
    Process items in batches with rate limiting to respect API limits.
    Args:
        items: List of items to process
        batch_size: Number of items to process per batch
        operation_name: Name of the operation for logging
    Yields:
        Batches of items
    """
    total_batches = (len(items) + batch_size - 1) // batch_size
    log_message(
        "info",
        f"Processing {len(items)} items in {total_batches} batches of"
        f" {batch_size} for {operation_name}",
    )

    for i in range(0, len(items), batch_size):
        batch = items[i:i + batch_size]
        batch_num = (i // batch_size) + 1
        log_message(
            "info",
            f"Processing batch {batch_num}/{total_batches} for "
            f"{operation_name} ({len(batch)} items)",
        )
        yield batch

        # Add delay between batches (except for the last batch)
        if i + batch_size < len(items):
            log_message(
                "info",
                f"Rate limiting: waiting {__BATCH_DELAY}s before"
                f" next batch",
            )
            time.sleep(__BATCH_DELAY)


def should_checkpoint(
    last_checkpoint_time: float, current_time: float
) -> bool:
    """
    Check if a periodic checkpoint should be performed.
    Args:
        last_checkpoint_time: Timestamp of the last checkpoint
        current_time: Current timestamp
    Returns:
        True if checkpoint is needed, False otherwise
    """
    if last_checkpoint_time is None:
        return False
    return (current_time - last_checkpoint_time) >= __CHECKPOINT_INTERVAL


def perform_periodic_checkpoint(
    endpoints_last_sync_time: dict,
    completed_endpoints: List[str],
    operation_name: str,
    progress_info: str = "",
):
    """
    Perform a periodic checkpoint during long-running operations.
    Args:
        endpoints_last_sync_time: Dictionary of endpoint sync times
        completed_endpoints: List of completed endpoints
        operation_name: Name of the current operation
        progress_info: Additional progress information
    """
    checkpoint_state = {
        "endpoints_last_sync_time": (endpoints_last_sync_time.copy()),
        "completed_endpoints": completed_endpoints,
        "last_periodic_checkpoint": time.time(),
        "current_operation": operation_name,
    }
    op.checkpoint(checkpoint_state)
    log_message(
        "info", f"Periodic checkpoint during {operation_name}: {progress_info}"
    )


def clean_order_data(order: dict) -> dict:
    """
    Clean order data to ensure all values are supported
    by Fivetran Connector SDK.  Converts list values to
    JSON strings and handles other unsupported types.
    Args:
        order: Raw order data dictionary
    Returns:
        Cleaned order data with supported data types
    """
    cleaned_order = {}

    for key, value in order.items():
        if isinstance(value, list):
            # Convert lists to JSON strings
            cleaned_order[key] = json.dumps(value)
        elif isinstance(value, dict):
            # Convert nested dictionaries to JSON strings
            cleaned_order[key] = json.dumps(value)
        elif value is None:
            # Keep None values as is
            cleaned_order[key] = value
        else:
            # Keep other supported types (str, int, float, bool) as is
            cleaned_order[key] = value

    return cleaned_order


def should_sync_endpoint(
    endpoint_name: str,
    last_sync_time: float,
    hours_between_syncs: float,
    current_time: float,
) -> bool:
    """
    Check if an endpoint should sync based on the time elapsed since last sync.
    Args:
        endpoint_name: Name of the endpoint
        last_sync_time: Last sync timestamp for the endpoint
        hours_between_syncs: Hours to wait between syncs
        current_time: Current timestamp
    Returns:
        True if endpoint should sync, False otherwise
    """
    if last_sync_time is None:
        log_message(
            "info",
            f"Endpoint {endpoint_name} has never" f" been synced, will sync",
        )
        return True

    # Convert seconds to hours
    time_elapsed_hours = (current_time - last_sync_time) / 3600.0
    should_sync = time_elapsed_hours >= hours_between_syncs

    if should_sync:
        message = (
            f"Endpoint {endpoint_name} should sync - "
            f"{time_elapsed_hours:.2f} hours elapsed "
            f"(threshold: {hours_between_syncs} hours)"
        )
        log_message(
            "info",
            message,
        )
    else:
        error = (
            f"Endpoint {endpoint_name} skipping sync - "
            f"{time_elapsed_hours:.2f} hours elapsed (threshold: "
            f"{hours_between_syncs} hours)"
        )
        log_message(
            "info",
            error,
        )

    return should_sync


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure
    it contains all required parameters.
    This function is called at the start of the update
    method to ensure that the connector has all necessary
    configuration values.
    Args:
        configuration: a dictionary that holds the
        configuration settings for the connector.
    Raises:
        ValueError: if any required configuration
        parameter is missing.
    """
    # Validate required configuration parameters
    required_configs = ["api_key", "api_secret"]
    for key in required_configs:
        if key not in configuration:
            raise ValueError(f"Missing required configuration value: {key}")

    # Set default value for hours_between_syncs if not present
    if "hours_between_syncs" not in configuration:
        configuration["hours_between_syncs"] = "12.0"
        log_message(
            "info",
            "hours_between_syncs not found in configuration,"
            " defaulting to 12.0 hours",
        )


def create_signature_old(
    api_key: str,
    api_secret: str,
    timestamp: str,
    method: str,
    path: str,
    body: str,
) -> str:
    """
    Create HMAC SHA256 signature for public endpoints (old format).
    Args:
        api_key: The API key
        api_secret: The API secret key
        timestamp: The timestamp for the request (nonce)
        method: The HTTP method (GET, POST, etc.)
        path: The API endpoint path
        body: The request body (empty string for GET requests)
    Returns:
        The signature string
    """
    # For public endpoints, use the old format
    message = api_key + timestamp + method + path + body
    return hmac.new(
        api_secret.encode("utf-8"), message.encode("utf-8"), hashlib.sha256
    ).hexdigest()


def create_signature(api_key: str, api_secret: str, request_data: dict) -> str:
    """
    Create HMAC SHA256 signature for authenticated
    requests according to Crypto.com API v1 spec.
    Args:
        api_key: The API key
        api_secret: The API secret key
        request_data: The request data dictionary
        containing id, method,
            params, nonce
    Returns:
        The signature string
    """
    # According to Crypto.com API v1 documentation,
    # the signature is created using:
    # HMAC-SHA256(api_secret, method +
    # id + api_key + paramsString + nonce)

    MAX_LEVEL = 3

    def params_to_str(obj, level):
        if level >= MAX_LEVEL:
            return str(obj)

        return_str = ""
        for key in sorted(obj):
            return_str += key
            if obj[key] is None:
                return_str += "null"
            elif isinstance(obj[key], list):
                for subObj in obj[key]:
                    return_str += params_to_str(subObj, level + 1)
            else:
                return_str += str(obj[key])
        return return_str

    # Extract values from request_data
    method = request_data.get("method", "")
    request_id = str(request_data.get("id", ""))
    params = request_data.get("params", {})
    nonce = str(request_data.get("nonce", ""))

    # Convert params to sorted string
    param_str = params_to_str(params, 0)

    # Create signature payload: method + id + api_key + paramsString + nonce
    payload_str = method + request_id + api_key + param_str + nonce

    return hmac.new(
        bytes(str(api_secret), "utf-8"),
        msg=bytes(payload_str, "utf-8"),
        digestmod=hashlib.sha256,
    ).hexdigest()


def make_authenticated_request(
    method: str,
    endpoint: str,
    configuration: dict,
    params: Optional[Dict] = None,
    data: Optional[Dict] = None,
) -> Dict:
    """
    Make an authenticated request to the
    Crypto.com API with retry logic.
    Args:
        method: HTTP method (GET, POST, etc.)
        endpoint: API endpoint path
        configuration: Configuration dictionary
        containing API credentials
        params: Query parameters
        data: Request body data
    Returns:
        Response data as dictionary
    """
    url = f"{__CRYPTO_COM_BASE_URL}{endpoint}"
    # Generate timestamp once and use it consistently
    current_timestamp = int(time.time() * 1000)
    timestamp = str(current_timestamp)

    log_message("info", f"Making {method} request to: {url}")
    log_message("info", f"Request timestamp: {timestamp}")

    # Prepare request body
    body = ""
    request_data = None

    if endpoint.startswith("/private/"):
        # Private endpoints require specific JSON structure
        request_data = {
            # Use same timestamp as ID
            "id": current_timestamp,
            # Remove leading slash from endpoint
            "method": endpoint.lstrip("/"),
            # Include api_key in request body
            "api_key": configuration["api_key"],
            "params": data if data else {},
            # Use same timestamp as nonce
            "nonce": current_timestamp,
        }

        # Create signature for private endpoints
        signature = create_signature(
            configuration["api_key"], configuration["api_secret"], request_data
        )

        # Add signature to request body
        request_data["sig"] = signature

        body = json.dumps(request_data)
        log_message("debug", f"Private endpoint request body: {body}")
    elif data:
        body = json.dumps(data)
        log_message("debug", f"Request body: {body}")

    # Create signature according to Crypto.com API v1 spec
    if endpoint.startswith("/private/"):
        # For private endpoints, signature was
        # already created and added to request body
        signature = request_data["sig"]
    else:
        # For public endpoints, use the old format
        signature = create_signature_old(
            configuration["api_key"],
            configuration["api_secret"],
            timestamp,
            method,
            endpoint,
            body,
        )

    # Prepare headers according to Crypto.com API v1 documentation
    headers = {
        "Content-Type": "application/json",
        "X-CRYPTO-APIKEY": configuration["api_key"],
        "X-CRYPTO-SIGNATURE": signature,
        "X-CRYPTO-TIMESTAMP": timestamp,
    }

    log_message("info", f"Request headers: {list(headers.keys())}")
    log_message(
        "info",
        f"Content-Type header: {headers.get('Content-Type', 'NOT SET')}",
    )

    # For private endpoints, ensure we're using the correct method and headers
    if endpoint.startswith("/private/"):
        log_message("info", f"Private endpoint detected: {endpoint}")
        log_message("debug", f"Request method: {method}")
        log_message("debug", f"Request params: {params}")
        log_message("debug", f"Request data: {data}")

    # Apply rate limiting before making the request
    apply_rate_limit()

    # Make request with retry logic using exponential backoff
    for attempt in range(__MAX_RETRIES):
        try:
            message = (
                f"Attempt {attempt + 1}/{__MAX_RETRIES} for"
                f" {method} {endpoint}"
            )
            log_message(
                "info",
                message,
            )

            if endpoint.startswith("/private/"):
                # Private endpoints always use POST with JSON body
                response = requests.post(
                    url, headers=headers, data=body, timeout=30
                )
            elif method.upper() == "GET":
                response = requests.get(
                    url, headers=headers, params=params, timeout=30
                )
            else:
                response = requests.post(
                    url, headers=headers, json=data, timeout=30
                )

            log_message("debug", f"Response status: {response.status_code}")
            log_message("debug", f"Response headers: {dict(response.headers)}")

            # Log full response if status is not 200
            if response.status_code != 200:
                try:
                    message = (
                        f"Non-200 response received. Full response:"
                        f" {response.text}"
                    )
                    log_message(
                        "severe",
                        message,
                    )
                except Exception as e:
                    error = (
                        f"Non-200 response received"
                        f" but could not read"
                        f" response text: {str(e)}"
                    )
                    log_message(
                        "severe",
                        error,
                    )

            response.raise_for_status()

            response_data = response.json()
            if isinstance(response_data, dict):
                message = (
                    f"Response data keys:" f" {list(response_data.keys())}"
                )
            else:
                message = "Response data keys: Not a dict"
            log_message(
                "debug",
                message,
            )
            message = (
                f"Complete response JSON:"
                f" {json.dumps(response_data, indent=2)}"
            )
            log_message(
                "debug",
                message,
            )

            return response_data

        except requests.exceptions.RequestException as e:
            if attempt == __MAX_RETRIES - 1:
                log_message(
                    "severe",
                    f"Failed to make API request after {__MAX_RETRIES}"
                    f" attempts: {str(e)}",
                )
                log_message("severe", f"Final URL: {url}")
                log_message("severe", f"Final headers: {headers}")
                raise e
            delay = __BASE_DELAY * (2**attempt)
            status_code = (
                getattr(e.response, "status_code", "unknown")
                if hasattr(e, "response") and e.response
                else "unknown"
            )
            message = (
                f"Request failed with status {status_code}, retrying in"
                f" {delay} seconds (attempt {attempt + 1}/{__MAX_RETRIES})"
            )
            log_message(
                "warning",
                message,
            )
            log_message("warning", f"Error details: {str(e)}")
            time.sleep(delay)

    return {}


def fetch_instruments(configuration: dict) -> List[Dict]:
    """
    Fetch available trading instruments from Crypto.com API.
    Args:
        configuration: Configuration dictionary
    Returns:
        List of instrument data
    """
    try:
        log_message("info", "Starting instruments fetch...")
        response = make_authenticated_request(
            "GET", "/public/get-instruments", configuration
        )
        log_message(
            "debug", f"Instruments response received: {type(response)}"
        )

        if isinstance(response, dict):
            log_message(
                "debug",
                f"Full response JSON: {json.dumps(response, indent=2)}",
            )
            result = response.get("result", {})
            # Changed from "instruments" to "data"
            instruments = result.get("data", [])
            log_message(
                "info",
                "Extracted {len(instruments)} instruments from response",
            )
            if instruments:
                if json.dumps(instruments[0]):
                    message = (
                        f"Sample instrument: "
                        f"{json.dumps(instruments[0], indent=2)}"
                    )
                else:
                    message = "Sample instrument: No instruments"

                log_message(
                    "debug",
                    message,
                )
            return instruments
        else:
            log_message(
                "warning", f"Unexpected response type: {type(response)}"
            )
            log_message("warning", f"Response content: {response}")
            return []

    except Exception as e:
        log_message("severe", f"Error fetching instruments: {str(e)}")
        log_message("severe", f"Exception type: {type(e).__name__}")
        return []


def fetch_tickers(configuration: dict) -> List[Dict]:
    """
    Fetch current market tickers from Crypto.com API.
    Args:
        configuration: Configuration dictionary
    Returns:
        List of ticker data
    """
    try:
        log_message("info", "Starting tickers fetch...")
        response = make_authenticated_request(
            "GET", "/public/get-tickers", configuration
        )
        log_message("debug", f"Tickers response received: {type(response)}")

        if isinstance(response, dict):
            log_message(
                "debug",
                f"Full response JSON: {json.dumps(response, indent=2)}",
            )
            result = response.get("result", {})
            tickers = result.get("data", [])
            log_message(
                "info", f"Extracted {len(tickers)} tickers from response"
            )
            if tickers:
                sample_ticker = json.dumps(tickers[0], indent=2)
                log_message(
                    "debug",
                    f"Sample ticker: {sample_ticker}",
                )
            return tickers
        else:
            log_message(
                "warning", f"Unexpected response type: {type(response)}"
            )
            log_message("warning", f"Response content: {response}")
            return []

    except Exception as e:
        log_message("severe", f"Error fetching tickers: {str(e)}")
        log_message("severe", f"Exception type: {type(e).__name__}")
        return []


def fetch_trades(
    configuration: dict, instrument_name: Optional[str] = None
) -> List[Dict]:
    """
    Fetch recent trades from Crypto.com API.
    Args:
        configuration: Configuration dictionary
        instrument_name: Optional instrument name to filter trades
    Returns:
        List of trade data
    """
    try:
        message = (
            f"Starting trades fetch for instrument:"
            f" {instrument_name or 'all'}"
        )
        log_message(
            "info",
            message,
        )

        # Build endpoint with optional instrument parameter
        endpoint = "/public/get-trades"
        params = {}
        if instrument_name:
            params["instrument_name"] = instrument_name

        response = make_authenticated_request(
            "GET", endpoint, configuration, params=params
        )
        log_message("debug", f"Trades response received: {type(response)}")

        if isinstance(response, dict):
            log_message(
                "debug",
                f"Full response JSON: {json.dumps(response, indent=2)}",
            )
            result = response.get("result", {})
            trades = result.get("data", [])
            trade_len = len(trades)
            log_message("debug", f"Extracted {trade_len} trades from response")
            if trades:
                if json.dumps(trades[0]):
                    message = (
                        f"Sample trade:" f" {json.dumps(trades[0], indent=2)}"
                    )
                else:
                    message = "Sample trade: No trades"
                log_message(
                    "debug",
                    message,
                )
            return trades
        else:
            log_message(
                "warning", f"Unexpected response type: {type(response)}"
            )
            log_message("warning", f"Response content: {response}")
            return []

    except Exception as e:
        log_message("severe", f"Error fetching trades: {str(e)}")
        log_message("severe", f"Exception type: {type(e).__name__}")
        return []


def fetch_book(
    configuration: dict, instrument_name: str, depth: int = 50
) -> Dict:
    """
    Fetch order book data from Crypto.com API.
    Args:
        configuration: Configuration dictionary
        instrument_name: Name of the instrument
        depth: Order book depth (default 50)
    Returns:
        Order book data dictionary
    """
    try:
        log_message(
            "info",
            f"Starting book fetch for {instrument_name} with depth {depth}",
        )

        params = {"instrument_name": instrument_name, "depth": depth}

        response = make_authenticated_request(
            "GET", "/public/get-book", configuration, params=params
        )
        log_message("debug", f"Book response received: {type(response)}")

        if isinstance(response, dict):
            log_message(
                "debug",
                f"Full response JSON: {json.dumps(response, indent=2)}",
            )
            result = response.get("result", {})
            book_data = result.get("data", {})
            log_message("info", f"Extracted book data for {instrument_name}")
            return book_data
        else:
            log_message(
                "warning", f"Unexpected response type: {type(response)}"
            )
            log_message("warning", f"Response content: {response}")
            return {}

    except Exception as e:
        log_message(
            "severe", f"Error fetching book for {instrument_name}: {str(e)}"
        )
        log_message("severe", f"Exception type: {type(e).__name__}")
        return {}


def fetch_candlestick(
    configuration: dict,
    instrument_name: str,
    timeframe: str = "1m",
    count: int = 100,
) -> List[Dict]:
    """
    Fetch historical candlestick data from Crypto.com API.
    Args:
        configuration: Configuration dictionary
        instrument_name: Name of the instrument
        timeframe: Timeframe (1m, 5m, 15m, 30m, 1h, 4h, 1d)
        count: Number of candles to fetch (default 100)
    Returns:
        List of candlestick data
    """
    try:
        log_message(
            "info",
            f"Starting candlestick fetch for {instrument_name}"
            f" with timeframe {timeframe}",
        )

        params = {
            "instrument_name": instrument_name,
            "timeframe": timeframe,
            "count": count,
        }

        response = make_authenticated_request(
            "GET", "/public/get-candlestick", configuration, params=params
        )
        log_message(
            "debug", f"Candlestick response received: {type(response)}"
        )

        if isinstance(response, dict):
            log_message(
                "debug",
                f"Full response JSON: {json.dumps(response, indent=2)}",
            )
            result = response.get("result", {})
            candles = result.get("data", [])
            log_message(
                "info", f"Extracted {len(candles)} candles from response"
            )
            if candles:
                sample_candle = json.dumps(candles[0], indent=2)
                log_message(
                    "debug",
                    f"Sample candle: {sample_candle}",
                )
            return candles
        else:
            log_message(
                "warning", f"Unexpected response type: {type(response)}"
            )
            log_message("warning", f"Response content: {response}")
            return []

    except Exception as e:
        log_message(
            "severe",
            f"Error fetching candlestick for {instrument_name}: {str(e)}",
        )
        log_message("severe", f"Exception type: {type(e).__name__}")
        return []


def fetch_user_balance(configuration: dict) -> List[Dict]:
    """
    Fetch user balance information from Crypto.com API.
    Args:
        configuration: Configuration dictionary
    Returns:
        List of balance data
    """
    try:
        log_message("info", "Starting user balance fetch...")

        response = make_authenticated_request(
            "POST", "/private/user-balance", configuration, data={}
        )
        log_message(
            "debug", f"User balance response received: {type(response)}"
        )

        if isinstance(response, dict):
            log_message(
                "debug",
                f"Full response JSON: {json.dumps(response, indent=2)}",
            )

            # Check for Crypto.com API response format
            if "code" in response and "result" in response:
                # Crypto.com API format: {"code": 0, "result": {"data": [...]}}
                if response.get("code") == 0:
                    result = response.get("result", {})
                    data = result.get("data", [])

                    # Extract position_balances from each account
                    all_balances = []
                    for account in data:
                        position_balances = account.get(
                            "position_balances", []
                        )
                        for balance in position_balances:
                            # Add account-level data to each position balance
                            balance_record = balance.copy()
                            balance_record.update(
                                {
                                    "total_available_balance": account.get(
                                        "total_available_balance"
                                    ),
                                    "total_margin_balance": account.get(
                                        "total_margin_balance"
                                    ),
                                    "total_initial_margin": account.get(
                                        "total_initial_margin"
                                    ),
                                    "total_position_im": account.get(
                                        "total_position_im"
                                    ),
                                    "total_haircut": account.get(
                                        "total_haircut"
                                    ),
                                    "total_maintenance_margin": account.get(
                                        "total_maintenance_margin"
                                    ),
                                    "total_position_cost": account.get(
                                        "total_position_cost"
                                    ),
                                    "total_cash_balance": account.get(
                                        "total_cash_balance"
                                    ),
                                    "total_collateral_value": account.get(
                                        "total_collateral_value"
                                    ),
                                    "total_unrealized_pnl": account.get(
                                        "total_session_unrealized_pnl"
                                    ),
                                    "account_instrument_name": account.get(
                                        "instrument_name"
                                    ),
                                    "total_session_realized_pnl": account.get(
                                        "total_session_realized_pnl"
                                    ),
                                    "is_liquidating": account.get(
                                        "is_liquidating"
                                    ),
                                    "total_effective_leverage": account.get(
                                        "total_effective_leverage"
                                    ),
                                    "position_limit": account.get(
                                        "position_limit"
                                    ),
                                    "used_position_limit": account.get(
                                        "used_position_limit"
                                    ),
                                }
                            )
                            all_balances.append(balance_record)
                    message = (
                        f"Extracted {len(all_balances)} balance records"
                        " from Crypto.com API response"
                    )
                    log_message(
                        "info",
                        message,
                    )
                    if all_balances:
                        if json.dumps(all_balances[0]):
                            message = (
                                f"Sample balance:"
                                f" {json.dumps(all_balances[0], indent=2)}"
                            )
                        else:
                            message = "Sample balance: No balances"
                        log_message(
                            "debug",
                            message,
                        )
                    return all_balances
                else:
                    log_message(
                        "severe",
                        "API returned error code {response.get('code')}: {"
                        "response.get('msg', 'Unknown error')}",
                    )
                    return []
            else:
                log_message(
                    "warning", f"Unexpected response format: {response}"
                )
                return []
        else:
            log_message(
                "warning", f"Unexpected response type: {type(response)}"
            )
            log_message("warning", f"Response content: {response}")
            return []

    except Exception as e:
        log_message("severe", f"Error fetching user balance: {str(e)}")
        log_message("severe", f"Exception type: {type(e).__name__}")
        return []


def fetch_user_positions(configuration: dict) -> List[Dict]:
    """
    Fetch user positions from Crypto.com API.
    Args:
        configuration: Configuration dictionary
    Returns:
        List of position data
    """
    try:
        log_message("info", "Starting user positions fetch...")

        response = make_authenticated_request(
            "POST", "/private/get-positions", configuration, data={}
        )
        log_message(
            "info", f"User positions response received: {type(response)}"
        )

        if isinstance(response, dict):
            log_message(
                "debug",
                f"Full response JSON: {json.dumps(response, indent=2)}",
            )

            # Check for Crypto.com API response format
            if "code" in response and "result" in response:
                # Crypto.com API format: {"code": 0, "result": {"data": [...]}}
                if response.get("code") == 0:
                    result = response.get("result", {})
                    positions = result.get("data", [])
                    message = (
                        f"Extracted {len(positions)} position records"
                        " from Crypto.com API response"
                    )
                    log_message(
                        "info",
                        message,
                    )
                    if positions:
                        if json.dumps(positions[0]):
                            message = (
                                f"Sample position:"
                                f" {json.dumps(positions[0], indent=2)}"
                            )
                        else:
                            message = "Sample position: No positions"
                        log_message(
                            "debug",
                            message,
                        )
                    return positions
                else:
                    message = (
                        f"API returned error code {response.get('code')}"
                        f": {response.get('msg', 'Unknown error')}"
                    )
                    log_message(
                        "severe",
                        message,
                    )
                    return []
            else:
                log_message(
                    "warning", f"Unexpected response format: {response}"
                )
                return []
        else:
            log_message(
                "warning", f"Unexpected response type: {type(response)}"
            )
            log_message("warning", f"Response content: {response}")
            return []

    except Exception as e:
        log_message("severe", f"Error fetching user positions: {str(e)}")
        log_message("severe", f"Exception type: {type(e).__name__}")
        return []


def fetch_open_orders(
    configuration: dict, instrument_name: str = None
) -> List[Dict]:
    """
    Fetch open orders from Crypto.com API.
    Args:
        configuration: Configuration dictionary
        instrument_name: Optional trading instrument name. If None,
            fetches all open orders.
    Returns:
        List of open order data
    """
    try:
        if instrument_name:
            log_message(
                "info", f"Starting open orders fetch for {instrument_name}..."
            )
            params = {"instrument_name": instrument_name}
        else:
            log_message(
                "info", "Starting open orders fetch for all instruments..."
            )
            params = {}

        response = make_authenticated_request(
            "POST", "/private/get-open-orders", configuration, data=params
        )
        log_message(
            "debug", f"Open orders response received: {type(response)}"
        )

        if isinstance(response, dict):
            log_message(
                "debug",
                f"Full response JSON: {json.dumps(response, indent=2)}",
            )

            # Check for Crypto.com API response format
            if "code" in response and "result" in response:
                # Crypto.com API format: {"code": 0, "result": {"data": [...]}}
                if response.get("code") == 0:
                    result = response.get("result", {})
                    orders = result.get("data", [])
                    message = (
                        f"Extracted {len(orders)} open order records"
                        " from Crypto.com API response"
                    )
                    log_message(
                        "info",
                        message,
                    )
                    if orders:
                        if json.dumps(orders[0]):
                            message = (
                                f"Sample order:"
                                f" {json.dumps(orders[0], indent=2)}"
                            )
                        else:
                            message = "Sample order: No orders"
                        log_message(
                            "debug",
                            message,
                        )
                    return orders
                else:
                    message = (
                        f"API returned error code {response.get('code')}"
                        f": {response.get('msg', 'Unknown error')}"
                    )
                    log_message(
                        "severe",
                        message,
                    )
                    return []
            else:
                log_message(
                    "warning", f"Unexpected response format: {response}"
                )
                return []
        else:
            log_message(
                "warning", f"Unexpected response type: {type(response)}"
            )
            log_message("warning", f"Response content: {response}")
            return []

    except Exception as e:
        log_message("severe", f"Error fetching open orders: {str(e)}")
        log_message("severe", f"Exception type: {type(e).__name__}")
        return []


def fetch_order_history(
    configuration: dict,
    instrument_name: str = None,
    start_time: float = None,
    end_time: float = None,
    limit: int = 100,
) -> List[Dict]:
    """
    Fetch order history from Crypto.com API.
    Args:
        configuration: Configuration dictionary
        instrument_name: Optional trading instrument name. If None,
            fetches all order history.
        start_time: Start timestamp in seconds (optional, for incremental sync)
        end_time: End timestamp in seconds (optional, for incremental sync)
        limit: Maximum number of orders to fetch (default 100, max 100)
    Returns:
        List of order history data
    """
    try:
        if instrument_name:
            message = (
                f"Starting order history fetch for {instrument_name}"
                f" with limit {limit}..."
            )
            log_message(
                "info",
                message,
            )
        else:
            message = (
                f"Starting order history fetch for all instruments"
                f" with limit {limit}..."
            )
            log_message(
                "info",
                message,
            )

        # Handle incremental sync timestamps
        if start_time is not None:
            # Convert from seconds to nanoseconds for API
            start_time_ns = int(start_time * 1000000000)
            message = (
                f"Using incremental sync start time"
                f": {start_time} ({start_time_ns} ns)"
            )
            log_message(
                "info",
                message,
            )
        else:
            # Default to 30 days ago if no start time provided
            start_time_ns = int((time.time() - 60 * 24 * 3600) * 1000000000)
            message = (
                f"No start time provided, using default"
                f" 30 days ago: {start_time_ns} ns"
            )
            log_message(
                "info",
                message,
            )

        if end_time is not None:
            # Convert from seconds to nanoseconds for API
            end_time_ns = int(end_time * 1000000000)
            message = (
                f"Using incremental sync end time"
                f": {end_time} ({end_time_ns} ns)"
            )
            log_message(
                "info",
                message,
            )
        else:
            # Default to now if no end time provided
            end_time_ns = int(time.time() * 1000000000)
            log_message(
                "info",
                f"No end time provided, using current time: {end_time_ns} ns",
            )

        params = {
            "start_time": start_time_ns,
            "end_time": end_time_ns,
            # Cap at 100 as per API limit
            "limit": min(limit, 100),
        }

        # Only add instrument_name if specified
        if instrument_name:
            params["instrument_name"] = instrument_name

        response = make_authenticated_request(
            "POST", "/private/get-order-history", configuration, data=params
        )
        log_message(
            "info", f"Order history response received: {type(response)}"
        )

        if isinstance(response, dict):
            log_message(
                "debug",
                f"Full response JSON: {json.dumps(response, indent=2)}",
            )

            # Check for Crypto.com API response format
            if "code" in response and "result" in response:
                # Crypto.com API format: {"code": 0, "result": {"data": [...]}}
                if response.get("code") == 0:
                    result = response.get("result", {})
                    orders = result.get("data", [])
                    message = (
                        f"Extracted {len(orders)} order history"
                        " records from Crypto.com API response"
                    )
                    log_message(
                        "info",
                        message,
                    )
                    if orders:
                        if json.dumps(orders[0]):
                            message = (
                                f"Sample order:"
                                f" {json.dumps(orders[0], indent=2)}"
                            )
                        else:
                            message = "Sample order: No orders"
                        log_message(
                            "debug",
                            message,
                        )
                    return orders
                else:
                    message = (
                        f"API returned error code {response.get('code')}"
                        f": {response.get('msg', 'Unknown error')}"
                    )
                    log_message(
                        "severe",
                        message,
                    )
                    return []
            else:
                log_message(
                    "warning", f"Unexpected response format: {response}"
                )
                return []
        else:
            log_message(
                "warning", f"Unexpected response type: {type(response)}"
            )
            log_message("warning", f"Response content: {response}")
            return []

    except Exception as e:
        log_message("severe", f"Error fetching order history: {str(e)}")
        log_message("severe", f"Exception type: {type(e).__name__}")
        return []


def schema(configuration: dict):
    """
    Define the schema function which lets you configure
    the schema your connector delivers.
    See the technical reference documentation for
    more details on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical
        -reference#schema
    Args:
        configuration: a dictionary that holds the
        configuration settings for the connector.
    """
    return [
        {
            "table": "instrument",
            # Name of the table in the destination, required.
            # Primary key column(s) for the table, optional.
            "primary_key": ["symbol"],
        },
        {
            # Market ticker data
            "table": "ticker",
            # Primary key for ticker data
            "primary_key": ["instrument"],
        },
        {
            # Trade history data
            "table": "trade",
            # Primary key for trade data
            "primary_key": ["trade_id"],
        },
        {
            # Order book data
            "table": "book",
            "primary_key": [
                "instrument",
                "timestamp",
                # Composite primary key
            ],
        },
        {
            # Historical price data
            "table": "candlestick",
            # Composite primary key
            "primary_key": [
                "instrument",
                "timestamp",
                "timeframe",
            ],
        },
        {
            # User balance information
            "table": "user_balance",
            # Primary key for balance data
            "primary_key": ["instrument_name"],
        },
        {
            # User trading positions
            "table": "user_position",
            # Primary key for position data
            "primary_key": ["instrument_name"],
        },
        {
            # Open orders
            "table": "open_order",
            # Primary key for order data
            "primary_key": ["order_id"],
        },
        {
            # Order history
            "table": "order_history",
            # Primary key for order history data
            "primary_key": ["order_id"],
        },
    ]


def update(configuration: dict, state: dict):
    """
    Define the update function, which is a required function, and
    is called by Fivetran during each sync.
    See the technical reference documentation for
    more details on the update function
    https://fivetran.com/docs/connectors/connector-sdk/technical
        -reference#update
    Args:
        configuration: A dictionary containing connection details
        state: A dictionary containing state information from previous runs
        The state dictionary is empty for the first sync or
            for any full re-sync
    """
    # Set up file logging if enabled
    setup_file_logging(configuration)

    log_message("info", "Starting Crypto.com connector sync")
    log_message("info", f"Configuration keys: {list(configuration.keys())}")
    log_message("info", f"Previous state: {state}")

    # Validate the configuration to ensure it contains all required values
    validate_configuration(configuration=configuration)
    log_message("info", "Configuration validation passed")

    # Get the state variable for the sync, if needed
    endpoints_last_sync_time = state.get("endpoints_last_sync_time", {})
    sync_start_time = time.time()
    log_message(
        "info", f"Endpoints last sync times: {endpoints_last_sync_time}"
    )
    log_message("info", f"Sync start time: {sync_start_time}")

    # Parse hours_between_syncs from configuration
    hours_between_syncs = float(
        configuration.get("hours_between_syncs", "12.0")
    )
    log_message("info", f"Hours between syncs: {hours_between_syncs}")

    # Track completed endpoints from previous runs
    completed_endpoints = state.get("completed_endpoints", [])
    log_message(
        "info", f"Previously completed endpoints: {completed_endpoints}"
    )

    # Track operation counts for logging
    total_upserts = 0
    total_records = 0

    try:
        # Check if instruments endpoint should sync
        instruments_last_sync = endpoints_last_sync_time.get("instruments")
        if not should_sync_endpoint(
            "instruments",
            instruments_last_sync,
            hours_between_syncs,
            sync_start_time,
        ):
            log_message(
                "info",
                "Skipping instruments endpoint - not enough time elapsed",
            )
            instruments = []
        else:
            # Fetch instruments
            log_message("info", "Fetching instruments...")
            instruments_start_time = time.time()
            instruments = fetch_instruments(configuration)
            instruments_duration = time.time() - instruments_start_time
            message = (
                "Instruments fetch completed."
                f" Records: {len(instruments)}"
                f" in {instruments_duration:.2f}s"
            )
            log_message(
                "info",
                message,
            )

        if instruments:
            # 'upsert' inserts or updates data in the table
            # Each instrument record is upserted individually
            for instrument in instruments:
                # Call op.upsert with:
                # 1) table name to upsert into
                # 2) dict of data to upsert
                op.upsert(table="instrument", data=instrument)
                total_records += 1

            log_message(
                "info", f"Upserted {len(instruments)} instrument records"
            )
            total_upserts += len(instruments)

            # Checkpoint after instruments endpoint
            endpoints_last_sync_time["instruments"] = sync_start_time
            instruments_state = {
                "endpoints_last_sync_time": endpoints_last_sync_time.copy(),
                "completed_endpoints": ["instruments"],
            }
            op.checkpoint(instruments_state)
            message = (
                "Checkpointed after instruments endpoint with sync time"
                f" {sync_start_time}"
            )
            log_message(
                "info",
                message,
            )

        # Check if tickers endpoint should sync
        tickers_last_sync = endpoints_last_sync_time.get("tickers")
        if not should_sync_endpoint(
            "tickers", tickers_last_sync, hours_between_syncs, sync_start_time
        ):
            log_message(
                "info", "Skipping tickers endpoint - not enough time elapsed"
            )
            tickers = []
        else:
            # Fetch tickers
            log_message("info", "Fetching tickers...")
            tickers_start_time = time.time()
            tickers = fetch_tickers(configuration)
            tickers_duration = time.time() - tickers_start_time
            message = (
                "Tickers fetch completed."
                f" Records: {len(tickers)}"
                f" in {tickers_duration:.2f}s"
            )
            log_message(
                "info",
                message,
            )

        if tickers:
            for ticker in tickers:
                # Map single-letter fields to descriptive names
                processed_ticker = {
                    # Instrument identifier
                    "instrument": ticker.get("i"),
                    # High price
                    "high": ticker.get("h"),
                    # Low price
                    "low": ticker.get("l"),
                    # Ask price
                    "ask": ticker.get("a"),
                    # Volume
                    "volume": ticker.get("v"),
                    # Volume value
                    "volume_value": ticker.get("vv"),
                    # Change
                    "change": ticker.get("c"),
                    # Bid price
                    "bid": ticker.get("b"),
                    # Last price
                    "last_price": ticker.get("k"),
                    # Open interest
                    "open_interest": ticker.get("oi"),
                    # Timestamp
                    "timestamp": ticker.get("t"),
                }

                op.upsert(table="ticker", data=processed_ticker)
                total_records += 1

            log_message("info", "Upserted {len(tickers)} ticker records")
            total_upserts += len(tickers)

            # Checkpoint after tickers endpoint
            endpoints_last_sync_time["tickers"] = sync_start_time
            tickers_state = {
                "endpoints_last_sync_time": endpoints_last_sync_time.copy(),
                "completed_endpoints": ["instruments", "tickers"],
            }
            op.checkpoint(tickers_state)
            message = (
                "Checkpointed after tickers"
                f" endpoint with sync time {sync_start_time}"
            )
            log_message(
                "info",
                message,
            )

        # Check if trades endpoint should sync
        trades_last_sync = endpoints_last_sync_time.get("trades")
        if not should_sync_endpoint(
            "trades", trades_last_sync, hours_between_syncs, sync_start_time
        ):
            log_message(
                "info", "Skipping trades endpoint - not enough time elapsed"
            )
        elif instruments:
            log_message(
                "info",
                "Fetching trades for all instruments with rate limiting...",
            )
            # Get all instruments
            all_instruments = [inst["symbol"] for inst in instruments]

            trades_start_time = time.time()
            last_checkpoint_time = trades_start_time
            total_trades_processed = 0
            processed_instruments = 0

            for batch in process_in_batches(
                all_instruments, __BATCH_SIZE, "trades"
            ):
                batch_trades = []

                for instrument_name in batch:
                    log_message(
                        "info", f"Fetching trades for {instrument_name}"
                    )
                    trades = fetch_trades(configuration, instrument_name)
                    if trades:
                        batch_trades.extend(trades)

                    processed_instruments += 1
                    current_time = time.time()

                    # Check if periodic checkpoint is needed
                    if should_checkpoint(last_checkpoint_time, current_time):
                        message = (
                            "Processed"
                            f" {processed_instruments}/{len(all_instruments)}"
                            f" instruments, {total_trades_processed}"
                            " trades upserted"
                        )
                        perform_periodic_checkpoint(
                            endpoints_last_sync_time,
                            ["instruments", "tickers"],
                            "trades",
                            message,
                        )
                        last_checkpoint_time = current_time

                # Upsert all trades in this batch immediately
                if batch_trades:
                    for trade in batch_trades:
                        # Map single-letter fields to descriptive names
                        processed_trade = {
                            # Trade ID
                            "trade_id": trade.get("d"),
                            # Timestamp
                            "timestamp": trade.get("t"),
                            # Trade number
                            "trade_number": trade.get("tn"),
                            # Quantity
                            "quantity": trade.get("q"),
                            # Price
                            "price": trade.get("p"),
                            # Side
                            "side": trade.get("s"),
                            # Instrument
                            "instrument": trade.get("i"),
                            # Match ID
                            "match_id": trade.get("m"),
                        }

                        op.upsert(table="trade", data=processed_trade)
                        total_records += 1

                    total_trades_processed += len(batch_trades)
                    message = (
                        f"Upserted {len(batch_trades)} trades"
                        f" from batch, total: {total_trades_processed}"
                    )
                    log_message(
                        "info",
                        message,
                    )
                    total_upserts += len(batch_trades)

            trades_duration = time.time() - trades_start_time
            message = (
                "Trades fetch completed."
                f" Records: {total_trades_processed}"
                f" in {trades_duration:.2f}s"
            )
            log_message(
                "info",
                message,
            )

            # Final checkpoint after all trades batches are processed
            endpoints_last_sync_time["trades"] = sync_start_time
            trades_state = {
                "endpoints_last_sync_time": endpoints_last_sync_time.copy(),
                "completed_endpoints": ["instruments", "tickers", "trades"],
            }
            op.checkpoint(trades_state)
            message = (
                "Checkpointed after trades"
                f" endpoint with sync time {sync_start_time}"
            )
            log_message(
                "info",
                message,
            )

        # Check if order book endpoint should sync
        book_last_sync = endpoints_last_sync_time.get("book")
        if not should_sync_endpoint(
            "order book", book_last_sync, hours_between_syncs, sync_start_time
        ):
            log_message(
                "info",
                "Skipping order book endpoint - not enough time elapsed",
            )
        elif instruments:
            message = (
                "Fetching order book data for"
                " all instruments with rate limiting..."
            )
            log_message(
                "info",
                message,
            )
            # Get all instruments
            all_instruments = [inst["symbol"] for inst in instruments]

            book_start_time = time.time()
            last_checkpoint_time = book_start_time
            book_count = 0
            processed_instruments = 0

            for batch in process_in_batches(
                all_instruments, __BATCH_SIZE, "order book"
            ):
                for instrument_name in batch:
                    log_message(
                        "info", f"Fetching book data for {instrument_name}"
                    )

                    # Capture timestamp before making the request
                    request_timestamp = int(time.time() * 1000)
                    book_data = fetch_book(configuration, instrument_name)

                    if book_data and isinstance(book_data, dict):
                        # Map fields to descriptive names
                        processed_entry = {
                            "instrument": instrument_name,
                            # From request params
                            "timestamp": request_timestamp,
                            # Timestamp before request
                            # Order book depth
                            "depth": book_data.get("depth"),
                            # Bids as JSON string
                            "bids": json.dumps(book_data.get("bids", [])),
                            # Asks as JSON string
                            "asks": json.dumps(book_data.get("asks", [])),
                        }

                        op.upsert(table="book", data=processed_entry)
                        total_records += 1
                        book_count += 1
                    elif (
                        book_data
                        and isinstance(book_data, list)
                        and len(book_data) > 0
                    ):
                        # Handle case where book_data is a list
                        for book_entry in book_data:
                            if isinstance(book_entry, dict):
                                processed_entry = {
                                    "instrument": instrument_name,
                                    # From request params
                                    "timestamp": request_timestamp,
                                    # Timestamp before request
                                    # Order book depth
                                    "depth": book_entry.get("depth"),
                                    # Bids as JSON string
                                    "bids": json.dumps(
                                        book_entry.get("bids", [])
                                    ),
                                    # Asks as JSON string
                                    "asks": json.dumps(
                                        book_entry.get("asks", [])
                                    ),
                                }

                                op.upsert(table="book", data=processed_entry)
                                total_records += 1
                                book_count += 1

                    processed_instruments += 1
                    current_time = time.time()

                    # Check if periodic checkpoint is needed
                    if should_checkpoint(last_checkpoint_time, current_time):
                        message = (
                            "Processed"
                            f" {processed_instruments}/{len(all_instruments)}"
                            f"instruments, {book_count} book records upserted"
                        )
                        perform_periodic_checkpoint(
                            endpoints_last_sync_time,
                            ["instruments", "tickers", "trades"],
                            "order book",
                            message,
                        )
                        last_checkpoint_time = current_time

            book_duration = time.time() - book_start_time
            message = (
                f"Upserted {book_count} book records for"
                f" {len(all_instruments)} instruments in"
                f" {book_duration:.2f}s"
            )
            log_message(
                "info",
                message,
            )
            total_upserts += book_count

            # Checkpoint after order book endpoint
            endpoints_last_sync_time["book"] = sync_start_time
            book_state = {
                "endpoints_last_sync_time": endpoints_last_sync_time.copy(),
                "completed_endpoints": [
                    "instruments",
                    "tickers",
                    "trades",
                    "book",
                ],
            }
            op.checkpoint(book_state)
            message = (
                "Checkpointed after order book"
                f" endpoint with sync time {sync_start_time}"
            )
            log_message(
                "info",
                message,
            )

        # Check if candlestick endpoint should sync
        candlestick_last_sync = endpoints_last_sync_time.get("candlestick")
        if not should_sync_endpoint(
            "candlestick",
            candlestick_last_sync,
            hours_between_syncs,
            sync_start_time,
        ):
            log_message(
                "info",
                "Skipping candlestick endpoint - not enough time elapsed",
            )
        elif instruments:
            message = (
                "Fetching candlestick data for"
                " all instruments with rate limiting..."
            )
            log_message(
                "info",
                message,
            )
            # Get all instruments
            all_instruments = [inst["symbol"] for inst in instruments]
            # Different timeframes
            timeframes = ["1m", "5m", "1h"]

            candlestick_start_time = time.time()
            last_checkpoint_time = candlestick_start_time
            candlestick_count = 0
            processed_instruments = 0

            for batch in process_in_batches(
                all_instruments, __BATCH_SIZE, "candlestick"
            ):
                for instrument_name in batch:
                    for timeframe in timeframes:
                        message = (
                            f"Fetching {timeframe} candlestick data for"
                            f" {instrument_name}"
                        )
                        log_message(
                            "debug",
                            message,
                        )
                        candles = fetch_candlestick(
                            configuration, instrument_name, timeframe, count=50
                        )
                        if candles:
                            for candle in candles:
                                # Map single-letter fields to descriptive names
                                processed_candle = {
                                    "instrument": instrument_name,
                                    # From request params
                                    "timeframe": timeframe,
                                    # From request params
                                    # Timestamp from candle data
                                    "timestamp": candle.get("t"),
                                    # Open price
                                    "open": candle.get("o"),
                                    # High price
                                    "high": candle.get("h"),
                                    # Low price
                                    "low": candle.get("l"),
                                    # Close price
                                    "close": candle.get("c"),
                                    # Volume
                                    "volume": candle.get("v"),
                                }

                                op.upsert(
                                    table="candlestick", data=processed_candle
                                )
                                total_records += 1
                                candlestick_count += 1

                    processed_instruments += 1
                    current_time = time.time()

                    # Check if periodic checkpoint is needed
                    if should_checkpoint(last_checkpoint_time, current_time):
                        message = (
                            "Processed"
                            f" {processed_instruments}/{len(all_instruments)}"
                            f" instruments, {candlestick_count}"
                            "candlestick records upserted"
                        )
                        perform_periodic_checkpoint(
                            endpoints_last_sync_time,
                            ["instruments", "tickers", "trades", "book"],
                            "candlestick",
                            message,
                        )
                        last_checkpoint_time = current_time

            candlestick_duration = time.time() - candlestick_start_time
            message = (
                f"Upserted {candlestick_count} candlestick records for"
                f" {len(all_instruments)} instruments across"
                f" {len(timeframes)} timeframes in"
                f" {candlestick_duration:.2f}s"
            )
            log_message(
                "info",
                message,
            )
            total_upserts += candlestick_count

            # Checkpoint after candlestick endpoint
            endpoints_last_sync_time["candlestick"] = sync_start_time
            candlestick_state = {
                "endpoints_last_sync_time": endpoints_last_sync_time.copy(),
                "completed_endpoints": [
                    "instruments",
                    "tickers",
                    "trades",
                    "book",
                    "candlestick",
                ],
            }
            op.checkpoint(candlestick_state)
            message = (
                "Checkpointed after candlestick"
                f" endpoint with sync time {sync_start_time}"
            )
            log_message(
                "info",
                message,
            )

        # ===== PRIVATE ENDPOINTS =====
        log_message("info", "Starting private endpoints sync (always sync)...")

        # Fetch user balance (always sync)
        log_message("info", "Fetching user balance...")
        balance_start_time = time.time()
        balances = fetch_user_balance(configuration)
        balance_duration = time.time() - balance_start_time
        message = (
            "User balance fetch completed."
            f" Records: {len(balances)}"
            f" in {balance_duration:.2f}s"
        )
        log_message(
            "info",
            message,
        )

        if balances:
            for balance in balances:
                # Clean the balance data to ensure
                # all values are supported types
                cleaned_balance = clean_order_data(balance)
                op.upsert(table="user_balance", data=cleaned_balance)
                total_records += 1

            log_message(
                "info", f"Upserted {len(balances)} user balance records"
            )
            total_upserts += len(balances)

            # Checkpoint after user balance endpoint
            endpoints_last_sync_time["user_balance"] = sync_start_time
            balance_state = {
                "endpoints_last_sync_time": endpoints_last_sync_time.copy(),
                "completed_endpoints": [
                    "instruments",
                    "tickers",
                    "trades",
                    "book",
                    "candlestick",
                    "user_balance",
                ],
            }
            op.checkpoint(balance_state)
            message = (
                "Checkpointed after user balance"
                f" endpoint with sync time {sync_start_time}"
            )
            log_message(
                "info",
                message,
            )

        # Fetch open orders for all instruments (always sync)
        log_message("info", "Fetching open orders for all instruments...")
        open_orders_start_time = time.time()

        # Use single API call to get all open
        # orders (more efficient than per-instrument calls)
        # No instrument_name = get all
        all_open_orders = fetch_open_orders(configuration)

        open_orders_duration = time.time() - open_orders_start_time

        message = (
            "Open orders fetch completed."
            f" Total records: {len(all_open_orders)}"
            f" in {open_orders_duration:.2f}s"
        )
        log_message(
            "info",
            message,
        )

        if all_open_orders:
            for order in all_open_orders:
                # Clean the order data to ensure all values are supported types
                cleaned_order = clean_order_data(order)
                op.upsert(table="open_order", data=cleaned_order)
                total_records += 1

            log_message(
                "info", f"Upserted {len(all_open_orders)} open order records"
            )
            total_upserts += len(all_open_orders)
        else:
            log_message("info", "No open orders found across all instruments")

        # Checkpoint after open orders endpoint
        endpoints_last_sync_time["open_orders"] = sync_start_time
        open_orders_state = {
            "endpoints_last_sync_time": endpoints_last_sync_time.copy(),
            "completed_endpoints": [
                "instruments",
                "tickers",
                "trades",
                "book",
                "candlestick",
                "user_balance",
                "open_orders",
            ],
        }
        op.checkpoint(open_orders_state)
        message = (
            "Checkpointed after open orders"
            f" endpoint with sync time {sync_start_time}"
        )
        log_message(
            "info",
            message,
        )

        # Fetch order history for all instruments (incremental sync)
        log_message("info", "Fetching order history for all instruments...")
        order_history_start_time = time.time()

        # Get last sync time for order history from state
        order_history_last_sync = state.get("order_history_last_sync")

        # Use single API call to get order history with incremental sync
        all_order_history = fetch_order_history(
            configuration,
            start_time=order_history_last_sync,
            end_time=sync_start_time,
            limit=100,
            # No instrument_name = get all
        )

        order_history_duration = time.time() - order_history_start_time
        message = (
            "Order history fetch completed."
            f" Total records: {len(all_order_history)}"
            f" in {order_history_duration:.2f}s"
        )
        log_message(
            "info",
            message,
        )

        if all_order_history:
            for order in all_order_history:
                # Clean the order data to ensure all values are supported types
                cleaned_order = clean_order_data(order)
                op.upsert(table="order_history", data=cleaned_order)
                total_records += 1

            log_message(
                "info",
                "Upserted {len(all_order_history)} order history records",
            )
            total_upserts += len(all_order_history)
        else:
            log_message(
                "info", "No order history found across all instruments"
            )

        # Checkpoint after order history endpoint
        endpoints_last_sync_time["order_history"] = sync_start_time
        order_history_state = {
            "endpoints_last_sync_time": endpoints_last_sync_time.copy(),
            "completed_endpoints": [
                "instruments",
                "tickers",
                "trades",
                "book",
                "candlestick",
                "user_balance",
                "open_orders",
                "order_history",
            ],
            "order_history_last_sync": sync_start_time,
            # Store for incremental sync
        }
        op.checkpoint(order_history_state)
        message = (
            "Checkpointed after order history"
            f" endpoint with sync time {sync_start_time}"
        )
        log_message(
            "info",
            message,
        )

        # Log summary of operations
        message = (
            f"Sync summary: {total_upserts} upsert operations,"
            f" {total_records} total records processed"
        )
        log_message(
            "info",
            message,
        )

        # Final checkpoint with all endpoints completed
        final_state = {
            "endpoints_last_sync_time": endpoints_last_sync_time.copy(),
            "completed_endpoints": [
                "instruments",
                "tickers",
                "trades",
                "book",
                "candlestick",
                "user_balance",
                "open_orders",
                "order_history",
            ],
            "order_history_last_sync": sync_start_time,
            # Store for incremental sync
        }
        log_message("info", f"Final state: {final_state}")

        # Save the progress by checkpointing the state. This
        # is important for ensuring that the sync process
        # can resume from the correct position in case of
        # next sync or interruptions.  Learn more about how
        # and where to checkpoint by reading our best
        # practices documentation
        # (https://fivetran.com/docs/connectors/
        # connector-sdk/best-practices#largedatasetrecommendation).
        op.checkpoint(final_state)
        log_message(
            "info",
            "Final state checkpointed successfully - all endpoints completed",
        )

        log_message("info", "Crypto.com connector completed successfully")

    except Exception as e:
        # In case of an exception, raise a runtime error
        log_message("severe", f"Crypto.com connector failed: {str(e)}")
        log_message("severe", f"Exception type: {type(e).__name__}")
        message = (
            "Total operations completed before"
            f" failure: {total_upserts} upserts,"
            f" {total_records} records"
        )
        log_message(
            "severe",
            message,
        )
        raise RuntimeError(f"Failed to sync data: {str(e)}")


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your
# script to be run directly from the command line or IDE
#  'run' button.  This is useful for debugging while you
# write your code. Note this method is not called by
# Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to
#  finalizing and deploying your connector.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents
    with open("configuration.json", "r") as f:
        configuration = json.load(f)

    # Test the connector locally
    connector.debug(configuration=configuration)
