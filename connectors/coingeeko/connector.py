# This is an example for how to work with the fivetran_connector_sdk module.
# This example demonstrates how to build a connector for the CoinGecko API, including handling large datasets and API pagination.
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details

# Import required classes from fivetran_connector_sdk.
# For supporting Connector operations like Update() and Schema()
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

import requests  # Used to make HTTP requests to external APIs like CoinGecko.
import json      # Provides JSON serialization/deserialization (convert dict ↔ JSON string).
import time      # Gives access to time-related functions like sleep and timestamps.
from datetime import datetime, timezone  # Used for date/time handling and time zone awareness.
from typing import Dict, List, Any        # Provides type hints for dictionaries, lists, and generic objects.

def schema(configuration: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    tables = [
        {
            "table": "coins_list",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "symbol": "STRING",
                "name": "STRING",
                "platforms": "JSON",
                "_fivetran_synced": "UTC_DATETIME",
            },
        },
        {
            "table": "coins_markets",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "symbol": "STRING",
                "name": "STRING",
                "image": "STRING",
                "current_price": "DOUBLE",
                "market_cap": "LONG",
                "market_cap_rank": "FLOAT",
                "fully_diluted_valuation": "LONG",
                "total_volume": "LONG",
                "high_24h": "DOUBLE",
                "low_24h": "DOUBLE",
                "price_change_24h": "DOUBLE",
                "price_change_percentage_24h": "DOUBLE",
                "market_cap_change_24h": "DOUBLE",
                "market_cap_change_percentage_24h": "DOUBLE",
                "circulating_supply": "DOUBLE",
                "total_supply": "DOUBLE",
                "max_supply": "DOUBLE",
                "ath": "DOUBLE",
                "ath_change_percentage": "DOUBLE",
                "ath_date": "UTC_DATETIME",
                "atl": "DOUBLE",
                "atl_change_percentage": "DOUBLE",
                "atl_date": "UTC_DATETIME",
                "last_updated": "UTC_DATETIME",
                "_fivetran_synced": "UTC_DATETIME",
            },
        },
        {
            "table": "coin_details",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "symbol": "STRING",
                "name": "STRING",
                "description": "STRING",
                "categories": "JSON",
                "links": "JSON",
                "image": "JSON",
                "genesis_date": "STRING",
                "sentiment_votes_up_percentage": "DOUBLE",
                "sentiment_votes_down_percentage": "DOUBLE",
                "market_cap_rank": "FLOAT",
                "coingecko_rank": "FLOAT",
                "coingecko_score": "DOUBLE",
                "developer_score": "DOUBLE",
                "community_score": "DOUBLE",
                "liquidity_score": "DOUBLE",
                "public_interest_score": "DOUBLE",
                "last_updated": "UTC_DATETIME",
                "_fivetran_synced": "UTC_DATETIME",
            },
        },
    ]

    if configuration.get("sync_historical_data", False):
        tables.append(
            {
                "table": "coin_market_chart",
                "primary_key": ["coin_id", "timestamp"],
                "columns": {
                    "coin_id": "STRING",
                    "timestamp": "UTC_DATETIME",
                    "price": "DOUBLE",
                    "market_cap": "DOUBLE",
                    "total_volume": "DOUBLE",
                    "_fivetran_synced": "UTC_DATETIME",
                },
            }
        )

    return tables


def update(configuration: Dict[str, Any], state: Dict[str, Any]):
    """
     Define the update function, which is a required function, and is called by Fivetran during each sync.
    See the technical reference documentation for more details on the update function
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    Args:
        configuration: A dictionary containing connection details
        state: A dictionary containing state information from previous runs
        The state dictionary is empty for the first sync or for any full re-sync
    """

    api_key = configuration.get('api_key', '')
    use_pro_api = configuration.get('use_pro_api', False)
    currency = configuration.get('currency', 'usd')
    coin_ids_str = configuration.get('coin_ids', '')
    sync_historical = configuration.get('sync_historical_data', False)
    historical_days = int(configuration.get('historical_days', 30))

    # Handle string boolean values
    if isinstance(use_pro_api, str):
        use_pro_api = use_pro_api.lower() in ('true', '1', 'yes')

    if isinstance(sync_historical, str):
        sync_historical = sync_historical.lower() in ('true', '1', 'yes')

    if isinstance(historical_days, str):
        historical_days = int(historical_days)

    # Parse coin IDs
    coin_ids = [c.strip() for c in coin_ids_str.split(',') if c.strip()] if coin_ids_str else []

    try:
        # Initialize API client
        client = CoinGeckoClient(api_key, use_pro_api)

        # Sync coins list
        log.info("Syncing coins list")
        sync_coins_list(client, state)

        # Get coin IDs to sync
        if not coin_ids:
            log.info("No specific coins specified, fetching top 250 by market cap")
            coin_ids = get_top_coins(client, currency, limit=250)

        log.info(f"Syncing data for {len(coin_ids)} coins")

        # Sync market data for each coin
        log.info("Syncing coins markets data")
        sync_coins_markets(client, coin_ids, currency, state)

        # Sync detailed coin information
        log.info("Syncing coin details")
        sync_coin_details(client, coin_ids, state)

        # Sync historical data if enabled
        if sync_historical:
            log.info(f"Syncing historical data for last {historical_days} days")
            sync_historical_data(client, coin_ids, currency, historical_days, state)

        log.info("Sync completed successfully")

    except Exception as e:
        log.severe(f"Sync failed: {str(e)}")
        raise e


class CoinGeckoClient:
    """CoinGecko API client with rate limiting"""

    def __init__(self, api_key: str = '', use_pro: bool = False):
        self.api_key = api_key
        self.base_url = 'https://pro-api.coingecko.com/api/v3' if use_pro else 'https://api.coingecko.com/api/v3'
        self.rate_limit_delay = 1.2 if not use_pro else 0.1  # Free tier: 50 calls/min, Pro: 500 calls/min
        self.last_request_time = 0

    def rate_limit(self):
        """
        Enforce rate limiting based on last request time and configured delay.
        """
        elapsed = time.time() - self.last_request_time
        if elapsed < self.rate_limit_delay:
            time.sleep(self.rate_limit_delay - elapsed)
        self.last_request_time = time.time()

    def make_request(self, endpoint: str, params: Dict[str, Any] = None) -> Any:
        """
        Make an API request to the specified endpoint with parameters.
        Handles rate limiting and errors.
        Args: param endpoint: API endpoint to call
              param params: Query parameters for the request
        Returns: JSON response from the API
        """
        self.rate_limit()

        url = f"{self.base_url}/{endpoint}"
        headers = {}

        if self.api_key:
            headers['x-cg-pro-api-key'] = self.api_key

        try:
            log.info(f"Requesting: {url}")
            response = requests.get(url, params=params, headers=headers, timeout=30)
            response.raise_for_status()
            return response.json()

        except requests.exceptions.Timeout:
            log.warning(f"Timeout fetching {url}. Retrying in 10 seconds...")
            time.sleep(10)
            return self.make_request(endpoint, params)

        except requests.exceptions.ConnectionError as e:
            log.severe(f"Network connection error: {e}. Retrying in 20 seconds...")
            time.sleep(20)
            return self.make_request(endpoint, params)

        except requests.exceptions.HTTPError as e:
            status = e.response.status_code
            if status == 429:
                log.warning("Rate limit exceeded. Waiting 60 seconds before retrying.")
                time.sleep(60)
                return self.make_request(endpoint, params)
            elif 500 <= status < 600:
                log.warning(f"Server error {status}. Retrying in 15 seconds...")
                time.sleep(15)
                return self.make_request(endpoint, params)
            elif status == 404:
                log.warning(f"Endpoint not found: {url}")
                return {}
            else:
                log.severe(f"HTTP error {status}: {e.response.text}")
                raise

        except json.JSONDecodeError as e:
            log.warning(f"Invalid JSON response for {url}: {e}")
            return {}

        except Exception as e:
            log.severe(f"Unexpected error in make_request: {type(e).__name__}: {e}")
            raise e

    def get_coins_list(self) -> List[Dict]:
        """
        Get list of all coins
        """
        return self.make_request('coins/list', {'include_platform': 'true'})

    def get_coins_markets(self, currency: str, coin_ids: List[str], per_page: int = 250, page: int = 1) -> List[Dict]:
        """
        Get market data for coins
        Args: param currency: Currency to get market data in
              param coin_ids: List of coin IDs to fetch data for
              param per_page: Number of coins per page (max 250)
              param page: Page number to fetch
        Returns: List of market data for coins
        """
        params = {
            'vs_currency': currency,
            'page': page,
            'per_page': per_page,
            'sparkline': 'false'
        }

        if coin_ids:
            params['ids'] = ','.join(coin_ids[:per_page])  # Max 250 coins per request


        return self.make_request('coins/markets', params)

    def get_coin_details(self, coin_id: str) -> Dict:
        """
        Get detailed information for a coin
        Args: param coin_id: ID of the coin to fetch details for
        Returns: Detailed information about the coin
        """
        return self.make_request(f'coins/{coin_id}', {
            'localization': 'false',
            'tickers': 'false',
            'market_data': 'false',
            'community_data': 'true',
            'developer_data': 'true'
        })

    def get_coin_market_chart(self, coin_id: str, currency: str, days: int) -> Dict:
        """
        Get historical market data for a coin
        Args: param: coin_id: ID of the coin
              param: currency: Currency to get market data in
              param: days: Number of days of data to fetch
        Returns: Historical market data including prices, market caps, and total volumes
        """
        return self.make_request(f'coins/{coin_id}/market_chart', {
            'vs_currency': currency,
            'days': days,
            'interval': 'daily'
        })


def get_top_coins(client: CoinGeckoClient, currency: str, limit: int = 250) -> List[str]:
    """
    Get top coins by market cap
    Args: param client: CoinGecko API client
          param currency: Currency to get market data in
          param limit: Number of top coins to fetch
    Returns: List of top coin IDs
    """
    try:
        markets = client.get_coins_markets(currency, [], per_page=limit, page=1)
        return [coin['id'] for coin in markets]
    except Exception as e:
        log.warning(f"Failed to get top coins: {e}")
        return []


def sync_coins_list(client: CoinGeckoClient, state: Dict[str, Any]):
    """
    Sync the list of all coins
    Args: param client: CoinGecko API client
          param state: State dictionary
    """
    table_name = 'coins_list'

    try:
        coins = client.get_coins_list()
        log.info(f"Fetched {len(coins)} coins")

        sync_time = datetime.now(timezone.utc).isoformat()

        for coin in coins:
            row = {
                'id': coin['id'],
                'symbol': coin['symbol'],
                'name': coin['name'],
                'platforms': json.dumps(coin.get('platforms', {})),
                '_fivetran_synced': sync_time
            }

            # The 'upsert' operation is used to insert or update data in the destination table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into.
            # - The second argument is a dictionary containing the data to be upserted.
            op.upsert(table=table_name, data=row)

        # Checkpoint
        new_state = {
            table_name: {
                'last_sync': sync_time
            }
        }
        if state is None:
            state = {}
        if table_name not in state:
            state[table_name] = {}

        state[table_name]['last_sync'] = new_state[table_name]['last_sync']
        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        op.checkpoint(state=state)

    except Exception as e:
        log.severe(f"Failed to sync coins list: {e}")
        raise e


def sync_coins_markets(client: CoinGeckoClient, coin_ids: List[str], currency: str, state: Dict[str, Any]):
    """
    Sync market data for coins
    Args: param client: CoinGecko API client
          param coin_ids: List of coin IDs to fetch market data for
          param currency: Currency to get market data in
        param state: State dictionary
    """
    table_name = 'coins_markets'

    try:
        # Process in batches of 250 (API limit)
        batch_size = 250
        sync_time = datetime.now(timezone.utc).isoformat()

        for i in range(0, len(coin_ids), batch_size):
            batch = coin_ids[i:i + batch_size]
            log.info(f"Fetching market data for batch {i//batch_size + 1} ({len(batch)} coins)")

            markets = client.get_coins_markets(currency, batch)

            for coin in markets:
                row = {
                    'id': coin['id'],
                    'symbol': coin['symbol'],
                    'name': coin['name'],
                    'image': coin.get('image'),
                    'current_price': coin.get('current_price'),
                    'market_cap': coin.get('market_cap'),
                    'market_cap_rank': coin.get('market_cap_rank'),
                    'fully_diluted_valuation': coin.get('fully_diluted_valuation'),
                    'total_volume': coin.get('total_volume'),
                    'high_24h': coin.get('high_24h'),
                    'low_24h': coin.get('low_24h'),
                    'price_change_24h': coin.get('price_change_24h'),
                    'price_change_percentage_24h': coin.get('price_change_percentage_24h'),
                    'market_cap_change_24h': coin.get('market_cap_change_24h'),
                    'market_cap_change_percentage_24h': coin.get('market_cap_change_percentage_24h'),
                    'circulating_supply': coin.get('circulating_supply'),
                    'total_supply': coin.get('total_supply'),
                    'max_supply': coin.get('max_supply'),
                    'ath': coin.get('ath'),
                    'ath_change_percentage': coin.get('ath_change_percentage'),
                    'ath_date': coin.get('ath_date'),
                    'atl': coin.get('atl'),
                    'atl_change_percentage': coin.get('atl_change_percentage'),
                    'atl_date': coin.get('atl_date'),
                    'last_updated': coin.get('last_updated'),
                    '_fivetran_synced': sync_time
                }

                # The 'upsert' operation is used to insert or update data in the destination table.
                # The op.upsert method is called with two arguments:
                # - The first argument is the name of the table to upsert the data into.
                # - The second argument is a dictionary containing the data to be upserted.
                op.upsert(table=table_name, data=row)

        new_state = {
            table_name: {
                'last_sync': sync_time
            }
        }

        # Checkpoint
        if state is None:
            state = {}
        if table_name not in state:
            state[table_name] = {}

        state[table_name]['last_sync'] = new_state[table_name]['last_sync']

        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        op.checkpoint(state=state)

    except Exception as e:
        log.severe(f"Failed to sync coins markets: {e}")
        raise e


def sync_coin_details(client: CoinGeckoClient, coin_ids: List[str], state: Dict[str, Any]):
    """
    Sync detailed information for coins
    Args: param client: CoinGecko API client
          param coin_ids: List of coin IDs to fetch details for
          param state: State dictionary
    """
    table_name = 'coin_details'

    try:
        sync_time = datetime.now(timezone.utc).isoformat()

        for coin_id in coin_ids:
            try:
                log.info(f"Fetching details for {coin_id}")
                details = client.get_coin_details(coin_id)

                # Extract description (use English)
                description = ''
                if 'description' in details and isinstance(details['description'], dict):
                    description = details['description'].get('en', '')

                row = {
                    'id': details['id'],
                    'symbol': details.get('symbol'),
                    'name': details.get('name'),
                    'description': description,
                    'categories': json.dumps(details.get('categories', [])),
                    'links': json.dumps(details.get('links', {})),
                    'image': json.dumps(details.get('image', {})),
                    'genesis_date': details.get('genesis_date'),
                    'sentiment_votes_up_percentage': details.get('sentiment_votes_up_percentage'),
                    'sentiment_votes_down_percentage': details.get('sentiment_votes_down_percentage'),
                    'market_cap_rank': details.get('market_cap_rank'),
                    'coingecko_rank': details.get('coingecko_rank'),
                    'coingecko_score': details.get('coingecko_score'),
                    'developer_score': details.get('developer_score'),
                    'community_score': details.get('community_score'),
                    'liquidity_score': details.get('liquidity_score'),
                    'public_interest_score': details.get('public_interest_score'),
                    'last_updated': details.get('last_updated'),
                    '_fivetran_synced': sync_time
                }

                # The 'upsert' operation is used to insert or update data in the destination table.
                # The op.upsert method is called with two arguments:
                # - The first argument is the name of the table to upsert the data into.
                # - The second argument is a dictionary containing the data to be upserted.
                op.upsert(table=table_name, data=row)

            except requests.exceptions.Timeout:
                log.warning(f"Timeout fetching details for {coin_id}")
                continue
            except requests.exceptions.HTTPError as e:
                log.warning(f"HTTP error for {coin_id}: {e}")
                continue
            except Exception as e:
                log.warning(f"Unexpected error for {coin_id}: {e}")
                continue

        new_state = {
            table_name: {
                'last_sync': sync_time
            }
        }

        if state is None:
            state = {}
        if table_name not in state:
            state[table_name] = {}

        state[table_name]['last_sync'] = new_state[table_name]['last_sync']

        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        op.checkpoint(state=state)

    except Exception as e:
        log.severe(f"Failed to sync coin details: {e}")
        raise e


def sync_historical_data(client: CoinGeckoClient, coin_ids: List[str], currency: str,
                         days: int, state: Dict[str, Any]):
    """
    Sync historical market data for coins
    Args: param client: CoinGecko API client
          param coin_ids: List of coin IDs to fetch historical data for
          param currency: Currency to get market data in
          param days: Number of days of historical data to fetch
          param state: State dictionary
    """
    table_name = 'coin_market_chart'
    last_sync_date = state.get(table_name, {}).get('last_sync_date')

    try:
        sync_time = datetime.now(timezone.utc).isoformat()

        # Determine how many days to fetch
        if last_sync_date:
            try:
                last_sync = datetime.fromisoformat(last_sync_date)
                if last_sync.tzinfo is None:
                    last_sync = last_sync.replace(tzinfo=timezone.utc)

                now_utc = datetime.now(timezone.utc)
                days_since_last = (now_utc - last_sync).days + 1
                days_to_fetch = min(days_since_last, days)
            except (ValueError, TypeError) as parse_err:
                log.warning(f"Could not parse last_sync_date '{last_sync_date}': {parse_err}")
                days_to_fetch = days
        else:
            days_to_fetch = days

        log.info(f"Fetching {days_to_fetch} days of historical data")

        for coin_id in coin_ids:
            try:
                log.info(f"Fetching historical data for {coin_id}")
                chart = client.get_coin_market_chart(coin_id, currency, days_to_fetch)

                if 'prices' in chart:
                    for price_point in chart['prices']:
                        timestamp_ms = price_point[0]

                        try:
                            # Ensure numeric and handle both ms & sec inputs
                            ts_val = float(timestamp_ms)
                            # If the timestamp looks like milliseconds (larger than 10^12), convert
                            if ts_val > 1e12:
                                ts_val = ts_val / 1000.0
                                timestamp = datetime.fromtimestamp(ts_val, tz=timezone.utc).isoformat()
                        except (ValueError, TypeError, OverflowError) as ts_err:
                            log.warning(f"Invalid timestamp for {coin_id}: {timestamp_ms} ({ts_err})")
                            continue


                        # Find matching market cap and volume
                        market_cap = None
                        total_volume = None

                        if 'market_caps' in chart:
                            for mc in chart['market_caps']:
                                if mc[0] == timestamp_ms:
                                    market_cap = mc[1]
                                    break

                        if 'total_volumes' in chart:
                            for tv in chart['total_volumes']:
                                if tv[0] == timestamp_ms:
                                    total_volume = tv[1]
                                    break

                        row = {
                            'coin_id': coin_id,
                            'timestamp': timestamp,
                            'price': price_point[1],
                            'market_cap': market_cap,
                            'total_volume': total_volume,
                            '_fivetran_synced': sync_time
                        }

                        # The 'upsert' operation is used to insert or update data in the destination table.
                        # The op.upsert method is called with two arguments:
                        # - The first argument is the name of the table to upsert the data into.
                        # - The second argument is a dictionary containing the data to be upserted.
                        op.upsert(table=table_name, data=row)

            except Exception as e:
                log.warning(f"Failed to fetch historical data for {coin_id}: {e}")
                continue

        # Save checkpoint safely
        new_state = {
            table_name: {
                'last_sync': sync_time,
                'last_sync_date': datetime.now(timezone.utc).date().isoformat()
            }
        }

        if state is None:
            state = {}
        if table_name not in state:
            state[table_name] = {}

        state[table_name] = new_state[table_name]

        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        op.checkpoint(state=state)

    except Exception as e:
        log.severe(f"Failed to sync historical data: {e}")
        raise e


# Connector instance
connector = Connector(update=update, schema=schema)


# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents
    with open("configuration.json", "r") as f:
        configuration = json.load(f)

    # Test the connector locally
    connector.debug(configuration=configuration)


# Fivetran debug results:
# Operation       | Calls
# ----------------+------------
# Upserts         | 19336
# Updates         | 0
# Deletes         | 0
# Truncates       | 0
# SchemaChanges   | 4
# Checkpoints     | 4