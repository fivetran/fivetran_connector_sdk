# CoinGecko Connector Example

## Connector overview

The [CoinGecko](https://www.coingecko.com/en/api) custom connector for Fivetran fetches cryptocurrency market, asset, and historical data from the **CoinGecko API** and syncs it to your destination.

This connector pulls data from multiple CoinGecko endpoints, including:
- `/coins/list` (coins list)
- `/coins/markets` (market data)
- `/coins/{id}` (detailed coin information)
- `/coins/{id}/market_chart` (historical pricing and volume data)

It uses built-in rate limiting, error handling, and incremental syncs to ensure efficient and reliable data ingestion — ideal for financial analytics, portfolio tracking, and market monitoring.

---

## Requirements

- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
    - Windows: 10 or later (64-bit only)
    - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
    - Linux: Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)
- A CoinGecko API key (optional for free tier, required for Pro API)
- Internet access to `api.coingecko.com` or `pro-api.coingecko.com`

---

## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to set up and test the connector in your environment.  
You can run it locally with `fivetran debug --configuration configuration.json` before deployment.

---

## Features

The connector supports the following features:

- **Multiple data types**: Syncs coin lists, market data, detailed information, and historical charts.
- **Incremental syncs**: Tracks data changes using timestamps for efficient updates.
- **Rate limiting**: Automatically enforces CoinGecko API rate limits based on the free or pro plan.
- **Automatic batching**: Splits large requests into chunks of 250 coins (CoinGecko API limit).
- **Configurable historical syncs**: Optionally fetches historical prices for a specified number of days.
- **State management**: Uses checkpointing to track the last sync time for each table.
- **Error handling**: Retries on transient network or rate-limit errors.
- **Comprehensive logging**: Logs every major sync step and API request for transparency.

---

## Configuration file

The connector connects to the CoinGecko API and requires a configuration file in JSON format:

```json
{
  "api_key": "<YOUR_API_KEY>",
  "use_pro_api": "<YOUR_USE_PRO_API_OPTION>",
  "currency": "<YOUR_TARGET_CURRENCY>",
  "coin_ids": "<YOUR_COIN_IDS>",
  "sync_historical_data": "<YOUR_SYNC_HISTORICAL_DATA_OPTION>",
  "historical_days": "<YOUR_SYNC_HISTORICAL_DATA_DURATION>"
}
```

### Configuration parameters

| Key | Required | Description |
|-----|-----------|-------------|
| `api_key` | No | Your CoinGecko API key. Required for Pro API users. |
| `use_pro_api` | No | Set to `true` for the CoinGecko Pro API endpoint. |
| `currency` | Yes | Target fiat currency (e.g., `usd`, `eur`, `gbp`). |
| `coin_ids` | No | Comma-separated list of coin IDs to sync (e.g., `bitcoin,ethereum`). If not provided, top 250 coins are synced. |
| `sync_historical_data` | No | Set to `true` to sync daily historical data. |
| `historical_days` | No | Number of days of historical data to fetch (default: 30). |

Note: Keep your `configuration.json` file private and never commit it to version control.

---

## Authentication

The connector supports both **Public** and **Pro** API access modes.

- **Public API**: No authentication required, but limited to 50 requests per minute.
- **Pro API**: Uses an API key in the header `x-cg-pro-api-key` with a higher rate limit (up to 500 requests per minute).

The authentication logic is managed by the `CoinGeckoClient` class.

Example request header (Pro API):
```
x-cg-pro-api-key: <YOUR_API_KEY>
```

---

## Pagination

Pagination is implemented through **API batching**.  
CoinGecko limits each `/coins/markets` request to 250 records.

The connector handles this automatically:
- Iterates through coin batches of 250 using Python slicing.
- Waits between requests to comply with API rate limits.
- Continues until all coins are processed.

Refer to the `sync_coins_markets()` function for pagination implementation.

---

## Data handling

The connector processes CoinGecko data in the following way:

1. **Coins List** (`/coins/list`) – Retrieves all coins with metadata and platforms.
2. **Market Data** (`/coins/markets`) – Fetches current prices, volumes, and rankings.
3. **Coin Details** (`/coins/{id}`) – Retrieves coin-specific metadata, descriptions, and community/developer metrics.
4. **Historical Data** (`/coins/{id}/market_chart`) – Pulls daily market prices and volume for up to the configured number of days.

Each dataset is processed into separate destination tables:
- Records are emitted using `op.upsert()` for idempotent updates.
- Historical datasets are checkpointed to prevent duplicate pulls.
- Checkpoints are persisted using `op.checkpoint(state)`.

---

## Error handling

The connector includes extensive error handling for network and API-level issues:

- **HTTP 429 (Rate Limited)** – Retries automatically after waiting 60 seconds.
- **Connection/Timeout Errors** – Logs and retries with exponential backoff.
- **Partial Failures** – Logs individual record errors without halting the full sync.
- **Invalid JSON Responses** – Logged and skipped safely.
- **API Client Errors** – Handled in the `CoinGeckoClient.make_request()` method with retries.

Each failure type is logged at the appropriate level (`info`, `warning`, or `severe`) using Fivetran’s logging framework.

---

## Tables created

The connector creates the following tables in your destination:

| Table name | Primary key | Description |
|-------------|--------------|-------------|
| `coins_list` | `id` | List of all supported coins and their metadata. |
| `coins_markets` | `id` | Market information for each coin, including price, volume, and rank. |
| `coin_details` | `id` | Extended coin details including categories, links, and scores. |
| `coin_market_chart` | `coin_id`, `timestamp` | Historical daily data for coin prices, market caps, and volumes. |

Each table includes a `_fivetran_synced` column for sync timestamps.

---

## Additional considerations

- The CoinGecko free API enforces strict rate limits; consider upgrading to **CoinGecko Pro** for faster syncs.
- Large historical syncs (over 90 days) may take several minutes to complete.
- Use smaller `historical_days` or fewer `coin_ids` to optimize performance.
- The connector is designed for **educational and demonstration purposes** using Fivetran’s Connector SDK.
- Production usage should include robust retry policies, logging persistence, and exception tracking.

For support or advanced usage, contact the Fivetran Support team.
