# Redis Connector Example

## Connector overview
This connector syncs gaming leaderboards, player statistics, and real-time engagement data from Redis to Fivetran for historical analytics. Designed for gaming platforms and competitive applications that use Redis with persistence (AOF/RDB) as their primary database for leaderboards, player profiles, and real-time game state. The connector retrieves all key-value pairs including sorted sets (leaderboards), hashes (player profiles), and counters, using Redis SCAN command for efficient, resumable synchronization with cursor-based pagination and state management.

The connector supports the following use cases:
- Gaming leaderboards: Track historical leaderboard positions, analyze player progression, identify top performers over time
- Player analytics: Aggregate player statistics, skill progression, engagement patterns, and retention metrics
- Competitive balance: Analyze game difficulty, reward distribution, and competitive fairness
- Real-time counters: Historical trends for views, likes, engagement metrics stored in Redis
- Session analytics: User journey analysis when Redis stores session data with persistence enabled

## Requirements
- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features
- Synchronizes gaming leaderboards (sorted sets), player profiles (hashes), and statistics from Redis
- Supports all Redis data types: strings (counters), hashes (player data), lists (match history), sets (achievements), and sorted sets (leaderboards)
- Configurable key pattern filtering for targeted data extraction (e.g., "leaderboard:*", "player:*:stats")
- Captures comprehensive metadata including TTL, data type, size, and timestamps for historical trend analysis
- Implements cursor-based pagination using Redis SCAN for memory-efficient processing
- Resumable syncs with state management and checkpointing every 1000 keys
- Designed for Redis with persistence enabled (AOF/RDB) as primary database
- Multi-database support for Redis database numbers 0-15
- SSL/TLS connection support for secure communication
- Password authentication for protected Redis instances

## When to use this connector
This connector is designed for scenarios where Redis is used as a persistent database (not cache):
- Gaming platforms storing leaderboards, player stats, achievements
- Real-time analytics platforms with Redis Streams or persistent counters
- Applications with Redis persistence enabled (AOF or RDB snapshots)
- Redis is the source of truth (data not duplicated in another database)

This connector is NOT suitable for:
- Redis used purely as cache layer (use source database connector instead)
- Ephemeral session stores with no persistence

## Configuration file
The configuration keys required for your connector are as follows:

```
{
  "username": "<YOUR_REDIS_USERNAME>",
  "password": "<YOUR_REDIS_PASSWORD>",
  "database": "<YOUR_REDIS_DATABASE_NUMBER>",
  "ssl": "<ENABLE_OR_DISABLE_SSL>",
  "table_name": "<YOUR_REDIS_TABLE_NAME>",
  "key_pattern": "<YOUR_REDIS_KEY_PATTERN>",
  "batch_size": "<YOUR_BATCH_SIZE>"
}
```

### Configuration parameters

- `host` (required): Redis server hostname or IP address
- `port` (required): Redis server port (typically 6379)
- `password` (optional): Redis authentication password (leave empty for no authentication)
- `database` (optional): Redis database number 0-15 (defaults to 0)
- `ssl` (optional): Enable SSL/TLS connection - "true" or "false" (defaults to "false")
- `table_name` (optional): Destination table name (defaults to "redis_data")
- `key_pattern` (optional): Pattern to match keys - supports wildcards (defaults to "*" for all keys)
- `batch_size` (optional): Number of keys to process per batch (defaults to 100)

Key pattern examples:
- `"*"`: All keys (for full database sync)
- `"leaderboard:*"`: All leaderboard keys (e.g., "leaderboard:global", "leaderboard:weekly")
- `"player:*:profile"`: All player profile hashes
- `"game:*:stats"`: All game statistics
- `"achievement:*"`: All achievement data

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file
The `requirements.txt` file specifies the Python libraries required by the connector:

```
redis
```

The connector uses the `redis` library for connecting to and querying Redis databases.

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication
The connector supports both local and cloud Redis deployments with flexible authentication:

Basic configuration (no password):
```
{
  "host": "<YOUR_REDIS_HOST>",
  "port": "<YOUR_REDIS_PORT>",
  "password": ""
}
```

Password-protected Redis:
```
{
  "host": "<YOUR_REDIS_HOST>",
  "port": "<YOUR_REDIS_PORT>",
  "password": "<YOUR_PASSWORD>",
  "username": "<YOUR_USERNAME>"
}
```

Note: Redis Cloud and other managed services typically require both username (usually "default") and password. The connector automatically handles SSL/TLS when connecting to cloud providers.

## Pagination
The connector implements Redis SCAN command for efficient, non-blocking key iteration. Refer to the `scan_redis_keys` function in `connector.py` which handles pagination with the following approach:

- Uses cursor-based iteration starting from cursor position 0
- Processes keys in configurable batches (default 100 keys per SCAN operation)
- Returns next cursor position for resuming iteration
- Cursor = 0 indicates scan completion
- Implements checkpointing every 1000 keys in the `update` function
- State tracking allows resumption from last cursor position after interruptions

The SCAN operation is memory-efficient, non-blocking, and allows other Redis operations to continue during data extraction.

## Data handling
The connector performs comprehensive data extraction and transformation. Refer to the `get_redis_value_info` function in `connector.py` for data type handling:

1. Key discovery: Uses SCAN command with optional pattern filtering to discover matching keys
2. Type detection: Identifies Redis data type for each key (string, hash, list, set, zset)
3. Value extraction: Retrieves values using type-specific Redis commands:
  - Strings: Direct value extraction with GET
  - Hashes: Extracted with HGETALL and converted to JSON objects
  - Lists: Retrieved with LRANGE and converted to JSON arrays
  - Sets: Extracted with SMEMBERS and converted to JSON arrays
  - Sorted sets: Retrieved with ZRANGE including scores, converted to JSON arrays of [member, score] pairs
4. Metadata collection: Gathers TTL (Time To Live), size, and timestamp for each key
5. Upsert operations: All records are upserted to handle both new and updated keys

The `update` function orchestrates the complete sync process with state management and error handling.

## Error handling
The connector implements comprehensive error handling strategies. Refer to the following functions in `connector.py`:

- Connection validation (`create_redis_client`): Tests Redis connectivity during client creation with ping operation, handles SSL configuration errors
- Configuration validation (`validate_configuration`): Ensures required parameters (host, port) are present
- Key access errors (`get_redis_value_info`): Handles expired or deleted keys gracefully during scanning, logs warnings for failed key access
- Network timeouts: Configured 30-second socket timeout for connection resilience
- Connection cleanup (`update` function finally block): Ensures Redis connections are properly closed even on errors

## Tables created
The connector creates a table in the destination based on your configuration (configurable via `table_name` parameter, defaults to `redis_data`):

| Column | Type | Description |
|--------|------|-------------|
| key | STRING | Redis key name (Primary Key) |
| value | STRING | Redis value - JSON string for complex types (hashes, lists, sets) |
| data_type | STRING | Redis data type: string, hash, list, set, or zset |
| ttl | INT | Time to live in seconds (-1 = no expiry, -2 = expired/non-existent) |
| last_modified | UTC_DATETIME | Timestamp when key was last processed by connector |
| size | INT | Size metric - length for strings, count for collections |

The table uses `key` as the primary key, enabling upserts for handling updated Redis values across syncs.

## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.