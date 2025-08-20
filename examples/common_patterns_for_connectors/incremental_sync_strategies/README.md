# Incremental Sync Strategies Examples

This directory contains individual examples demonstrating different incremental sync strategies using the Fivetran Connector SDK. Each strategy is implemented as a separate, focused example with its own configuration and documentation.

## Available Strategies

### 1. [Keyset Pagination](./keyset_pagination/)
**Best for**: APIs with cursor-based pagination support
- Uses a cursor (e.g., `updatedAt` timestamp) to fetch new/updated records
- Saves the last `updatedAt` value as state
- Handles scroll parameters for pagination continuation
- **Most efficient** for truly incremental syncs

### 2. [Offset Pagination](./offset_pagination/)
**Best for**: APIs with offset pagination and timestamp filtering support
- Uses timestamp filtering with offset pagination to fetch records in batches
- Saves the latest processed timestamp as state
- Combines incremental sync with pagination for large datasets
- **Efficient incremental sync** with pagination support

### 3. [Timestamp Sync](./timestamp_sync/)
**Best for**: APIs with timestamp-based filtering
- Uses a timestamp to fetch records updated since the last sync
- Saves the latest processed timestamp as state
- **Truly incremental** - only processes changed records
- **Most efficient** for APIs that support timestamp filtering

### 4. [Step-size Sync](./step_size_sync/)
**Best for**: APIs without traditional pagination support
- Uses ID ranges to fetch records in batches
- Saves the current ID position as state
- Works with any API that supports ID-based filtering
- **Universal compatibility** but not truly incremental

### 5. [Replay Sync](./replay_sync/)
**Best for**: Read-replica scenarios with replication lag
- Uses timestamp-based sync with a configurable buffer
- Goes back X hours from the last timestamp to handle delays
- **Most robust** for distributed systems
- **Data consistency** focused

## Quick Comparison

| Strategy | Incremental | Efficiency | Complexity | Use Case |
|----------|-------------|------------|------------|----------|
| Keyset Pagination | ✅ Yes | High | Medium | APIs with cursor support |
| Offset Pagination | ✅ Yes | High | Medium | APIs with offset + timestamp filtering |
| Timestamp Sync | ✅ Yes | High | Low | APIs with timestamp filtering |
| Step-size Sync | ❌ No | Medium | Low | APIs without pagination |
| Replay Sync | ✅ Yes | Medium | Medium | Read-replica systems |

## Requirements

* [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)   
* Operating system:
  * Windows: 10 or later (64-bit only)
  * macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  * Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting Started

1. **Choose your strategy** based on your API capabilities and requirements
2. **Navigate to the strategy folder** (e.g., `keyset_pagination/`)
3. **Install dependencies**:
   ```
   pip install fivetran-connector-sdk requests
   ```
4. **Configure your API endpoint** in `configuration.json`
5. **Run the connector** for local testing:
   ```
   python connector.py
   ```

## Common Schema

All strategies sync data to the same `user` table with this schema:

```json
{
   "table": "user",
   "primary_key": ["id"],
   "columns": {
      "id": "STRING",
      "name": "STRING",
      "email": "STRING",
      "address": "STRING",
      "company": "STRING",
      "job": "STRING",
      "updatedAt": "UTC_DATETIME",
      "createdAt": "UTC_DATETIME"
   }
}
```

## Choosing the Right Strategy

### For APIs with cursor-based pagination:
- **Use**: Keyset Pagination
- **Why**: Most efficient for incremental syncs

### For APIs with offset pagination and timestamp filtering:
- **Use**: Offset Pagination
- **Why**: Combines incremental sync with pagination for large datasets

### For APIs with timestamp filtering:
- **Use**: Timestamp Sync
- **Why**: Truly incremental and efficient

### For APIs without pagination support:
- **Use**: Step-size Sync
- **Why**: Works with ID-based filtering

### For read-replica systems:
- **Use**: Replay Sync
- **Why**: Handles replication lag

## Notes

- All examples use dummy/mock data for educational purposes
- Each strategy includes detailed documentation in its respective folder
- See the [Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) for getting started with the Fivetran Connector SDK
- For inquiries, please reach out to our Support team

## Additional Considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples.
