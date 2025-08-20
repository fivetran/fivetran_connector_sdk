# Replay Incremental Sync Strategy (with Buffer)

## Overview

This example demonstrates **replay incremental sync with buffer** using the Fivetran Connector SDK. This strategy uses timestamp-based sync with a configurable buffer (goes back X hours from the last timestamp) to handle read-replica scenarios with replication lag.

## How Replay Sync Works

- **Buffer-based Filtering**: Applies a time buffer to the last processed timestamp
- **Replication Lag Handling**: Accounts for delays in read-replica systems
- **State Management**: Saves the latest processed timestamp in connector state
- **Overlap Processing**: Re-processes some records to ensure no data is missed

## Key Benefits

- **Replication Lag Safe**: Handles delays in read-replica systems
- **Data Consistency**: Ensures no records are missed due to timing issues
- **Configurable Buffer**: Adjustable buffer time for different replication scenarios
- **Reliable**: Robust against timing-related data inconsistencies

## Configuration

Edit `configuration.json` to set your API endpoint and buffer time:

```json
{
  "base_url": "http://127.0.0.1:5001/incremental/replay",
  "buffer_hours": 2
}
```

## Usage

1. Install dependencies:
   ```
   pip install fivetran-connector-sdk requests
   ```

2. Run a mock API (see [fivetran-api-playground](https://pypi.org/project/fivetran-api-playground/)) or point to your own API.

3. Run the connector for local testing:
   ```
   python connector.py
   ```

## API Requirements

Your API should support:
- `since` parameter to filter records by timestamp
- Records with an `updatedAt` field for timestamp tracking
- Consistent timestamp format (ISO 8601 recommended)
- Ability to return all records updated since a given timestamp

## State Management

The connector saves state as:
```json
{
  "last_timestamp": "2024-01-15T10:30:00Z"
}
```

## Tables Created

The connector syncs data to the `user` table with the following schema:

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

## When to Use Replay Sync

- **Read-replica scenarios**: When reading from a replica with potential replication lag
- **High-availability systems**: Where data consistency is critical
- **Distributed databases**: With eventual consistency models
- **APIs with timing delays**: Where updates may not be immediately available
- **Critical data scenarios**: Where missing records is not acceptable

## Buffer Configuration

The `buffer_hours` parameter determines how far back to look from the last timestamp:

- **Small buffer (1-2 hours)**: For systems with minimal replication lag
- **Medium buffer (2-6 hours)**: For typical read-replica scenarios
- **Large buffer (6+ hours)**: For systems with significant replication delays

## Important Considerations

- **Overlap Processing**: This strategy may re-process some records due to the buffer
- **Performance Impact**: Larger buffers mean more data processing
- **Duplicate Handling**: The connector uses upsert to handle potential duplicates
- **Buffer Sizing**: Choose buffer size based on your replication lag characteristics

## Comparison with Other Strategies

- **vs Timestamp Sync**: Adds buffer to handle replication lag
- **vs Keyset Pagination**: More robust for read-replica scenarios
- **vs Offset Pagination**: Much more efficient for incremental syncs

## Example Buffer Calculation

If your last timestamp was `2024-01-15T10:30:00Z` and buffer_hours is 2:
- Buffer timestamp: `2024-01-15T08:30:00Z`
- The connector will fetch all records updated since 8:30 AM instead of 10:30 AM
- This ensures any records that were delayed in replication are captured

## Notes

- This example uses dummy/mock data for educational purposes
- The connector automatically handles the initial sync (no timestamp) and subsequent incremental syncs
- Checkpointing occurs after processing all records to ensure state is saved
- The buffer is only applied when there's a previous timestamp (not on initial sync) 