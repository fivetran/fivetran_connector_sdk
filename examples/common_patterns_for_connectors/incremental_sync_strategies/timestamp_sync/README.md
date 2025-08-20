# Timestamp-based Incremental Sync Strategy

## Overview

This example demonstrates **timestamp-based incremental sync** using the Fivetran Connector SDK. This strategy uses a timestamp to fetch all records updated since the last sync, saving the latest timestamp as state for the next sync.

## How Timestamp-based Sync Works

- **Time-based Filtering**: Uses a timestamp to filter records updated since the last sync
- **State Management**: Saves the latest processed timestamp in connector state
- **Incremental Processing**: Only fetches records with `updatedAt` greater than the saved timestamp
- **Efficient Updates**: Processes only new and modified records

## Key Benefits

- **Truly Incremental**: Only processes records that have changed since the last sync
- **Efficient**: Minimizes API calls and data processing
- **Reliable**: Handles large datasets without performance degradation
- **Simple**: Easy to implement and maintain

## Configuration

Edit `configuration.json` to set your API endpoint:

```json
{
  "base_url": "http://127.0.0.1:5001/incremental/timestamp"
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

## When to Use Timestamp-based Sync

- APIs that support timestamp-based filtering
- When you need truly incremental syncs
- Large datasets where efficiency is important
- When records have reliable `updatedAt` timestamps
- APIs that can return all changes since a given timestamp

## Important Considerations

- **Timestamp Accuracy**: Requires accurate and consistent `updatedAt` timestamps
- **Time Zones**: Ensure consistent timezone handling
- **API Limitations**: Some APIs may have limits on how far back you can query
- **Data Consistency**: Assumes records are updated with current timestamps

## Comparison with Other Strategies

- **vs Keyset Pagination**: Similar concept, but timestamp sync is simpler for APIs that return all changes in one call
- **vs Offset Pagination**: Much more efficient for incremental syncs
- **vs Step-size Sync**: Better for APIs that support timestamp filtering

## Notes

- This example uses dummy/mock data for educational purposes
- The connector automatically handles the initial sync (no timestamp) and subsequent incremental syncs
- Checkpointing occurs after processing all records to ensure state is saved
- This strategy assumes the API returns all relevant records in a single call 