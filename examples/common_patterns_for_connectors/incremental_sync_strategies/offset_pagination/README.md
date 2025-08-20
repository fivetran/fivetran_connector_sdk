# Offset-based Pagination Incremental Sync Strategy

## Overview

This example demonstrates **offset-based pagination** for incremental syncs using the Fivetran Connector SDK. This strategy uses timestamp-based filtering with offset pagination to fetch records in batches, saving the latest timestamp as state for the next sync.

## How Offset Pagination Works

- **Timestamp Filtering**: Uses `updated_since` parameter to filter records by timestamp
- **Offset Tracking**: Uses offset from API response to track pagination progress
- **State Management**: Saves the latest processed timestamp in connector state
- **Batch Processing**: Processes records in configurable page sizes

## Key Benefits

- **Incremental**: Only processes records updated since the last sync
- **Efficient**: Uses timestamp filtering to avoid reprocessing unchanged records
- **Reliable**: Handles large datasets with proper pagination
- **Stateful**: Maintains sync progress across runs

## Configuration

Edit `configuration.json` to set your API endpoint and page size:

```json
{
  "base_url": "http://127.0.0.1:5001/pagination/offset",
  "page_size": 100
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
- `updated_since` parameter to filter records by timestamp
- `order_by` and `order_type` parameters for consistent ordering
- `limit` parameter to specify the number of records per page
- Response should include `offset`, `limit`, and `total` fields
- Records with an `updatedAt` field for timestamp tracking

## API Response Format

The API should return responses in this format:
```json
{
  "data": [
    {"id": "1", "name": "John Doe", "updatedAt": "2024-01-15T10:30:00Z", ...},
    {"id": "2", "name": "Jane Smith", "updatedAt": "2024-01-15T11:00:00Z", ...}
  ],
  "offset": 0,
  "limit": 100,
  "total": 500
}
```

## State Management

The connector saves state as:
```json
{
  "last_updated_at": "2024-01-15T10:30:00Z"
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

## When to Use Offset Pagination

- APIs that support both timestamp filtering and offset pagination
- When you need truly incremental syncs with pagination
- Large datasets where you want to avoid reprocessing unchanged records
- APIs that return offset information in responses

## Important Considerations

- **API Compatibility**: Requires specific API response format with offset/total fields
- **Timestamp Accuracy**: Requires accurate and consistent `updatedAt` timestamps
- **Ordering**: API must support consistent ordering by timestamp
- **Performance**: More efficient than simple offset pagination for incremental syncs

## Comparison with Other Strategies

- **vs Keyset Pagination**: Similar efficiency but different API requirements
- **vs Simple Offset Pagination**: More efficient due to timestamp filtering
- **vs Timestamp Sync**: Adds pagination support for large datasets

## Notes

- This example uses dummy/mock data for educational purposes
- The connector automatically handles the initial sync (no timestamp) and subsequent incremental syncs
- Checkpointing occurs after each batch to ensure progress is saved
- This strategy combines the benefits of timestamp filtering with offset pagination 