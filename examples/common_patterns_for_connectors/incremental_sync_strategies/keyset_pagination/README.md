# Keyset Pagination Incremental Sync Strategy

## Overview

This example demonstrates **keyset pagination** for incremental syncs using the Fivetran Connector SDK. Keyset pagination uses a cursor (typically an `updatedAt` timestamp) to fetch new and updated records since the last sync.

## How Keyset Pagination Works

- **Cursor-based**: Uses a cursor value (like `updatedAt` timestamp) to track the last processed record
- **State Management**: Saves the last `updatedAt` value in the connector state
- **Incremental Fetching**: Only fetches records where `updatedAt` is greater than the saved cursor
- **Scroll Support**: Handles APIs that use scroll parameters for pagination

## Key Benefits

- **Efficient**: Only processes new/updated records
- **Reliable**: Handles large datasets without missing records
- **Scalable**: Works well with APIs that support cursor-based pagination
- **Stateful**: Maintains sync progress across runs

## Configuration

Edit `configuration.json` to set your API endpoint:

```json
{
  "base_url": "http://127.0.0.1:5001/pagination/keyset"
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
- Optional `scroll_param` for pagination continuation
- Records with an `updatedAt` field for cursor tracking

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

## When to Use Keyset Pagination

- APIs that support cursor-based pagination
- When you need to track record updates by timestamp
- Large datasets where you want to avoid reprocessing unchanged records
- APIs that provide scroll parameters for pagination continuation

## Notes

- This example uses dummy/mock data for educational purposes
- The connector automatically handles the initial sync (no cursor) and subsequent incremental syncs
- Checkpointing occurs after each batch to ensure progress is saved 