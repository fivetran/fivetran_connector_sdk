# Offset-based Pagination Incremental Sync Strategy

## Overview

This example demonstrates **offset-based pagination** for incremental syncs using the Fivetran Connector SDK. Offset pagination uses an offset and page size to fetch records in batches, saving the offset as state for the next sync.

## How Offset Pagination Works

- **Batch Processing**: Fetches records in configurable page sizes
- **Offset Tracking**: Saves the current offset position in connector state
- **Sequential Access**: Processes records sequentially from the beginning
- **State Management**: Updates offset after each batch to maintain sync progress

## Key Benefits

- **Simple**: Easy to implement and understand
- **Predictable**: Sequential processing ensures no records are missed
- **Configurable**: Adjustable page size for performance optimization
- **Reliable**: Works with any API that supports offset/limit parameters

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
- `offset` parameter to specify the starting position
- `limit` parameter to specify the number of records per page
- Consistent record ordering (usually by ID or creation date)

## State Management

The connector saves state as:
```json
{
  "offset": 250
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

- APIs that support offset/limit pagination
- When you need to process all records sequentially
- Simple APIs without cursor-based pagination
- When record ordering is consistent and predictable
- Initial sync scenarios where you need to process all historical data

## Important Considerations

- **Not truly incremental**: This strategy processes all records from the beginning each time
- **Performance**: May be slower for large datasets as it processes from offset 0
- **Data consistency**: Assumes records don't change order between syncs
- **Memory usage**: Processes records in batches to manage memory efficiently

## Notes

- This example uses dummy/mock data for educational purposes
- The connector automatically handles the initial sync (offset 0) and subsequent syncs
- Checkpointing occurs after each batch to ensure progress is saved
- Consider using keyset pagination for truly incremental syncs with large datasets 