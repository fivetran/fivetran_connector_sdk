# Step-size Incremental Sync Strategy

## Overview

This example demonstrates **step-size incremental sync** using the Fivetran Connector SDK. This strategy uses ID ranges to fetch records in batches when pagination or count is not supported, saving the current ID as state for the next sync.

## How Step-size Sync Works

- **ID-based Ranges**: Fetches records in configurable ID ranges (e.g., IDs 1-1000, then 1001-2000)
- **Sequential Processing**: Processes records sequentially by ID
- **State Management**: Saves the current ID position in connector state
- **Batch Processing**: Configurable step size for optimal performance

## Key Benefits

- **Universal Compatibility**: Works with any API that supports ID-based filtering
- **Predictable**: Sequential ID processing ensures no records are missed
- **Configurable**: Adjustable step size for performance optimization
- **Reliable**: Works even when APIs don't support traditional pagination

## Configuration

Edit `configuration.json` to set your API endpoint and step parameters:

```json
{
  "base_url": "http://127.0.0.1:5001/incremental/step",
  "initial_id": 1,
  "step_size": 1000,
  "max_id": 100000
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
- `start_id` parameter to specify the beginning of the ID range
- `end_id` parameter to specify the end of the ID range
- Records with sequential or predictable IDs
- Ability to return records within a specified ID range

## State Management

The connector saves state as:
```json
{
  "current_id": 5001
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

## When to Use Step-size Sync

- APIs that don't support traditional pagination
- When you need to process records by ID ranges
- APIs that support ID-based filtering
- When record IDs are sequential or predictable
- Initial sync scenarios for large datasets

## Important Considerations

- **Not truly incremental**: This strategy processes all records from the beginning each time
- **ID Requirements**: Requires sequential or predictable record IDs
- **Performance**: May be slower for large datasets as it processes from the initial ID
- **Gaps**: May miss records if there are gaps in the ID sequence
- **Max ID**: Set an appropriate `max_id` to prevent infinite loops

## Configuration Parameters

- **initial_id**: Starting ID for the sync (default: 1)
- **step_size**: Number of IDs to process in each batch (default: 1000)
- **max_id**: Maximum ID to process (safety limit)

## Comparison with Other Strategies

- **vs Keyset Pagination**: Less efficient but works with more APIs
- **vs Offset Pagination**: Similar concept but uses ID ranges instead of offsets
- **vs Timestamp Sync**: Better for APIs that don't support timestamp filtering

## Notes

- This example uses dummy/mock data for educational purposes
- The connector automatically handles the initial sync (from initial_id) and subsequent syncs
- Checkpointing occurs after each batch to ensure progress is saved
- Consider using timestamp-based sync for truly incremental syncs when possible 