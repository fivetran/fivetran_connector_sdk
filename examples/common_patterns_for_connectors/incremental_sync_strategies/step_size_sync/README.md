# Step-size Incremental Sync Strategy Example

## Connector overview

This connector demonstrates **step-size incremental sync** using the Fivetran Connector SDK. This strategy uses ID ranges to fetch records in batches when pagination or count is not supported, saving the current ID as state for the next sync.

## Requirements

* [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)   
* Operating System:  
  * Windows 10 or later  
  * macOS 13 (Ventura) or later

## Getting Started

Refer to the [Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

- **ID-based Ranges**: Fetches records in configurable ID ranges (e.g., IDs 1-1000, then 1001-2000)
- **Sequential Processing**: Processes records sequentially by ID
- **State Management**: Saves the current ID position in connector state
- **Batch Processing**: Configurable step size for optimal performance
- **Universal Compatibility**: Works with any API that supports ID-based filtering
- **Predictable**: Sequential ID processing ensures no records are missed
- **Configurable**: Adjustable step size for performance optimization
- **Reliable**: Works even when APIs don't support traditional pagination

## Configuration

Edit the global variables in `connector.py` to set your API endpoint and step parameters:

```python
# Global configuration variables
BASE_URL = "http://127.0.0.1:5001/incremental/step"
INITIAL_ID = 1
STEP_SIZE = 1000
MAX_ID = 100000
```

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare it in your `requirements.txt`.

## API requirements

Your API should support:
- `start_id` parameter to specify the beginning of the ID range
- `end_id` parameter to specify the end of the ID range
- Records with sequential or predictable IDs
- Ability to return records within a specified ID range

## State management

The connector saves state as:
```json
{
  "current_id": 5001
}
```

## Data handling

The connector processes data as follows:
- **Data Extraction**: Fetches records using ID-based range filtering
- **Sequential Processing**: Processes records in sequential ID order
- **Batch Processing**: Processes records in configurable step sizes
- **State Tracking**: Updates the current ID after processing each batch

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

## When to use step-size sync

- APIs that don't support traditional pagination
- When you need to process records by ID ranges
- APIs that support ID-based filtering
- When record IDs are sequential or predictable
- Initial sync scenarios for large datasets

## Error handling

The connector implements comprehensive error handling:
- **API Response Validation**: Checks for successful HTTP responses
- **ID Range Validation**: Ensures proper ID range handling
- **Data Validation**: Ensures data contains required fields
- **State Management**: Safely updates and checkpoints state
- **Detailed Logging**: Provides informative log messages for troubleshooting

## Important considerations

- **Not truly incremental**: This strategy processes all records from the beginning each time
- **ID Requirements**: Requires sequential or predictable record IDs
- **Performance**: May be slower for large datasets as it processes from the initial ID
- **Gaps**: May miss records if there are gaps in the ID sequence
- **Max ID**: Set an appropriate `max_id` to prevent infinite loops

## Configuration parameters

- **initial_id**: Starting ID for the sync (default: 1)
- **step_size**: Number of IDs to process in each batch (default: 1000)
- **max_id**: Maximum ID to process (safety limit)

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team. 