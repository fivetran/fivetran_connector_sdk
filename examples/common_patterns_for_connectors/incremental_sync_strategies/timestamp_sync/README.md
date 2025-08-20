# Timestamp-based Incremental Sync Strategy Example

## Connector Overview

This connector demonstrates **timestamp-based incremental sync** using the Fivetran Connector SDK. This strategy uses a timestamp to fetch all records updated since the last sync, saving the latest timestamp as state for the next sync.

## Requirements

* [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)   
* Operating System:  
  * Windows 10 or later  
  * macOS 13 (Ventura) or later

## Getting Started

Refer to the [Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

- **Time-based Filtering**: Uses a timestamp to filter records updated since the last sync
- **State Management**: Saves the latest processed timestamp in connector state
- **Incremental Processing**: Only fetches records with `updatedAt` greater than the saved timestamp
- **Efficient Updates**: Processes only new and modified records
- **Truly Incremental**: Only processes records that have changed since the last sync
- **Efficient**: Minimizes API calls and data processing
- **Reliable**: Handles large datasets without performance degradation
- **Simple**: Easy to implement and maintain

## Configuration

Edit the global variables in `connector.py` to set your API endpoint:

```python
# Global configuration variables
BASE_URL = "http://127.0.0.1:5001/incremental/timestamp"
```

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare it in your `requirements.txt`.

## API requirements

Your API should support:
- `since` parameter to filter records by timestamp
- Records with an `updatedAt` field for timestamp tracking
- Consistent timestamp format (ISO 8601 recommended)
- Ability to return all records updated since a given timestamp

## State management

The connector saves state as:
```json
{
  "last_timestamp": "2024-01-15T10:30:00Z"
}
```

## Data handling

The connector processes data as follows:
- **Data Extraction**: Fetches records using timestamp-based filtering
- **Incremental Processing**: Only processes records updated since the last sync
- **State Tracking**: Updates the timestamp after processing all records

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

## When to use timestamp-based sync

- APIs that support timestamp-based filtering
- When you need truly incremental syncs
- Large datasets where efficiency is important
- When records have reliable `updatedAt` timestamps
- APIs that can return all changes since a given timestamp

## Error handling

The connector implements comprehensive error handling:
- **API Response Validation**: Checks for successful HTTP responses
- **Data Validation**: Ensures data contains required fields
- **State Management**: Safely updates and checkpoints state
- **Detailed Logging**: Provides informative log messages for troubleshooting

## Important considerations

- **Timestamp Accuracy**: Requires accurate and consistent `updatedAt` timestamps
- **Time Zones**: Ensure consistent timezone handling
- **API Limitations**: Some APIs may have limits on how far back you can query
- **Data Consistency**: Assumes records are updated with current timestamps

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team. 