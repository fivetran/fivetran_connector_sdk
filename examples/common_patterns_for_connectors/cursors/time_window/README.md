# Time Window Cursor Example

## Connector overview

This connector demonstrates how to implement cursor-based pagination using time windows with the Fivetran Connector SDK. It shows how to handle large datasets by breaking them into smaller time-based chunks during the initial sync, which is particularly useful for APIs that support date range queries.

The connector implements a cursor-forward pattern that tracks the last synced timestamp in the state and processes data in configurable time windows. For initial syncs, it starts from a configured date and limits each sync chunk to a specified number of days, continuing forward until reaching the current time.

## Requirements

* [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)   
* Operating system:
  * Windows: 10 or later (64-bit only)
  * macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  * Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started

Refer to the [Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

* **Time-based cursor pagination**: Implements cursor-forward pattern using timestamps
* **Configurable time windows**: Limits each sync chunk to a specified number of days (default: 30 days)
* **State management**: Tracks sync progress using checkpointing
* **Initial sync handling**: Starts from a configured date for first-time syncs
* **Timezone awareness**: Handles UTC timestamps with proper ISO formatting
* **Incremental syncs**: Resumes from the last synced timestamp for subsequent syncs

## Configuration file

This example does not require external configuration as it uses hardcoded constants.

## Requirements file

This example does not require additional dependencies beyond the Fivetran Connector SDK, as it only uses Python's built-in datetime module.

## Authentication

This example does not implement authentication as it's a demonstration of time window cursor logic. In a real implementation, you would add authentication mechanisms appropriate for your data source.

## Pagination

The connector implements time-based cursor pagination using the following approach:

**Refer to lines 25-60 in the `update()` function:**

1. **Time range calculation**: Determines the from/to timestamps for each sync chunk
2. **Chunk processing**: Processes data within the calculated time window
3. **State checkpointing**: Saves progress after each chunk to enable resumption
4. **Forward progression**: Continues until reaching the current time

**Refer to lines 62-85 in the `set_timeranges()` function:**

- For initial syncs: Starts from `__INITIAL_SYNC_START` (2024-06-01)
- For subsequent syncs: Resumes from the last checkpointed timestamp
- Chunk size: Limited to `__DAYS_PER_SYNC` (30 days) for large historical datasets
- Current time handling: Uses current sync time for recent data

## Data handling

**Refer to lines 45-50 in the `update()` function:**

The connector yields timestamp data to demonstrate the time window concept. In a real implementation, you would:

1. **API calls**: Make requests to your data source using the calculated time range
2. **Data transformation**: Process and format the retrieved data
3. **Schema mapping**: Map source data to your target table schema
4. **Data delivery**: Yield upsert operations with the processed data

**Refer to lines 87-100 in the `is_older_than_n_days()` function:**

The connector includes utility functions for timestamp handling:
- Timezone-aware date comparisons
- ISO timestamp parsing and formatting
- Automatic string-to-datetime conversion

## Error handling

This example includes basic error handling through the Fivetran Connector SDK's built-in logging system. The connector uses:

- **Warning logs**: For sync start notifications
- **Fine logs**: For detailed debugging information (only visible during debug mode)
- **State checkpointing**: Ensures sync progress is preserved even if errors occur

## Tables Created

The connector creates a single demonstration table:

**timestamps** - Contains messages showing the time ranges processed during each sync chunk

*Example data:*
```
message: "from 2024-06-01T00:00:00.000Z to 2024-07-01T00:00:00.000Z"
```

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.

### Key constants

**Refer to lines 15-16:**

- `__INITIAL_SYNC_START`: Starting timestamp for initial syncs (2024-06-01T00:00:00.000Z)
- `__DAYS_PER_SYNC`: Maximum days per sync chunk (30)

### Testing

You can test this connector locally using:

```bash
fivetran debug
```

This will run the connector in debug mode, showing detailed logs and processing the time windows as configured. 