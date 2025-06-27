# Cursor Forward Example

This example demonstrates how to implement cursor-based pagination using the Fivetran Connector SDK. It shows how to handle large datasets by breaking them into smaller time-based chunks during the initial sync.

## Overview

The connector implements a cursor-forward pattern where:

- It tracks the last synced timestamp in the state
- For initial syncs, it starts from a configured date (`INITIAL_SYNC_START`)
- It limits each sync chunk to 30 days of data (`DAYS_PER_SYNC`)
- It continues syncing forward until reaching the current time

## Key Components

### Configuration

- `INITIAL_SYNC_START`: The starting timestamp for initial syncs (2024-06-01)
- `DAYS_PER_SYNC`: Maximum number of days to sync in each chunk (30)

### Main Functions

- `update()`: The main sync function that:
  - Determines the time range to sync
  - Yields data to Fivetran
  - Checkpoints progress in state
  - Continues until reaching current time

- `set_timeranges()`: Calculates the from/to timestamps for each sync chunk

- `is_older_than_n_days()`: Helper to check if a date is beyond the chunk size

## Usage

The connector can be run locally for testing using:

```bash
fivetran debug
```
