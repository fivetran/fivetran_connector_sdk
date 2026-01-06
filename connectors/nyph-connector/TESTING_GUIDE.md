# NYPH Connector Demo - Testing Guide

This document outlines the step-by-step process for testing the NYPH Connector Demo with both sync modes: `incremental_full_mix` and `rolling_incremental`.

## Overview

The connector supports two sync modes:
- **incremental_full_mix**: Deletes records by UserID and inserts new records
- **rolling_incremental**: Deletes records by TransactionDate and inserts new records

---

## Testing Procedure: incremental_full_mix Mode

### Step 1: Set Sync Mode

1. Open `configuration.json` in the connector directory
2. Set `sync_mode` to `"incremental_full_mix"`:
   ```json
   {
     "sync_mode": "incremental_full_mix",
     ...
   }
   ```

### Step 2: Deploy Code

1. Deploy the connector code to Fivetran
2. Ensure all changes are committed and pushed

### Step 3: Update Private Key in Fivetran UI

**IMPORTANT**: After each deployment, update the `snowflake_private_key` in the Fivetran UI/dashboard:
1. Navigate to the connector configuration in Fivetran dashboard
2. Update the `snowflake_private_key` field with your current private key
3. If using an encrypted key, ensure `snowflake_private_key_passphrase` is also set correctly

### Step 4: Run Initial Historical Sync

1. In Fivetran dashboard, trigger an initial/historical sync
2. This will:
   - Create the `AVAILABILITY` table in Snowflake (if it doesn't exist)
   - Insert all records from `DEMO_INCREMENTAL_FULL_MIX_DATA` using `op.upsert()`
   - Establish initial state

3. Verify the sync completes successfully
4. Check Snowflake to confirm all records are present in the `AVAILABILITY` table

### Step 5: Simulate Incremental Sync

1. Open `connector.py`
2. Locate the `DEMO_INCREMENTAL_FULL_MIX_DATA` array (lines 39-170)
3. Modify the array to simulate incremental changes

### Step 6: Redeploy Code

1. Commit the changes to `connector.py`
2. Redeploy the connector code to Fivetran

### Step 7: Update Private Key Again

**IMPORTANT**: Update the `snowflake_private_key` in Fivetran UI/dashboard again after redeployment.

### Step 8: Trigger Incremental Sync

1. In Fivetran dashboard, trigger another sync
2. This incremental sync will:
   - **Delete** all records from `AVAILABILITY` table where `USERID` matches UserIDs in the updated demo data
   - **Insert** all records from the updated `DEMO_INCREMENTAL_FULL_MIX_DATA` array using `op.upsert()`
   - Update checkpoint state

3. Verify the sync completes successfully
4. Check Snowflake to confirm:
   - Records with modified UserIDs are updated
   - New records are added
   - Records not in the updated array are deleted (if their UserIDs were in the array)

### Expected Behavior

- **Initial Sync**: Uses `op.upsert()` to insert records
- **Incremental Sync**: 
  - Deletes records where `USERID IN (list of UserIDs from file)`
  - Inserts all records from the updated array
  - Processes records in batches of 100,000 with checkpointing

---

## Testing Procedure: rolling_incremental Mode

### Step 1: Set Sync Mode

1. Open `configuration.json`
2. Set `sync_mode` to `"rolling_incremental"`:
   ```json
   {
     "sync_mode": "rolling_incremental",
     ...
   }
   ```

### Step 2: Deploy Code

1. Deploy the connector code to Fivetran
2. Ensure all changes are committed and pushed

### Step 3: Update Private Key in Fivetran UI

**IMPORTANT**: Update the `snowflake_private_key` in the Fivetran UI/dashboard after deployment.

### Step 4: Run Initial Historical Sync

1. In Fivetran dashboard, trigger an initial/historical sync
2. This will:
   - Create the `TRANSACTIONS` table in Snowflake (if it doesn't exist)
   - Insert all records from `DEMO_ROLLING_INCREMENTAL_DATA` using `op.upsert()`
   - Establish initial state

3. Verify the sync completes successfully
4. Check Snowflake to confirm all records are present in the `TRANSACTIONS` table

### Step 5: Simulate Incremental Sync

1. Open `connector.py`
2. Locate the `DEMO_ROLLING_INCREMENTAL_DATA` array (lines 173-314)
3. Modify the array to simulate incremental changes

### Step 6: Redeploy Code

1. Commit the changes to `connector.py`
2. Redeploy the connector code to Fivetran

### Step 7: Update Private Key Again

**IMPORTANT**: Update the `snowflake_private_key` in Fivetran UI/dashboard again after redeployment.

### Step 8: Trigger Incremental Sync

1. In Fivetran dashboard, trigger another sync
2. This incremental sync will:
   - Calculate `min_timestamp` = minimum `TransactionDate` from the updated demo data
   - **Delete** all records from `TRANSACTIONS` table where `TRANSACTIONDATE >= min_timestamp`
   - **Insert** all records from the updated `DEMO_ROLLING_INCREMENTAL_DATA` array
   - Update checkpoint state

3. Verify the sync completes successfully
4. Check Snowflake to confirm:
   - Records with `TransactionDate >= min_timestamp` are deleted and replaced
   - All records from the updated array are inserted
   - Records with dates before `min_timestamp` remain unchanged

### Expected Behavior

- **Initial Sync**: Uses `op.upsert()` to insert records
- **Incremental Sync**: 
  - Finds minimum `TransactionDate` from demo data
  - Deletes records where `TRANSACTIONDATE >= min_timestamp`
  - Inserts all records from the updated array
  - Processes records in batches of 100,000 with checkpointing

---

## Important Notes

### Private Key Updates

- **CRITICAL**: The `snowflake_private_key` must be updated in the Fivetran UI/dashboard **after every deployment** and **before executing any sync**
- All other Snowflake configuration parameters remain constant and only need to be set once

### Sync Mode Switching

- When switching between sync modes, ensure you:
  1. Update `sync_mode` in `configuration.json`
  2. Deploy the code
  3. Update the private key in Fivetran UI
  4. Run a fresh initial sync (this will create/update the appropriate table)

### Demo Data Arrays

- `DEMO_INCREMENTAL_FULL_MIX_DATA`: Used for `incremental_full_mix` mode (targets `AVAILABILITY` table)
- `DEMO_ROLLING_INCREMENTAL_DATA`: Used for `rolling_incremental` mode (targets `TRANSACTIONS` table)
- Both arrays are defined at the top of `connector.py` (lines 39-170 and 173-314)

### Table Names

- Tables are created in **uppercase** format:
  - `AVAILABILITY` (for incremental_full_mix)
  - `TRANSACTIONS` (for rolling_incremental)
- Column names are also converted to uppercase (e.g., `UserID` → `USERID`)

### Checkpointing

- The connector checkpoints after each batch of 100,000 records
- Checkpoint state includes:
  - `last_sync_time`: ISO timestamp
  - `sync_mode`: Current sync mode
  - `records_processed`: Total records processed
  - `last_processed_user_id` (for incremental_full_mix) or `last_transaction_date` (for rolling_incremental)
