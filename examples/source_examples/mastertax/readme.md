# MasterTax Connector

This connector is designed to interact with the [ADP MasterTax API](https://api.adp.com), enabling automated data extract downloads and upsert operations to Fivetran.

## Overview
This connector:
- Submits data extract jobs to the MasterTax API.
- Monitors job status until completion.
- Downloads and unzips the extract files.
- Parses the tab-delimited data.
- Upserts the data into Fivetran using schema definitions.

## File Structure

```
.
├── connector.py            # Core connector logic for extract/transform/load
├── constants.py            # Layout definitions and column names
├── configuration.json      # Sample configuration (credentials and certs)
```

## Setup Instructions

### 1. Configuration
Update `configuration.json` with your credentials and certificates:

```json
{
  "clientId": "<YOUR_CLIENT_ID>",
  "clientSecret": "<YOUR_CLIENT_SECRET>",
  "keyFile": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----",
  "crtFile": "-----BEGIN CERTIFICATE-----\n...\n-----END CERTIFICATE-----"
}
```

### 2. Local Testing
Ensure you have the `fivetran_connector_sdk` Python package installed.
Run the connector locally using:

```bash
fivetran debug --configuration configuration.json
```

File cleanup is not part of the script since no files are retained between connector syncs. 

### 3. Deployment
Deploy the connector following Fivetran’s Connector SDK documentation: https://fivetran.com/docs/connectors/connector-sdk

## Core Functions

### `update(configuration, state)`
Main function invoked by Fivetran to fetch and sync data.

### `schema(configuration)`
Returns schema definition to Fivetran, including primary keys.

### `sync_items(...)`
Handles the request-submission-download cycle per data extract.

### `upsert_rows(...)`
Parses extracted files and sends rows to Fivetran.

## Notes
- Certificates are created on-the-fly during each run.
- Assumes tab-delimited files without headers.
- No connector-level state is stored.

## Example Extract
Defined in `constants.py` as:

```python
{
  "processNameCode": {"code": "DATA_EXTRACT"},
  "processDefinitionTags": [
    {"tagCode": "LAYOUT_NAME", "tagValues": ["EXTRACT_01"]},
    {"tagCode": "FILTER_NAME", "tagValues": ["EXTRACT_01_FILTER"]}
  ],
  "filterConditions": [
    {"joinType": "oneOf", "attributes": [
      {"attributeID": "COLUMN_03", "operator": "gt", "attributeValue": ["0"]}
    ]}
  ]
}
```
