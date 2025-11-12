# Bright Data Dataset Connector Configuration Guide

## Overview

This connector filters Bright Data Marketplace datasets using the Filter Dataset API and syncs the filtered results to your Fivetran destination.

**Important**: All configuration values must be provided as **strings** per Fivetran SDK requirements. This includes numeric values like `records_limit`, which should be provided as `"1000"` instead of `1000`.

## Configuration Parameters

### Required Parameters

- **`api_token`** (string, required): Your Bright Data API token.
  - Get your API token from: https://brightdata.com/cp/setting/users
  - Format: Bearer token (the connector will automatically add the "Bearer " prefix)

- **`dataset_id`** (string, required): The ID of the dataset to filter.
  - Example: `"gd_l1viktl72bvl7bjuj0"`
  - Find dataset IDs in your Bright Data account dashboard

- **`filter_name`** (string, required): The name of the field to filter on.
  - Example: `"name"`, `"rating"`, `"reviews_count"`

- **`filter_operator`** (string, required): The operator to use for filtering.
  - See [Supported Operators](#supported-operators) section below
  - Example: `"="`, `">"`, `">="`, `"includes"`

- **`filter_value`** (string, optional): The value to filter by.
  - Required for all operators except `is_null` and `is_not_null`
  - Example: `"John"`, `"4.5"`, `"200"`

### Optional Parameters

- **`records_limit`** (string, optional): Maximum number of records to include in the snapshot.
  - Must be a positive integer provided as a string
  - Example: `"1000"`
  - If not provided, all matching records will be included
  - **Note**: All configuration values must be strings per Fivetran SDK requirements

## Supported Operators

- `=`: Equal to
- `!=`: Not equal to
- `<`: Less than
- `<=`: Less than or equal
- `>`: Greater than
- `>=`: Greater than or equal
- `in`: Value is in the provided list (comma-separated values in filter_value)
- `not_in`: Value is not in the provided list (comma-separated values in filter_value)
- `includes`: Field value contains the filter value
- `not_includes`: Field value does not contain the filter value
- `array_includes`: Filter value is in field value (exact match)
- `not_array_includes`: Filter value is not in field value (exact match)
- `is_null`: Field value is NULL (filter_value not required)
- `is_not_null`: Field value is not NULL (filter_value not required)

## Example Configurations

### Basic Filter

```json
{
  "api_token": "your_api_token",
  "dataset_id": "gd_l1viktl72bvl7bjuj0",
  "filter_name": "name",
  "filter_operator": "=",
  "filter_value": "John"
}
```

### Filter with Records Limit

```json
{
  "api_token": "your_api_token",
  "dataset_id": "gd_l1viktl72bvl7bjuj0",
  "records_limit": "1000",
  "filter_name": "status",
  "filter_operator": "=",
  "filter_value": "active"
}
```

### Numeric Comparison Filter

```json
{
  "api_token": "your_api_token",
  "dataset_id": "gd_l1viktl72bvl7bjuj0",
  "filter_name": "rating",
  "filter_operator": ">=",
  "filter_value": "4.0"
}
```

### Null Check Filter

```json
{
  "api_token": "your_api_token",
  "dataset_id": "gd_l1viktl72bvl7bjuj0",
  "filter_name": "description",
  "filter_operator": "is_not_null"
}
```

Note: `filter_value` is not required for `is_null` and `is_not_null` operators.

### Contains Filter

```json
{
  "api_token": "your_api_token",
  "dataset_id": "gd_l1viktl72bvl7bjuj0",
  "filter_name": "title",
  "filter_operator": "includes",
  "filter_value": "example"
}
```

### In Operator Filter

```json
{
  "api_token": "your_api_token",
  "dataset_id": "gd_l1viktl72bvl7bjuj0",
  "filter_name": "category",
  "filter_operator": "in",
  "filter_value": "electronics,books,clothing"
}
```

## Notes

- The connector creates a snapshot by filtering the dataset, which may take up to 5 minutes to complete
- The connector automatically polls for snapshot completion
- Filtered records are flattened and dynamically mapped to table columns
- All discovered fields are documented in `fields.yaml`
- The connector uses dynamic schema discovery - only primary keys are defined in the schema
- For `is_null` and `is_not_null` operators, `filter_value` is not required and will be ignored if provided
- The connector builds a single-field filter object internally from the provided parameters

## Troubleshooting

- **Missing filter_value**: Ensure `filter_value` is provided for all operators except `is_null` and `is_not_null`
- **Invalid operator**: Verify the operator is one of the supported operators listed above
- **Snapshot timeout**: If snapshots consistently timeout, consider reducing `records_limit`
- **API errors**: Verify your API token is correct and has access to the specified dataset

## References

- [Bright Data Filter Dataset API Documentation](https://docs.brightdata.com/api-reference/marketplace-dataset-api/filter-dataset)
- [Fivetran Connector SDK Documentation](https://fivetran.com/docs/connectors/connector-sdk)
