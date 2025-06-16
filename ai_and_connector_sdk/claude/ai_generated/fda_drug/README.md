# FDA Drug API Fivetran Connector

A comprehensive Fivetran Connector SDK solution for syncing data from the FDA Drug API with endpoint discovery, incremental syncing, and configurable data flattening.

## Features

- **Dynamic Endpoint Discovery**: Automatically discovers and syncs from all available FDA Drug API endpoints
- **Incremental Sync**: Uses `listing_expiration_date` for efficient incremental updates
- **Flexible Data Structure**: Supports both flattened tables and child table relationships
- **Rate Limiting**: Built-in rate limiting with configurable delays to respect API limits
- **Quota Management**: Configurable requests per checkpoint to avoid quota exhaustion
- **Optional Authentication**: Works with or without API key (higher limits with key)
- **State Management**: Robust checkpointing for reliable incremental syncs

## Supported Endpoints

The connector automatically discovers and syncs from these FDA Drug API endpoints:
- `ndc` - National Drug Code Directory
- `event` - Adverse Event Reports
- `label` - Drug Labeling
- `enforcement` - Enforcement Reports

## Configuration

### Required Configuration (`configuration.json`)

```json
{
  "api_key": "",
  "base_url": "https://api.fda.gov/drug/",
  "requests_per_checkpoint": "10",
  "rate_limit_delay": "0.25",
  "flatten_nested": "true",
  "create_child_tables": "false",
  "incremental_date_field": "listing_expiration_date"
}
```

### Configuration Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `api_key` | string | `""` | Optional FDA API key for higher rate limits (120K/day vs 1K/day) |
| `base_url` | string | `"https://api.fda.gov/drug/"` | Base URL for FDA Drug API |
| `requests_per_checkpoint` | string | `"10"` | Number of API requests before checkpointing state |
| `rate_limit_delay` | string | `"0.25"` | Delay in seconds between API requests |
| `flatten_nested` | string | `"true"` | Flatten nested objects with prefixed column names |
| `create_child_tables` | string | `"false"` | Create separate tables for nested arrays |
| `incremental_date_field` | string | `"listing_expiration_date"` | Date field for incremental sync |

## Data Structure

### Flattened Mode (Default)
- Nested objects are flattened with prefixed column names
- Arrays are converted to JSON strings or indexed flat keys
- Single table per endpoint: `fda_drug_ndc`, `fda_drug_event`, etc.

### Child Tables Mode
- Nested arrays become separate child tables
- Main tables contain summary counts
- Child tables linked via `_parent_id` and `_index` keys

### Example Table Structure

**Main Table: `fda_drug_ndc`**
- `_primary_key` (product_id or product_ndc)
- `product_ndc`
- `generic_name`
- `brand_name`
- `openfda_manufacturer_name`
- `active_ingredients_0_name`
- `active_ingredients_0_strength`
- ... (flattened fields)

**Child Table: `fda_drug_ndc_packaging`** (if enabled)
- `_parent_id`
- `_index`
- `package_ndc`
- `description`
- `marketing_start_date`

## Installation & Setup

1. **Create project directory**:
   ```bash
   mkdir fda_drug
   cd fda_drug
   ```

2. **Add connector files**:
   - `connector.py` - Main connector implementation
   - `requirements.txt` - Python dependencies
   - `configuration.json` - Configuration template

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure API access** (optional):
   - Get free API key from https://open.fda.gov/apis/authentication/
   - Add to `configuration.json`

## Testing

### CLI Testing
```bash
fivetran debug --configuration configuration.json
```

### Python Testing
```python
python connector.py
```

### Validation Steps

1. **Check DuckDB Output**:
   ```bash
   sqlite3 warehouse.db ".tables"
   sqlite3 warehouse.db "SELECT COUNT(*) FROM fda_drug_ndc;"
   ```

2. **Review Logs**:
   - Look for endpoint discovery messages
   - Verify checkpointing at configured intervals
   - Check for rate limiting and quota management

3. **Expected Output**:
   ```
   Operation     | Calls
   ------------- + ------------
   Upserts       | 1000+
   Updates       | 0
   Deletes       | 0
   Truncates     | 0
   SchemaChanges | 4+
   Checkpoints   | 10+
   ```

## API Rate Limits

| Authentication | Requests/Minute | Requests/Day |
|---------------|-----------------|-------------- |
| No API Key | 240 | 1,000 |
| With API Key | 240 | 120,000 |

The connector respects these limits with:
- Built-in rate limiting (250ms delay between requests)
- Configurable checkpoint frequency
- Automatic retry on rate limit errors

## Incremental Sync Strategy

- Uses `listing_expiration_date` as cursor field
- Stores cursor per endpoint in connector state
- Searches for records >= last cursor value
- Falls back to full sync on first run

## Troubleshooting

### Common Issues

1. **Rate Limit Exceeded**:
   - Increase `rate_limit_delay` value
   - Reduce `requests_per_checkpoint`
   - Consider getting API key

2. **Authentication Errors**:
   - Verify API key format
   - Check API key is valid and active
   - Ensure HTTPS is used

3. **Data Structure Issues**:
   - Toggle `flatten_nested` setting
   - Enable `create_child_tables` for complex data
   - Check field mappings in logs

4. **Sync Performance**:
   - Adjust `requests_per_checkpoint` for balance
   - Monitor checkpoint frequency in logs
   - Consider endpoint-specific configuration

### Debug Commands

```bash
# Check connector structure
python -m py_compile connector.py

# Test configuration
python -c "import json; print(json.load(open('configuration.json')))"

# Validate dependencies
pip check
```

## Best Practices

1. **Start Small**: Begin with low `requests_per_checkpoint` values
2. **Monitor Quotas**: Track daily API usage
3. **Use API Keys**: Get higher limits for production use
4. **Test Incrementally**: Verify cursor-based syncing works correctly
5. **Handle Errors**: Monitor logs for API errors and timeouts

## Production Deployment

1. **Get API Key**: Register at https://open.fda.gov/apis/authentication/
2. **Configure Limits**: Set appropriate `requests_per_checkpoint` based on data volume
3. **Monitor Performance**: Track sync duration and data volumes
4. **Set Alerts**: Monitor for API errors and quota exhaustion

## Support

- **FDA API Documentation**: https://open.fda.gov/apis/
- **Fivetran Connector SDK**: https://fivetran.com/docs/connector-sdk
- **API Status**: https://open.fda.gov/apis/status/

## License

This connector follows FDA API terms of service and Fivetran SDK licensing.
