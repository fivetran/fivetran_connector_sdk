# FDA Tobacco Problem Reports Fivetran Connector

## Purpose
This connector ingests tobacco problem reports from the openFDA API (https://api.fda.gov/tobacco/problem.json) into your Fivetran destination. It dynamically flattens and upserts the first 10 records for demonstration and testing purposes.

## Setup Instructions
1. Place `connector.py`, `requirements.txt`, and `configuration.json` in the same directory.
2. (Optional) Add your openFDA API key to `configuration.json` if you have one.
3. Run the connector using the Fivetran CLI or Python:
   - CLI: `fivetran debug --configuration configuration.json`
   - Python: `python connector.py`

## Configuration Guide
- `api_key`: (string, optional) Your openFDA API key. Leave blank for public access.
- `base_url`: (string) The API endpoint. Default is `https://api.fda.gov/tobacco/problem.json`.

## Testing Procedures
- The connector fetches and upserts the first 10 records only, to avoid rate limits.
- Check logs for upsert counts and errors.
- Validate output in your Fivetran destination (DuckDB warehouse.db if local).

## Troubleshooting
- If you see rate limit errors, wait and retry.
- For API errors, check the openFDA status page.
- Review logs for details on failures.

## References
- [Fivetran Connector SDK Documentation](https://fivetran.com/docs/connector-sdk)
- [SDK Examples Repository](https://github.com/fivetran/fivetran_connector_sdk/tree/main/examples)
- [openFDA API Docs](https://open.fda.gov/apis/tobacco/problem/)

## Best Practices
- Only the primary key is defined in the schema; all other columns are inferred.
- Arrays are flattened with indexed column names.
- Logging and error handling follow Fivetran standards.

## Prompt used
"I need a Fivetran Connector SDK solution for https://api.fda.gov/tobacco/problem.json?. I have some notes and example queries in #file:notes.txt and the fields documented in #file:fields.yaml

Have it dynamically create tables based on the endpoints available. Flatten the dictionaries and upsert the key:value pairs as the columns for the tables. Only define the Primary Key for the schema objects, let Fivetran infer the rest. Process the first 10 responses from each endpoint and then exit gracefully, we do not have an API key and do not want to exceed the limits.

Create a Fivetran Connector SDK solution follows Fivetran best practice outlined in #file:fivetran_connector_sdk.instructions.md. I have the files prepared in #file:FDA_tobacco"

## Limitations
- Only the first 10 records are processed per run.
- No incremental sync/state management (demo only).
- No authentication by default (add API key for higher limits).
