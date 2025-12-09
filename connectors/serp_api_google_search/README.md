# SerpAPI Organic Google Search Connector Example

## Connector overview
This example demonstrates how to extract top organic Google Search results from the [SerpAPI](https://serpapi.com/search-api) service and load them into a destination using the Fivetran Connector SDK.  
The connector:
- Retrieves the top six organic Google Search results for a user-defined query.  
- Implements resilient API calls with exponential backoff and retries to handle transient network errors.  
- Flattens and enriches structured JSON responses into a compatible tabular format.  
- Performs upserts into a single destination table (`organic_google_search_results`) using a composite primary key.

## Requirements
- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) for setup instructions.

For local testing, this example includes a `__main__` block that reads `configuration.json` and runs `connector.debug(...)`.

## Features
- **Organic Search Results:** Retrieves top organic search results from SerpAPI for a user-specified query.  
- **Error handling:** Retries failed requests with exponential backoff for transient 5xx and connection errors.  
- **Data enrichment:** Merges search-level metadata (query date, URL, parameters) with each organic result.  
- **Schema:** Defines one destination table — `organic_google_search_results`.  
- **Logging:** Uses `fivetran_connector_sdk.Logging` for structured info and error logs.

## Configuration file
The `configuration.json` file provides the SerpAPI credentials and query parameters required for API requests.

```json
{
  "api_key": "<YOUR_SERPAPI_API_KEY>",
  "search_query": "<YOUR_SEARCH_QUERY>"
}
```
- `api_key`: Your SerpAPI API key for authentication.  
- `search_query`: The Google search query string to retrieve organic results for (e.g., "Python programming tutorials").
### Notes
- Ensure that `configuration.json` is not committed to version control.  
- Both configuration values are required; the connector will raise an error if either is missing.

## Requirements file
The `requirements.txt` file lists external libraries needed for this connector.

- `requests` is needed to get HTTP requests
Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## API calls
- **Endpoint:** `https://serpapi.com/search`

### Parameters
- `engine`: `"google"`  
- `q`: `<search_query>`  
- `hl`: `"en"`  
- `gl`: `"us"`  
- `google_domain`: `"google.com"`  
- `api_key`: `<api_key>`  

The connector fetches search results and processes the `organic_results` section of the response, along with metadata fields from `search_metadata`, `search_information`, and `search_parameters`.

## Data handling
- **Schema definition:** `schema(configuration)` defines one table:
  - `organic_google_search_results` (primary key: `search_metadata_id`, `position`)
- Each organic result record is enriched with metadata fields such as:
  - `query_date`
  - `query_url`
  - `search_parameters_q`
  - `search_information_query_displayed`
- Each enriched record is written using `op.upsert(...)` to allow incremental updates.

## Error handling
- Transient network errors: Automatically retried up to 5 times with exponential backoff (1, 2, 4, 8, 16 seconds).  
- Fatal errors: Logged and raised after all retries fail.  
- Configuration validation: Early failure if `api_key` or `search_query` are missing.  
- Logging: Provides detailed information and error messages during sync.

## Tables created
**Summary of the table replicated**

### `organic_google_search_results`
- Primary key: `search_metadata_id`, `position`
- Selected columns (not exhaustive):  
  `search_metadata_id`, `position`, `title`, `link`, `displayed_link`, `snippet`, `query_date`, `query_url`, `search_parameters_q`

## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. 
While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. 
For inquiries, please reach out to our Support team.
