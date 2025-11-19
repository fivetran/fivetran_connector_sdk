# Scrunch Connector Example

## Connector overview
This example shows how to pull AI/ML response and aggregate data from the Scrunch API and load it into a destination using the Fivetran Connector SDK. Scrunch provides individual responses, citations, and performance metrics across prompts, personas, and platforms. The connector:
- Retrieves paginated responses (raw records).
- Executes aggregate queries for overall performance, competitor performance, and daily citations.
- Flattens lists and serializes nested objects for destination compatibility.
- Implements checkpointed incremental syncs per dataset.

Related functions in `connector.py`: `schema`, `update`, `get_all_responses`, `get_responses`, `flatten_response`, `get_scrunch_performance`, `get_competitor_performance`, `get_daily_citations`.

## Requirements
- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements) 
- Operating system:
  - Windows: 10 or later (64-bit only).
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64]).
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64).

## Getting started
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

For local testing, this example includes a `__main__` block that reads `configuration.json` and runs `connector.debug(...)`.

## Features
- Responses ingestion: offset-based pagination over `/v1/{account_id}/responses`.
  - Refer to `get_all_responses` and `get_responses`.
- Aggregates:
  - Overall Scrunch performance.
    - Refer to `get_scrunch_performance`.
  - Competitor performance.
    - Refer to `get_competitor_performance`.
- Data shaping: list fields collapsed with a stable delimiter; `citations` serialized to JSON.
  - Refer to `flatten_response`.
- Incremental syncs: dataset-specific checkpoints with rolling lookbacks.
  - Refer to `update` plus each helper's `op.checkpoint(...)`.

## Configuration file
Detail the configuration keys defined for your connector, which are uploaded to Fivetran from the `configuration.json` file.
```
{
  "api_token": "<YOUR_SCRUNCH_AI_API_TOKEN>", 
  "brand_id": "<YOUR_SPECIFIC_BRAND_ID>"
}
```
- `api_token` (required): The Bearer token required for API authentication.
- `brand_id` (required): The specific brand identifier to filter data from Scrunch AI.

Note: The `brand_id` is a value that you will be getting directly from Scrunch AI.
Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file
The `requirements.txt` should contain the following packages:
`python-dateutil`

- `python-dateutil` is needed to calculate rolling lookback windows.

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication
- type: Bearer token.
- header: `authorization: bearer <api_token>` (provided via `configuration["api_token"]`).
- where set: All `requests.get(...)` calls across helpers set this header.

You can get the API token directly from Scrunch AI.

## Pagination
The `get_all_responses` function handles pagination.

- The function loops while the `offset` is less than the `total` number of responses.
- Each page of responses is retrieved and flattened.
- These flattened items are upserted into the "responses" table using `op.upsert(table="responses", data=...)`.
- The example code advances the offset in increments of 100, relying on the API's default page size.
- Refer to `get_all_responses` and `get_responses` for details.

## Data handling
- Schema definition: `schema(configuration)` returns three tables:
  - `responses` (primary key: `id`).
  - `overall_scrunch_performance`.
  - `competitor_performance`.

- Flattening: `flatten_response` transforms data for destination compatibility:
  - Preserves scalar fields.
  - Converts lists to delimited strings using `__LIST_JOINER = " | "`.
  - Serializes `citations` to a JSON string (`citations_json`) to retain structure without array types.

- Aggregates: Each aggregate helper builds `fields`/`group_by` and upserts rows as returned by the API; no additional type casting is performed in this example.

- Upserts: All writes use `op.upsert(...)`.

## Error handling
- HTTP errors: `response.raise_for_status()` is called on all requests to surface API errors immediately.
- Config validation: Early failure if the `api_token` is missing in configuration.
- Operational logging: Progress prints every 100 upserts in aggregate loaders.

## Tables created

Summary of tables replicated.

### responses
- primary key: `id`.
- selected columns (not exhaustive): `id`, `created_at`, `prompt_id`, `prompt`, `persona_id`, `persona_name`, `country`, `stage`, `branded`,
`platform`, `brand_present`, `brand_sentiment`, `brand_position`, `response_text`, `tags`, `key_topics`, `competitors_present`, `citations_json`.

### overall\_scrunch\_performance
- dimensions: `date`, `date_week`, `date_month`, `date_year`, `prompt_id`, `prompt`, `persona_id`, `persona_name`, `ai_platform`, `branded`.
- metrics: `responses`, `brand_presence_percentage`, `brand_position_score`, `brand_sentiment_score`, `competitor_presence_percentage`.

### competitor\_performance
- dimensions: `date`, `date_week`, `date_month`, `date_year`, `prompt_id`, `prompt`, `ai_platform`, `competitor_id`, `competitor_name`.
- metrics: `responses`, `brand_presence_percentage`, `competitor_presence_percentage`.


## Additional files
There are no additional files required for this connector.

## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
