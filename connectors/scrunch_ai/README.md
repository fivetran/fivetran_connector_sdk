# Scrunch Connector Example

## connector overview
this example shows how to pull ai/ml response and aggregate data from the scrunch api and load it into a destination using the fivetran connector sdk. scrunch provides individual responses, citations, and performance metrics across prompts, personas, and platforms. the connector:
- retrieves paginated responses (raw records)
- executes aggregate queries for overall performance, competitor performance, and daily citations
- flattens lists and serializes nested objects for destination compatibility
- implements checkpointed incremental syncs per dataset

related functions in `connector.py`: `schema`, `update`, `get_all_responses`, `get_responses`, `flatten_response`, `get_scrunch_peformance` (spelling matches code), `get_competitor_performance`, `get_daily_citations`

## requirements
- [supported python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements) (use **python 3**)
- operating system:
  - windows: 10 or later (64-bit only)
  - macos: 13 (ventura) or later (apple silicon [arm64] or intel [x86_64])
  - linux: ubuntu 20.04+ / debian 10+ / amazon linux 2+ (arm64 or x86_64)

## getting started
refer to the [connector sdk setup guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

for local testing, this example includes a `__main__` block that reads `configuration.json` and runs `connector.debug(...)`.

## features
- responses ingestion: offset-based pagination over `/v1/{account_id}/responses`  
  see `get_all_responses`, `get_responses`
- aggregates:
  - overall scrunch performance  
    see `get_scrunch_peformance`
  - competitor performance  
    see `get_competitor_performance`
  - daily citations  
    see `get_daily_citations`
- data shaping: list fields collapsed with a stable delimiter; `citations` serialized to json  
  see `flatten_response`
- incremental syncs: dataset-specific checkpoints with rolling lookbacks  
  see `update` plus each helper’s `op.checkpoint(...)`

## configuration file
the connector expects a minimal configuration uploaded from `configuration.json`.
```
{
  "api_token": "<your_api_token>", 
  "brand_id": "<specific_brand_id>"
}
```
The brand_id is a value that you will be getting directly from Scrunch AI

## Requirements file
python-dotenv==1.1.1 is required to load environment variables from a .env file

python_dateutil==2.9.0.post0 is needed to calclate rolling lookback windows

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication
- **type**: bearer token  
- **header**: `authorization: bearer <api_token>` (provided via `configuration["api_token"]`)  
- **where set**: all `requests.get(...)` calls across helpers set this header  

You can get the API token directly from Scrunch AI

## Pagination
The `get_all_responses` function loops while the `offset` is less than the `total` number of responses.
* Each page of responses is retrieved. The items from that page are then flattened.
* These flattened items are **upserted** into the "responses" table using `op.upsert(table="responses", data=...)`.
* The example code advances the **offset** in increments of 100. This relies on the API's default page size. 
* For more details, refer to the `get_responses` and `get_all_responses` functions.

## Data handling
### schema definition
`schema(configuration)` returns four tables:
- **responses** (primary key: `id`)
- **overall_scrunch_performance**
- **competitor_performance**
- **daily_citations**

### flattening
`flatten_response`:
- preserves scalar fields  
- converts lists to delimited strings using `LIST_JOINER = " | "`  
- serializes `citations` to a JSON string (`citations_json`) to retain structure without array types

### aggregates
each aggregate helper builds `fields`/`group_by` and upserts rows as returned by the API.  
no additional type casting is performed in this example.

### upserts
all writes use:
```
python op.upset(...)
```

## Error handling
- **http errors**:  
  `response.raise_for_status()` is called on all requests to surface API errors immediately.

- **config validation**:  
  early failure if token is missing in configuration.  

- **operational logging**:  
  progress prints every 100 upserts in aggregate loaders.


## Tables created

### responses
- **primary key**: `id`  
- **selected columns** (not exhaustive):  
  `id`, `created_at`, `prompt_id`, `prompt`, `persona_id`, `persona_name`, `country`, `stage`, `branded`, `platform`, `brand_present`, `brand_sentiment`, `brand_position`, `response_text`, `tags`, `key_topics`, `competitors_present`, `citations_json`

### overall_scrunch_performance
- **dimensions**:  
  `date`, `date_week`, `date_month`, `date_year`, `prompt_id`, `prompt`, `persona_id`, `persona_name`, `ai_platform`, `branded`
- **metrics**:  
  `responses`, `brand_presence_percentage`, `brand_position_score`, `brand_sentiment_score`, `competitor_presence_percentage`

### competitor_performance
- **dimensions**:  
  `date`, `date_week`, `date_month`, `date_year`, `prompt_id`, `prompt`, `ai_platform`, `competitor_id`, `competitor_name`
- **metrics**:  
  `responses`, `brand_presence_percentage`, `competitor_presence_percentage`

### daily_citations
- **dimensions**:  
  `date`, `date_week`, `date_month`, `date_year`, `prompt_id`, `prompt`, `ai_platform`, `source_type`, `source_url`
- **metrics**:  
  `responses`

