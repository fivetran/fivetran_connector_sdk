# BuildKite Connector for Fivetran

This connector syncs data from BuildKite's REST API to your data warehouse using Fivetran's Connector SDK.

## Tables Synced

- **organizations**: Information about BuildKite organizations
- **pipelines**: All pipelines across organizations
- **builds**: Build information for each pipeline
- **jobs**: Job details for each build
- **artifacts**: Build artifacts associated with jobs
- **agents**: Agent information across organizations

## Setup

1. Get a BuildKite API token with appropriate permissions:
   - Go to BuildKite Dashboard → Personal Settings → API Access
   - Create a new API token with the following permissions:
     - `read_organizations`
     - `read_pipelines`
     - `read_builds`
     - `read_agents`

2. Update the `configuration.json` file with your API token:
   ```json
   {
     "api_token": "YOUR_BUILDKITE_API_TOKEN"
   }
   ```

## Running the Connector Locally

For testing and development, you can run the connector locally:

```bash
python connector.py
```

This will create test database files in the `files` directory.

## Incremental Syncs

The connector tracks the following sync state for each table:
- Organizations: `organizations_updated_at`
- Pipelines: `pipelines_updated_at`
- Builds: `builds_updated_at`
- Jobs: `jobs_updated_at`
- Artifacts: `artifacts_updated_at`
- Agents: `agents_updated_at`

This ensures that subsequent syncs only fetch new or updated data since the last sync.

## Limitations

- Rate limiting: The connector respects BuildKite's API rate limits and implements retry logic
- Large datasets: For organizations with many builds, consider adjusting the sync schedule to avoid API rate limits

## Reference

- [BuildKite REST API Documentation](https://buildkite.com/docs/apis/rest-api)
- [Fivetran Connector SDK Documentation](https://fivetran.com/docs/connectors/connector-sdk)