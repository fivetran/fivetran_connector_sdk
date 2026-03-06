# Jenkins API Connector Example

## Connector overview
The **Jenkins** connector fetches job, build, and artifact data from the **Jenkins REST API** and syncs it to your Fivetran destination.

The connector retrieves detailed information for:
- **Jobs**: Metadata about all Jenkins jobs.
- **Builds**: Job build details such as status, results, timestamps, and URLs.
- **Artifacts**: Build output files with paths and metadata.

It supports incremental synchronization based on build timestamps and processes artifacts only for new builds since the last sync. The connector handles retries, rate limiting, and checkpointing for efficient and reliable data sync.

---

## Requirements
- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
    - Windows: 10 or later (64-bit only)
    - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
    - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)
- Jenkins instance with REST API access enabled
- A Jenkins **username** and **API token**

---

## Getting started
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

---

## Features
- Connects to Jenkins using basic authentication with API token.
- Fetches data from multiple endpoints:
    - `/api/json` for job metadata
    - `/job/{job_name}/api/json` for build listings
    - `/job/{job_name}/{build_number}/api/json` for build artifacts
- Supports incremental synchronization by tracking build timestamps.
- Handles API rate limits (HTTP 429) with exponential backoff.
- Retries failed requests for transient errors and 5xx responses.
- Automatically checkpointed state for resumable syncs.
- Converts timestamps from milliseconds to human-readable UTC format.
- Syncs jobs, builds, and artifacts into separate destination tables.

---

## Configuration file
The connector requires the following configuration parameters in the `configuration.json` file:

```json
{
  "base_url": "<YOUR_JENKINS_URL>",
  "username": "<YOUR_JENKINS_USERNAME>",
  "api_token": "<YOUR_JENKINS_API_TOKEN>"
}
```

**Required fields:**
- `base_url`: Base URL of your Jenkins instance (e.g., `https://jenkins.company.com`)
- `username`: Jenkins username for authentication.
- `api_token`: Jenkins API token for secure access.

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

---

## Authentication
The connector uses **basic authentication** with a username and API token for secure access.

### How to get a Jenkins API token:
1. Log in to your Jenkins account.
2. Click on your profile in the top-right corner.
3. Go to **Configure → API Token**.
4. Generate a new token or copy an existing one.
5. Use the `username` and `api_token` values in your `configuration.json` file.

All API requests are made over HTTPS and include basic authentication credentials.

---

## Pagination
The Jenkins REST API does not support standard pagination for builds and jobs.  
Instead, this connector:
- Fetches all jobs in a single call.
- Iterates through builds per job using the `/job/{job_name}/api/json` endpoint.
- Tracks incremental progress using build timestamps (`timestamp_ms` field).

Incremental state ensures that only new builds and artifacts are processed in each sync.

---

## Data handling
The connector processes Jenkins data as follows:

1. **Job Synchronization**
    - Fetches metadata for all jobs using `/api/json`.
    - Each job includes:
        - Job name
        - URL
        - Status color
        - Raw JSON metadata
    - Upserted into the `jenkins_jobs` table.

2. **Build Synchronization**
    - Retrieves builds for each job via `/job/{job_name}/api/json`.
    - Syncs new builds since the last processed timestamp.
    - Upserts each build into the `jenkins_builds` table with:
        - Build number
        - Result (success, failure, aborted)
        - Timestamps (milliseconds and UTC)
        - Build URL
        - Raw metadata

3. **Artifact Synchronization**
    - For each new build, fetches artifacts from `/job/{job_name}/{build_number}/api/json`.
    - Inserts artifact metadata into the `jenkins_artifacts` table.
    - Links artifacts to builds using `build_id`.

4. **Checkpointing**
    - After processing builds and artifacts for each job, the connector checkpoints the state with:
      ```python
      op.checkpoint(state)
      ```
    - Stores the latest processed timestamp per job for incremental syncs.

---

## Error handling
The connector includes robust error handling and retry logic:
- HTTP 429 (Rate Limit): Retries with exponential backoff using the `Retry-After` header.
- HTTP 5xx (Server Errors): Retries up to 5 times with increasing wait time.
- HTTP 4xx (Client Errors): Raises exceptions for invalid requests or credentials.
- Network Errors: Retries transient issues (timeouts, disconnects) automatically.
- Logging: All warnings, errors, and retry attempts are logged using `fivetran_connector_sdk.Logging`.

Example logs:
```
429 rate limited – sleeping 4s (attempt 2)
502 from https://jenkins.company.com/job/api/json; retrying (attempt 3)
Fetched 25 builds for job 'build_pipeline'
```

---

## Tables created

The connector creates three destination tables:

| Table name | Primary key | Description |
|-------------|-------------|-------------|
| `jenkins_jobs` | `id` | Metadata for each Jenkins job including name, URL, and status color. |
| `jenkins_builds` | `id` | Build records for each job including results, timestamps, and URLs. |
| `jenkins_artifacts` | `id` | Artifact metadata linked to builds including file names and paths. |

### Example table schemas:

#### `jenkins_jobs`
| Column | Type | Description |
|--------|------|-------------|
| `id` | STRING | Job URL or unique identifier. |
| `name` | STRING | Jenkins job name. |
| `url` | STRING | Job URL. |
| `color` | STRING | Job status color (e.g., blue = success). |
| `raw` | JSON | Raw API response for the job. |

#### `jenkins_builds`
| Column | Type | Description |
|--------|------|-------------|
| `id` | STRING | Unique build identifier (`job_name_number`). |
| `job_name` | STRING | Name of the job. |
| `number` | INTEGER | Build number. |
| `result` | STRING | Build result (SUCCESS, FAILURE, ABORTED). |
| `timestamp` | UTC_DATETIME | Build start time (UTC). |
| `timestamp_ms` | INTEGER | Original Jenkins timestamp (milliseconds). |
| `url` | STRING | Build URL. |
| `raw` | JSON | Raw build metadata. |

#### `jenkins_artifacts`
| Column | Type | Description |
|--------|------|-------------|
| `id` | STRING | Unique artifact ID (`jobName_buildNumber_fileName`). |
| `build_id` | STRING | Reference to the related build ID. |
| `job_name` | STRING | Jenkins job name. |
| `build_number` | INTEGER | Build number that generated the artifact. |
| `file_name` | STRING | Artifact file name. |
| `relative_path` | STRING | Artifact path relative to the job workspace. |
| `timestamp_ms` | INTEGER | Build timestamp (milliseconds). |
| `url` | STRING | Artifact URL. |
| `raw` | JSON | Raw artifact metadata. |

---

## Additional considerations
- Incremental syncs depend on the `timestamp_ms` field to track new builds.
- Jenkins instances with nested folders or pipelines may require additional traversal logic.
- The connector can be extended to include build parameters, logs, or test results.
- This example is designed for educational and demonstration purposes using the Fivetran Connector SDK.

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
