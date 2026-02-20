# Weights & Biases Connector Example

## Connector overview

This connector syncs data from Weights & Biases (W&B), a machine learning experiment tracking platform, into your data warehouse via Fivetran. It replicates three core entities: projects, runs (experiments), and artifacts (versioned data objects like models and datasets). The connector is designed for data scientists and ML engineers who want to analyze their experiment data alongside other business data for insights into model performance, training costs, and team productivity.

The connector uses the W&B Python SDK to authenticate and retrieve data from the W&B API. It performs full syncs of all available data for the specified entity (user or team) and tracks sync metadata in the state to monitor data volumes across syncs.

## Requirements

- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

- Full sync of W&B projects, runs, and artifacts for a specified entity
- Automatic extraction of run metadata including state, tags, creation date, and associated user
- Artifact versioning and metadata capture including size, type, and source project
- Robust error handling with graceful degradation for individual failed runs or artifacts
- State tracking to monitor sync progress and data volumes
- UTC timestamp tracking for all synced records

## Configuration file

The `configuration.json` file contains the authentication and entity information required to connect to your Weights & Biases account.

```json
{
  "api_key": "<YOUR_WANDB_API_KEY>",
  "entity": "<YOUR_ENTITY_NAME>"
}
```

Configuration parameters:

- `WandB_API_KEY` - Your Weights & Biases API key for authentication
- `entity` - The W&B entity name (username or team name) to sync data from

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file

The `requirements.txt` file specifies the Python libraries required by the connector. This connector requires the Weights & Biases Python SDK to interact with the W&B API.

```
wandb==0.25.0
```

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication

This connector uses API key authentication to access the Weights & Biases API. To obtain your API key:

1. Log in to your [Weights & Biases account](https://wandb.ai).

2. Navigate to your user settings by clicking your profile icon in the top right.

3. Select the **User Settings** option from the dropdown menu.

4. Scroll down to the **API Keys** section.

5. Copy your existing API key or generate a new one.

The API key should be placed in the `WandB_API_KEY` field of your `configuration.json` file. The API key provides access to all projects and runs within the specified entity that your account has permission to view.

## Pagination

The W&B Python SDK handles pagination automatically when iterating through projects, runs, and artifacts. The connector uses iterator patterns to fetch all available data without manual pagination management.

The connector iterates through:

- All projects for the specified entity
- All runs within each project
- All logged artifacts for each run

This approach ensures complete data retrieval while efficiently managing memory by processing records in a streaming fashion rather than loading all data at once.

## Data handling

The connector retrieves data from W&B and transforms it into a structured format suitable for data warehouse storage. Data is processed through three main functions:

- `fetch_projects` - Extracts project metadata including entity, name, URL, and generates a composite project_id
- `fetch_run_fields` - Captures run details including state, tags, user, timestamps, and constructs a full path identifier
- `fetch_artifacts` - Collects artifact information including type, version, size, state, and links back to the originating run

All data is upserted to the destination tables using the `op.upsert` operation in the `update` function. The connector adds a `synced_at` timestamp to projects and artifacts to track when records were last retrieved. Timestamps from W&B are converted to string format for consistent storage.

Data types are inferred by Fivetran based on the values provided, with explicit primary keys defined in the `schema` function for each table.

## Error handling

The connector implements defensive error handling to ensure maximum data retrieval even when individual records fail. Error handling is implemented in `fetch_run_fields` and `fetch_artifacts`:

- Run-level errors - If a single run fails to process, the connector logs a warning with the run ID and error details, then continues processing remaining runs
- Artifact-level errors - If artifacts fail to retrieve for a specific run, the connector logs a warning and continues with the next run
- Project-level errors - If all runs fail to fetch for a project, the connector logs a warning and proceeds to the next project

All errors are logged using the Fivetran logging system with descriptive messages including the affected resource identifier and error type. This allows for troubleshooting while ensuring that partial failures do not block the entire sync.

Configuration validation is performed at the start of each sync in the `validate_configuration` function, which raises a ValueError if required fields are missing.

## Tables created

The connector creates three tables in your destination:

### projects

Contains W&B project information.

- `project_id` (STRING, PRIMARY KEY) - Composite identifier in format "entity/project_name"
- `entity` (STRING) - The W&B entity that owns the project
- `name` (STRING) - Project name
- `url` (STRING) - Web URL to access the project in W&B
- `synced_at` (STRING) - ISO 8601 timestamp when the record was synced

### run_fields

Contains experiment run data with metadata about each training run or experiment.

- `run_id` (STRING, PRIMARY KEY) - Unique identifier for the run
- `name` (STRING) - Display name of the run
- `state` (STRING) - Current state of the run (e.g., running, finished, crashed)
- `tags` (ARRAY) - List of tags applied to the run
- `url` (STRING) - Web URL to access the run in W&B
- `entity` (STRING) - The W&B entity that owns the run
- `project` (STRING) - Project name the run belongs to
- `path` (STRING) - Full path identifier in format "entity/project/run_id"
- `created_at` (STRING) - Timestamp when the run was created
- `user` (STRING) - Username of the person who created the run

### artifacts

Contains versioned artifacts logged during runs, such as models and datasets.

- `artifact_id` (STRING, PRIMARY KEY) - Unique identifier for the artifact
- `name` (STRING) - Artifact name
- `type` (STRING) - Artifact type (e.g., model, dataset)
- `version` (STRING) - Version identifier
- `state` (STRING) - Artifact state
- `size` (INTEGER) - Size in bytes
- `created_at` (STRING) - Timestamp when the artifact was created
- `description` (STRING) - Artifact description
- `source_project` (STRING) - Source project information
- `run_id` (STRING) - ID of the run that logged this artifact
- `entity` (STRING) - The W&B entity that owns the artifact
- `synced_at` (STRING) - ISO 8601 timestamp when the record was synced

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
