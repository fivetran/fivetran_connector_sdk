"""
Weights & Biases
This connector syncs data from Weights & Biases (W&B), including projects, runs, and artifacts.
See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

# For reading configuration from a JSON file
import json

# For working with timestamps and timezone conversions
from datetime import datetime, timezone

# For type hints to improve code readability
from typing import Dict, Any, List

# Weights & Biases Python SDK for accessing W&B API
import wandb

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like upsert(), update(), delete() and checkpoint()
from fivetran_connector_sdk import Operations as op


def validate_configuration(configuration: dict) -> None:
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    This function is called at the start of the update method to ensure that the connector has all necessary configuration values.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing.
    """
    required = ["WandB_API_KEY", "entity"]
    for k in required:
        if not configuration.get(k):
            raise ValueError(f"Missing required configuration value: {k}")


def _now_utc_iso() -> str:
    """
    Get the current UTC timestamp in ISO format.
    This is used for tracking when records were synced.
    Returns:
        A string representing the current UTC timestamp in ISO 8601 format.
    """
    return datetime.now(timezone.utc).isoformat()


def fetch_projects(api: wandb.Api, entity: str) -> List[Dict[str, Any]]:
    """
    Fetch all projects for a given W&B entity.
    Projects are the top-level containers in W&B that group related runs and artifacts.
    Args:
        api: An authenticated W&B API instance.
        entity: The W&B entity (username or team name) to fetch projects from.
    Returns:
        A list of dictionaries, each representing a project with its metadata.
    """
    projects: List[Dict[str, Any]] = []
    for p in api.projects(entity):
        projects.append(
            {
                "project_id": f"{p.entity}/{p.name}",
                "entity": p.entity,
                "name": p.name,
                "url": getattr(p, "url", None),
                "synced_at": _now_utc_iso(),
            }
        )
    return projects


def fetch_run_fields(api: wandb.Api, entity: str) -> List[Dict[str, Any]]:
    """
    Fetch all runs (experiments) across all projects for a given entity.
    Runs represent individual experiments or training sessions in W&B.
    This function iterates through all projects and fetches runs from each.
    Args:
        api: An authenticated W&B API instance.
        entity: The W&B entity (username or team name) to fetch runs from.
    Returns:
        A list of dictionaries, each representing a run with its metadata and state.
    """
    run_fields: List[Dict[str, Any]] = []
    for p in api.projects(entity):
        try:
            log.info(f"Fetching runs for project: {p.name}")
            runs = api.runs(f"{entity}/{p.name}")
            log.info(f"Found {len(runs)} runs in project {p.name}")

            for run in runs:
                try:
                    # Extract user information safely using getattr
                    user = getattr(run, "user", None)
                    username = getattr(user, "username", None) if user else None

                    run_obj = {
                        "run_id": run.id,
                        "name": run.name,
                        "state": run.state,
                        "tags": run.tags,
                        "url": run.url,
                        "entity": run.entity,
                        "project": run.project,
                        "path": f"{run.entity}/{run.project}/{run.id}",
                        "created_at": str(run.created_at),
                        "user": username,
                    }
                    run_fields.append(run_obj)

                except Exception as e:
                    # Log warning for individual run failures but continue processing
                    log.warning(
                        f"Failed to process run {getattr(run, 'id', 'unknown')}: "
                        f"{type(e).__name__}: {str(e)}"
                    )
                    continue

        except Exception as e:
            # Log warning for project-level failures but continue with next project
            log.warning(f"Failed to fetch runs for project {p.name}: {str(e)}")
            continue

    return run_fields


def fetch_artifacts(api: wandb.Api, entity: str) -> List[Dict[str, Any]]:
    """
    Fetch all artifacts across all projects and runs for a given entity.
    Artifacts are versioned data objects (models, datasets, etc.) logged during runs in W&B.
    This function iterates through all projects, then all runs within each project,
    and collects all logged artifacts.
    Args:
        api: An authenticated W&B API instance.
        entity: The W&B entity (username or team name) to fetch artifacts from.
    Returns:
        A list of dictionaries, each representing an artifact with its metadata, version, and size.
    """
    artifacts: List[Dict[str, Any]] = []

    for p in api.projects(entity):
        project_path = f"{entity}/{p.name}"
        try:
            log.info(f"Fetching runs for project: {p.name}")
            runs = api.runs(project_path)
            log.info(f"Found {len(runs)} runs in project {p.name}")

            for run in runs:
                try:
                    # Iterate through all artifacts logged by this run
                    for artifact in run.logged_artifacts():
                        artifacts.append(
                            {
                                "artifact_id": artifact.id,
                                "name": artifact.name,
                                "type": artifact.type,
                                "version": artifact.version,
                                "state": artifact.state,
                                "size": artifact.size,
                                "created_at": str(getattr(artifact, "created_at", None)),
                                "description": getattr(artifact, "description", None),
                                "source_project": str(getattr(artifact, "source_project", None)),
                                "run_id": run.id,
                                "entity": run.entity,
                                "synced_at": _now_utc_iso(),
                            }
                        )
                except Exception as e:
                    # Log warning for individual run failures but continue processing
                    log.warning(
                        f"Failed to fetch artifacts for run {getattr(run, 'id', 'unknown')}: "
                        f"{type(e).__name__}: {str(e)}"
                    )
                    continue

        except Exception as e:
            # Log warning for project-level failures but continue with next project
            log.warning(f"Failed to fetch runs for artifacts in project {p.name}: {str(e)}")
            continue

    return artifacts


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connector-sdk/technical-reference/connector-sdk-code/connector-sdk-methods#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Returns:
        A list of table definitions with their primary keys.
    """
    return [
        {
            "table": "projects",  # W&B projects (top-level containers)
            "primary_key": ["project_id"],  # Unique identifier: entity/project_name
        },
        {
            "table": "run_fields",  # W&B runs (individual experiments)
            "primary_key": ["run_id"],  # Unique identifier for each run
        },
        {
            "table": "artifacts",  # W&B artifacts (versioned data objects)
            "primary_key": ["artifact_id"],  # Unique identifier for each artifact
        },
    ]


def update(configuration: dict, state: dict):
    """
    Define the update function, which is a required function, and is called by Fivetran during each sync.
    See the technical reference documentation for more details on the update function
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    Args:
        configuration: A dictionary containing connection details (WandB_API_KEY and entity)
        state: A dictionary containing state information from previous runs
        The state dictionary is empty for the first sync or for any full re-sync
    """
    # Validate the configuration to ensure it contains all required values
    validate_configuration(configuration)

    # Extract configuration parameters
    api_key = configuration["WandB_API_KEY"]
    entity = configuration["entity"]

    # Initialize the W&B API client with the provided API key
    api = wandb.Api(api_key=api_key)

    # Fetch projects data
    projects = fetch_projects(api, entity)
    log.info(f"Found {len(projects)} projects")

    # Fetch runs data
    run_fields = fetch_run_fields(api, entity)
    log.info(f"Found {len(run_fields)} run-fields")

    # Fetch artifacts data
    artifacts = fetch_artifacts(api, entity)
    log.info(f"Found {len(artifacts)} artifacts")

    # Upsert projects data to destination
    # The 'upsert' operation is used to insert or update data in the destination table
    for proj in projects:
        # The 'upsert' operation is used to insert or update data in the destination table.
        # The first argument is the name of the destination table.
        # The second argument is a dictionary containing the record to be upserted.
        op.upsert(table="projects", data=proj)

    # Upsert run fields data to destination
    for run in run_fields:
        # The 'upsert' operation is used to insert or update data in the destination table.
        # The first argument is the name of the destination table.
        # The second argument is a dictionary containing the record to be upserted.
        op.upsert(table="run_fields", data=run)

    # Upsert artifacts data to destination
    for artifact in artifacts:
        # The 'upsert' operation is used to insert or update data in the destination table.
        # The first argument is the name of the destination table.
        # The second argument is a dictionary containing the record to be upserted.
        op.upsert(table="artifacts", data=artifact)

    # Update state with sync metadata for tracking purposes
    state["last_sync_time"] = _now_utc_iso()
    state["projects_count"] = len(projects)
    state["runs_count"] = len(run_fields)
    state["artifacts_count"] = len(artifacts)

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
    # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
    op.checkpoint(state=state)


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
#
# IMPORTANT: The recommended way to test your connector is using the Fivetran debug command:
#   fivetran debug
#
# This local testing block is provided as a convenience for quick debugging during development,
# such as using IDE debug tools (breakpoints, step-through debugging, etc.).
# Note: This method is not called by Fivetran when executing your connector in production.
# Always test using 'fivetran debug' prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents
    with open("configuration.json", "r") as f:
        configuration = json.load(f)

    # Test the connector locally
    connector.debug(configuration=configuration)
