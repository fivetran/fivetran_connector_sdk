"""
This connector syncs workflow and schedule data from Temporal Cloud to Fivetran.
It connects to Temporal Cloud using the Temporal Python SDK and fetches workflow executions and schedule configurations.
See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

# For reading configuration from a JSON file
import json

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like upsert(), update(), delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# For running asynchronous Temporal client operations
import asyncio

# Temporal Python SDK for connecting to Temporal Cloud
from temporalio.client import Client

# For handling timestamps with timezone awareness
from datetime import datetime, timezone

# For implementing retry logic with exponential backoff
import time
from functools import wraps

# Constants for checkpoint intervals
__WORKFLOW_CHECKPOINT_INTERVAL = 100  # Checkpoint every N workflows
__SCHEDULE_CHECKPOINT_INTERVAL = 50  # Checkpoint every N schedules

# Constants for retry logic
__MAX_RETRIES = 5  # Maximum number of retry attempts for transient failures
__MAX_BACKOFF_SECONDS = 60  # Maximum backoff time in seconds


def is_transient_error(error: Exception) -> bool:
    """
    Determine if an error is transient and should be retried.

    Args:
        error: Exception that occurred

    Returns:
        True if error is transient (network, timeout, rate limit), False for permanent errors
    """
    error_message = str(error).lower()

    # Permanent errors - fail fast (authentication, authorization, invalid configuration)
    permanent_error_indicators = [
        "unauthorized",
        "authentication",
        "invalid api key",
        "forbidden",
        "access denied",
        "invalid credentials",
        "permission denied",
    ]

    for indicator in permanent_error_indicators:
        if indicator in error_message:
            return False

    # Transient errors - should retry (network, timeouts, rate limits, 5xx errors)
    transient_error_indicators = [
        "timeout",
        "connection",
        "network",
        "unavailable",
        "rate limit",
        "too many requests",
        "503",
        "502",
        "500",
        "429",
    ]

    for indicator in transient_error_indicators:
        if indicator in error_message:
            return True

    # Default to treating unknown errors as transient to avoid data loss
    return True


def retry_with_exponential_backoff(max_retries: int = __MAX_RETRIES):
    """
    Decorator that implements retry logic with exponential backoff for transient failures.

    Retries transient errors (network, rate limits, 5xx) with exponential backoff.
    Fails fast for permanent errors (authentication, authorization).

    Args:
        max_retries: Maximum number of retry attempts (default: __MAX_RETRIES)

    Returns:
        Decorated function with retry logic
    """

    def decorator(func):
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            last_exception = None

            for attempt in range(max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    last_exception = e

                    # Check if error is permanent - fail fast
                    if not is_transient_error(e):
                        log.severe(f"Permanent error in {func.__name__}: {str(e)}. Not retrying.")
                        raise

                    # Last attempt - don't retry
                    if attempt == max_retries:
                        log.severe(
                            f"Max retries ({max_retries}) reached for {func.__name__}: {str(e)}"
                        )
                        break

                    # Calculate exponential backoff: 2^attempt, capped at __MAX_BACKOFF_SECONDS
                    sleep_time = min(__MAX_BACKOFF_SECONDS, 2**attempt)

                    log.warning(
                        f"Transient error in {func.__name__} (attempt {attempt + 1}/{max_retries}): {str(e)}. "
                        f"Retrying in {sleep_time} seconds..."
                    )

                    time.sleep(sleep_time)

            # All retries exhausted
            raise RuntimeError(
                f"Failed after {max_retries} retries: {str(last_exception)}"
            ) from last_exception

        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            last_exception = None

            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e

                    # Check if error is permanent - fail fast
                    if not is_transient_error(e):
                        log.severe(f"Permanent error in {func.__name__}: {str(e)}. Not retrying.")
                        raise

                    # Last attempt - don't retry
                    if attempt == max_retries:
                        log.severe(
                            f"Max retries ({max_retries}) reached for {func.__name__}: {str(e)}"
                        )
                        break

                    # Calculate exponential backoff: 2^attempt, capped at __MAX_BACKOFF_SECONDS
                    sleep_time = min(__MAX_BACKOFF_SECONDS, 2**attempt)

                    log.warning(
                        f"Transient error in {func.__name__} (attempt {attempt + 1}/{max_retries}): {str(e)}. "
                        f"Retrying in {sleep_time} seconds..."
                    )

                    time.sleep(sleep_time)

            # All retries exhausted
            raise RuntimeError(
                f"Failed after {max_retries} retries: {str(last_exception)}"
            ) from last_exception

        # Return appropriate wrapper based on function type
        import inspect

        if inspect.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper

    return decorator


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    This function is called at the start of the update method to ensure that the connector has all necessary configuration values.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing.
    """
    required_configs = ["temporal_host", "temporal_namespace", "temporal_api_key"]
    for key in required_configs:
        if key not in configuration or not configuration.get(key):
            raise ValueError(f"Missing required configuration value: {key}")


def _extract_config_values(configuration: dict):
    """Extract Temporal Cloud configuration values as a tuple."""
    return (
        configuration.get("temporal_host"),
        configuration.get("temporal_namespace"),
        configuration.get("temporal_api_key"),
    )


@retry_with_exponential_backoff(max_retries=__MAX_RETRIES)
async def _connect_temporal_client(
    temporal_host: str, temporal_namespace: str, temporal_api_key: str
):
    """
    Create and return a connected Temporal Cloud client with retry logic.

    Automatically retries transient connection failures with exponential backoff.
    Fails fast for authentication errors.
    """
    return await Client.connect(
        temporal_host, namespace=temporal_namespace, api_key=temporal_api_key, tls=True
    )


def _calculate_execution_time(workflow):
    """
    Calculate workflow execution time in seconds.

    Args:
        workflow: Workflow object from Temporal

    Returns:
        Execution time in seconds or None if unavailable
    """
    if workflow.start_time and workflow.close_time:
        duration = workflow.close_time - workflow.start_time
        return duration.total_seconds()

    if hasattr(workflow, "execution_time") and workflow.execution_time:
        if hasattr(workflow.execution_time, "total_seconds"):
            return workflow.execution_time.total_seconds()

    return None


def _build_workflow_data(workflow):
    """
    Build workflow data dictionary from workflow object.

    Args:
        workflow: Workflow object from Temporal

    Returns:
        Dictionary containing workflow data
    """
    return {
        "workflow_id": workflow.id,
        "run_id": workflow.run_id,
        "workflow_type": workflow.workflow_type,
        "start_time": workflow.start_time.isoformat() if workflow.start_time else None,
        "close_time": workflow.close_time.isoformat() if workflow.close_time else None,
        "status": str(workflow.status),
        "task_queue": workflow.task_queue,
        "execution_time_seconds": _calculate_execution_time(workflow),
        "memo": workflow.memo if hasattr(workflow, "memo") else None,
        "search_attributes": (
            workflow.search_attributes if hasattr(workflow, "search_attributes") else None
        ),
    }


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connector-sdk/technical-reference/connector-sdk-code/connector-sdk-methods#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    return [
        {"table": "workflow", "primary_key": ["workflow_id", "run_id"]},
        {"table": "schedule", "primary_key": ["schedule_id"]},
    ]


async def fetch_temporal_workflows(configuration: dict, state: dict):
    """
    Async function to connect to Temporal Cloud and fetch workflow data.
    Upserts workflows immediately and checkpoints periodically to avoid memory overflow.

    Args:
        configuration: Dictionary containing Temporal Cloud credentials
        state: State dictionary for checkpointing

    Returns:
        Number of workflows processed
    """
    try:
        # Validate and extract configuration
        validate_configuration(configuration)
        temporal_host, temporal_namespace, temporal_api_key = _extract_config_values(configuration)

        log.info(f"Connecting to Temporal Cloud at {temporal_host}")

        # Connect to Temporal Cloud
        client = await _connect_temporal_client(
            temporal_host, temporal_namespace, temporal_api_key
        )

        log.info("Successfully connected to Temporal Cloud")

        # List all workflows (no filters for maximum data coverage)
        workflow_count = 0
        async for workflow in client.list_workflows():
            workflow_count += 1

            # Build workflow data using helper function
            workflow_data = _build_workflow_data(workflow)

            # The 'upsert' operation is used to insert or update data in the destination table.
            # The first argument is the name of the destination table.
            # The second argument is a dictionary containing the record to be upserted.
            op.upsert(table="workflow", data=workflow_data)

            # Checkpoint periodically to ensure safe resume and avoid memory overflow
            # This tells Fivetran it's safe to write to destination at regular intervals
            if workflow_count % __WORKFLOW_CHECKPOINT_INTERVAL == 0:
                current_timestamp = datetime.now(timezone.utc).isoformat()
                checkpoint_state = {
                    **state,
                    "last_sync": current_timestamp,
                    "workflows_synced": workflow_count,
                }
                # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                # from the correct position in case of next sync or interruptions.
                # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
                # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
                # Learn more about how and where to checkpoint by reading our best practices documentation
                # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
                op.checkpoint(checkpoint_state)
                log.info(f"Processed and checkpointed {workflow_count} workflows")

        log.info(f"Successfully fetched {workflow_count} workflows from Temporal Cloud")
        return workflow_count

    except ValueError as ve:
        log.severe(f"Configuration error: {str(ve)}")
        raise
    except Exception as e:
        log.severe(f"Error connecting to Temporal Cloud: {str(e)}")
        raise


def _extract_next_action_times(schedule):
    """Extract next action times from schedule info."""
    next_action_times = []
    if hasattr(schedule.info, "next_action_times") and schedule.info.next_action_times:
        next_action_times = [time.isoformat() for time in schedule.info.next_action_times]
    return next_action_times


def _extract_action_data(action):
    """Extract action data from a recent action."""
    return {
        "scheduled_time": (
            action.scheduled_time.isoformat()
            if hasattr(action, "scheduled_time") and action.scheduled_time
            else None
        ),
        "actual_time": (
            action.actual_time.isoformat()
            if hasattr(action, "actual_time") and action.actual_time
            else None
        ),
        "start_workflow_result": (
            str(action.start_workflow_result) if hasattr(action, "start_workflow_result") else None
        ),
    }


def _extract_recent_actions(schedule):
    """Extract recent actions from schedule info."""
    recent_actions = []
    if hasattr(schedule.info, "recent_actions") and schedule.info.recent_actions:
        for action in schedule.info.recent_actions:
            recent_actions.append(_extract_action_data(action))
    return recent_actions


def _extract_spec_field(spec, field_name, converter=None):
    """Extract a field from spec with optional conversion."""
    if not hasattr(spec, field_name):
        return None
    value = getattr(spec, field_name)
    if not value:
        return None
    if converter:
        return converter(value)
    return value


def _extract_schedule_spec(schedule):
    """Extract schedule specification details."""
    if not hasattr(schedule.schedule, "spec") or not schedule.schedule.spec:
        return None

    spec = schedule.schedule.spec
    return {
        "calendars": _extract_spec_field(spec, "calendars", str),
        "cron_expressions": _extract_spec_field(spec, "cron_expressions", list),
        "intervals": _extract_spec_field(
            spec, "intervals", lambda x: [str(interval) for interval in x]
        ),
        "start_at": _extract_spec_field(spec, "start_at", lambda x: x.isoformat()),
        "end_at": _extract_spec_field(spec, "end_at", lambda x: x.isoformat()),
        "timezone": _extract_spec_field(spec, "timezone_name"),
        "jitter": _extract_spec_field(spec, "jitter", str),
    }


def _extract_schedule_action(schedule):
    """Extract schedule action (workflow to trigger)."""
    if not hasattr(schedule.schedule, "action") or not schedule.schedule.action:
        return None

    action = schedule.schedule.action
    if not hasattr(action, "workflow"):
        return None

    return {
        "workflow_type": (
            action.workflow if isinstance(action.workflow, str) else str(action.workflow)
        ),
        "task_queue": action.task_queue if hasattr(action, "task_queue") else None,
        "workflow_id": action.id if hasattr(action, "id") else None,
    }


def _extract_schedule_state(schedule):
    """Extract schedule state (paused/active and notes)."""
    paused = False
    note = None

    if hasattr(schedule.schedule, "state") and schedule.schedule.state:
        state = schedule.schedule.state
        paused = state.paused if hasattr(state, "paused") else False
        note = state.note if hasattr(state, "note") else None

    return paused, note


def _build_schedule_data(schedule):
    """Build complete schedule data dictionary from schedule object."""
    paused, note = _extract_schedule_state(schedule)

    return {
        "schedule_id": schedule.id,
        "paused": paused,
        "note": note,
        "next_action_times": _extract_next_action_times(schedule),
        "recent_actions": _extract_recent_actions(schedule),
        "schedule_spec": _extract_schedule_spec(schedule),
        "schedule_action": _extract_schedule_action(schedule),
        "search_attributes": (
            schedule.search_attributes if hasattr(schedule, "search_attributes") else None
        ),
    }


async def fetch_temporal_schedules(configuration: dict, state: dict):
    """
    Async function to connect to Temporal Cloud and fetch schedule data.
    Upserts schedules immediately and checkpoints periodically to avoid memory overflow.

    Args:
        configuration: Dictionary containing Temporal Cloud credentials
        state: State dictionary for checkpointing

    Returns:
        Number of schedules processed
    """
    try:
        # Validate and extract configuration
        validate_configuration(configuration)
        temporal_host, temporal_namespace, temporal_api_key = _extract_config_values(configuration)

        log.info(f"Connecting to Temporal Cloud at {temporal_host} to fetch schedules")

        # Connect to Temporal Cloud
        client = await _connect_temporal_client(
            temporal_host, temporal_namespace, temporal_api_key
        )

        log.info("Successfully connected to Temporal Cloud for schedules")

        # List all schedules
        schedule_count = 0
        schedule_iterator = await client.list_schedules()
        async for schedule in schedule_iterator:
            schedule_count += 1

            # Build schedule data using helper function
            schedule_data = _build_schedule_data(schedule)

            # The 'upsert' operation is used to insert or update data in the destination table.
            # The first argument is the name of the destination table.
            # The second argument is a dictionary containing the record to be upserted.
            op.upsert(table="schedule", data=schedule_data)

            # Checkpoint periodically to ensure safe resume and avoid memory overflow
            # This tells Fivetran it's safe to write to destination at regular intervals
            if schedule_count % __SCHEDULE_CHECKPOINT_INTERVAL == 0:
                current_timestamp = datetime.now(timezone.utc).isoformat()
                checkpoint_state = {
                    **state,
                    "last_sync": current_timestamp,
                    "schedules_synced": schedule_count,
                }
                # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                # from the correct position in case of next sync or interruptions.
                # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
                # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
                # Learn more about how and where to checkpoint by reading our best practices documentation
                # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
                op.checkpoint(checkpoint_state)
                log.info(f"Processed and checkpointed {schedule_count} schedules")

        log.info(f"Successfully fetched {schedule_count} schedules from Temporal Cloud")
        return schedule_count

    except ValueError as ve:
        log.severe(f"Configuration error: {str(ve)}")
        raise
    except Exception as e:
        log.severe(f"Error fetching schedules from Temporal Cloud: {str(e)}")
        raise


def update(configuration: dict, state: dict):
    """
    Define the update function, which is a required function, and is called by Fivetran during each sync.
    See the technical reference documentation for more details on the update function
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    Args:
        configuration: A dictionary containing connection details
        state: A dictionary containing state information from previous runs
        The state dictionary is empty for the first sync or for any full re-sync
    """

    # Validate the configuration to ensure it contains all required values.
    validate_configuration(configuration=configuration)

    log.info("Starting Temporal sync for workflows and schedules")
    log.warning("Example: Connectors : Temporal Cloud")
    try:
        # Fetch and upsert workflows immediately with periodic checkpointing
        # The fetch function handles upsert and checkpoint internally to avoid memory overflow
        log.info("Fetching workflows...")
        workflow_count = asyncio.run(fetch_temporal_workflows(configuration, state))
        log.info(f"Successfully synced {workflow_count} workflows")

        # Fetch and upsert schedules immediately with periodic checkpointing
        # The fetch function handles upsert and checkpoint internally to avoid memory overflow
        log.info("Fetching schedules...")
        schedule_count = asyncio.run(fetch_temporal_schedules(configuration, state))
        log.info(f"Successfully synced {schedule_count} schedules")

        # Final checkpoint with sync completion timestamp
        current_timestamp = datetime.now(timezone.utc).isoformat()
        final_state = {
            "last_sync": current_timestamp,
            "workflows_synced": workflow_count,
            "schedules_synced": schedule_count,
        }

        log.info(f"Sync completed: {workflow_count} workflows, {schedule_count} schedules")

        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
        # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
        op.checkpoint(final_state)

    except Exception as e:
        # In case of an exception, raise a runtime error
        raise RuntimeError(f"Failed to sync data: {str(e)}")


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
    with open("configuration.json", "r", encoding="utf-8") as f:
        configuration = json.load(f)

    # Test the connector locally
    connector.debug(configuration=configuration)
