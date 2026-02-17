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


async def _connect_temporal_client(
    temporal_host: str, temporal_namespace: str, temporal_api_key: str
):
    """Create and return a connected Temporal Cloud client."""
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


async def fetch_temporal_workflows(configuration: dict):
    """
    Async function to connect to Temporal Cloud and fetch workflow data.
    """
    workflows = []

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
            workflows.append(workflow_data)

            # Log progress every 100 workflows
            if workflow_count % 100 == 0:
                log.info(f"Processed {workflow_count} workflows")

        log.info(f"Successfully fetched {workflow_count} workflows from Temporal Cloud")

    except ValueError as ve:
        log.severe(f"Configuration error: {str(ve)}")
        raise
    except Exception as e:
        log.severe(f"Error connecting to Temporal Cloud: {str(e)}")
        raise

    return workflows


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


async def fetch_temporal_schedules(configuration: dict):
    """
    Async function to connect to Temporal Cloud and fetch schedule data.
    """
    schedules = []

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
            schedules.append(schedule_data)

            # Log progress every 100 schedules
            if schedule_count % 100 == 0:
                log.info(f"Processed {schedule_count} schedules")

        log.info(f"Successfully fetched {schedule_count} schedules from Temporal Cloud")

    except ValueError as ve:
        log.severe(f"Configuration error: {str(ve)}")
        raise
    except Exception as e:
        log.severe(f"Error fetching schedules from Temporal Cloud: {str(e)}")
        raise

    return schedules


def update(configuration: dict, state: dict):
    """
    Main sync function to fetch workflow and schedule data from Temporal Cloud and upsert to Fivetran.
    """
    log.info("Starting Temporal sync for workflows and schedules")

    try:
        # Fetch workflows using async function
        log.info("Fetching workflows...")
        workflows = asyncio.run(fetch_temporal_workflows(configuration))

        # Upsert each workflow
        for workflow in workflows:
            op.upsert(table="workflow", data=workflow)

        log.info(f"Successfully synced {len(workflows)} workflows")

        # Fetch schedules using async function
        log.info("Fetching schedules...")
        schedules = asyncio.run(fetch_temporal_schedules(configuration))

        # Upsert each schedule
        for schedule in schedules:
            op.upsert(table="schedule", data=schedule)

        log.info(f"Successfully synced {len(schedules)} schedules")

        # Update state with current sync timestamp
        current_timestamp = datetime.now(timezone.utc).isoformat()
        new_state = {"last_sync": current_timestamp}

        log.info(f"Sync completed: {len(workflows)} workflows, {len(schedules)} schedules")

        # Checkpoint with updated state
        op.checkpoint(state=new_state)

    except Exception as e:
        log.severe(f"Error during sync: {str(e)}")
        raise


# Connector declaration
connector = Connector(update=update, schema=schema)


if __name__ == "__main__":
    connector.debug()
