"""This connector demonstrates how to fetch data from Apache Pulsar topics and sync it to a destination using the Fivetran Connector SDK.
It supports multiple topics and uses Pulsar's reader API to consume messages with proper checkpointing for incremental syncs.
See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

# For reading configuration from a JSON file
import base64
import json

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# Import Apache Pulsar client library for connecting to Pulsar clusters and consuming messages
import pulsar
from pulsar import MessageId

# For handling timestamps and data parsing
from datetime import datetime

# Maximum number of messages to process per topic per sync (prevents memory overflow)
_MAX_MESSAGES_PER_TOPIC = 1000

# Timeout for reading messages from Pulsar (in milliseconds)
_READ_TIMEOUT_MS = 5000

# Checkpoint interval in number of messages
_CHECKPOINT_INTERVAL = 100

# Default partition index for non-partitioned topics
_DEFAULT_PARTITION_INDEX = -1


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.

    Args:
        configuration: A dictionary that holds the configuration settings for the connector.

    Raises:
        ValueError: If any required configuration parameter is missing.
    """
    # Validate required configuration parameters
    required_configs = ["service_url", "tenant", "namespace", "topics", "subscription_name"]

    for key in required_configs:
        if key not in configuration:
            raise ValueError(f"Missing required configuration value: {key}")

    # Validate that topics is either a string or a list
    topics = configuration["topics"]
    if isinstance(topics, str):
        # Parse comma-separated string
        if not topics.strip():
            raise ValueError("'topics' must be a non-empty string or list of topic names")
    elif isinstance(topics, list):
        if len(topics) == 0:
            raise ValueError("'topics' must be a non-empty list of topic names")
    else:
        raise ValueError("'topics' must be a string (comma-separated) or a list of topic names")


def parse_topics(configuration: dict) -> list:
    """
    Parse topics from configuration. Handles both string (comma-separated) and list formats.

    Args:
        configuration: Configuration dictionary

    Returns:
        List of topic names
    """
    topics = configuration.get("topics", [])

    if isinstance(topics, str):
        # Parse comma-separated string
        return [t.strip() for t in topics.split(",") if t.strip()]
    elif isinstance(topics, list):
        return topics
    else:
        return []


def schema(configuration: dict):
    """
    Define the schema for the connector.
    Creates a separate table for each Pulsar topic with a unified schema.

    Args:
        configuration: A dictionary that holds the configuration settings for the connector.

    Returns:
        A list of table schemas
    """
    topics = parse_topics(configuration)

    # Create a table schema for each topic
    schemas = []

    for topic in topics:
        # Normalize topic name for table name (replace hyphens and dots with underscores)
        table_name = topic.replace("-", "_").replace(".", "_").lower()

        schemas.append(
            {
                "table": table_name,  # Table name based on topic
                "primary_key": ["message_id"],  # Pulsar message ID as primary key
                "columns": {
                    "message_id": "STRING",  # Unique message identifier from Pulsar
                    "topic": "STRING",  # Source topic name
                    "publish_time": "UTC_DATETIME",  # When the message was published
                    "event_time": "UTC_DATETIME",  # Event timestamp (if present in message)
                    "message_key": "STRING",  # Message key (optional)
                    "data": "JSON",  # The actual message payload as JSON
                    "properties": "JSON",  # Message properties/metadata
                    "producer_name": "STRING",  # Name of the producer
                    "sequence_id": "INT",  # Message sequence ID
                    "synced_at": "UTC_DATETIME",  # When this message was synced by Fivetran
                },
            }
        )

    return schemas


def update(configuration: dict, state: dict):
    """
    Define the update function, which is called by Fivetran during each sync.
    Reads messages from Apache Pulsar topics and upserts them into destination tables.

    Args:
        configuration: A dictionary containing connection details
        state: A dictionary containing state information from previous runs
               The state dictionary is empty for the first sync or for any full re-sync
    """
    log.warning("Example: Apache Pulsar Source Connector")

    # Validate the configuration
    validate_configuration(configuration=configuration)

    # Extract configuration parameters
    service_url = configuration.get("service_url")
    tenant = configuration.get("tenant")
    namespace = configuration.get("namespace")
    topics = parse_topics(configuration)
    subscription_name = configuration.get("subscription_name")

    # Optional authentication token for secured Pulsar clusters
    auth_token = configuration.get("auth_token")

    log.info(f"Connecting to Pulsar at {service_url}")
    log.info(f"Topics to sync: {', '.join(topics)}")

    try:
        # Create Pulsar client with optional authentication
        client_params = {"service_url": service_url}

        # Add authentication if token is provided
        if auth_token:
            client_params["authentication"] = pulsar.AuthenticationToken(auth_token)

        client = pulsar.Client(**client_params)
        log.info("✓ Connected to Pulsar successfully")

        # Process each topic
        for topic in topics:
            sync_topic(
                client=client,
                tenant=tenant,
                namespace=namespace,
                topic=topic,
                subscription_name=subscription_name,
                state=state,
            )

        # Close the Pulsar client
        client.close()

    except Exception as e:
        raise RuntimeError(f"Failed to sync data from Pulsar: {str(e)}")


def create_pulsar_reader(client, full_topic_name: str, last_message_id_bytes: str, topic: str):
    """
    Create a Pulsar reader configured to start from the appropriate position.

    Args:
        client: Pulsar client instance
        full_topic_name: Full Pulsar topic name (persistent://tenant/namespace/topic)
        last_message_id_bytes: Serialized last message ID from state, or None for initial sync
        topic: Topic name for logging

    Returns:
        Configured Pulsar reader instance
    """
    reader_config = {
        "topic": full_topic_name,
        "start_message_id": pulsar.MessageId.earliest,
    }

    if last_message_id_bytes:
        last_message_id = MessageId.deserialize(bytes.fromhex(last_message_id_bytes))
        reader_config["start_message_id"] = last_message_id
        log.info(f"Resuming from last checkpoint for topic {topic}")
    else:
        log.info(f"Starting initial sync from earliest message for topic {topic}")

    return client.create_reader(**reader_config)


def process_messages_from_reader(reader, table_name: str, topic: str, state: dict, state_key: str):
    """
    Read and process messages from a Pulsar reader with checkpointing.

    Args:
        reader: Pulsar reader instance
        table_name: Destination table name
        topic: Topic name for logging
        state: State dictionary for checkpointing
        state_key: Key to store checkpoint in state

    Returns:
        Tuple of (messages_processed, last_message_id)
    """
    messages_processed = 0
    last_message_id = None

    while messages_processed < _MAX_MESSAGES_PER_TOPIC:
        try:
            msg = reader.read_next(timeout_millis=_READ_TIMEOUT_MS)
            message_data = parse_message(msg, topic)

            # The 'upsert' operation is used to insert or update data in the destination table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into.
            # - The second argument is a dictionary containing the data to be upserted.
            op.upsert(table=table_name, data=message_data)

            last_message_id = msg.message_id()
            messages_processed += 1

            if messages_processed % _CHECKPOINT_INTERVAL == 0:
                serialized_id = last_message_id.serialize().hex()
                updated_state = {**state, state_key: serialized_id}
                # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                # from the correct position in case of next sync or interruptions.
                # Learn more about how and where to checkpoint by reading our best practices documentation
                # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
                op.checkpoint(updated_state)
                log.info(f"Checkpointed after {messages_processed} messages for topic {topic}")

        except Exception as e:
            if "Timeout" in str(e) or "timeout" in str(e).lower():
                break
            else:
                log.warning(f"Error processing message: {str(e)}")
                continue

    return messages_processed, last_message_id


def sync_topic(
    client, tenant: str, namespace: str, topic: str, subscription_name: str, state: dict
) -> dict:
    """
    Sync messages from a single Pulsar topic.

    Args:
        client: Pulsar client instance
        tenant: Pulsar tenant name
        namespace: Pulsar namespace name
        topic: Topic name to sync
        subscription_name: Subscription name for consuming messages
        state: State dictionary for checkpointing

    Returns:
        Updated state dictionary with this topic's checkpoint
    """
    # Construct full topic name
    full_topic_name = f"persistent://{tenant}/{namespace}/{topic}"

    # Normalize topic name for table name
    table_name = topic.replace("-", "_").replace(".", "_").lower()

    log.info(f"Syncing topic: {full_topic_name} -> table: {table_name}")

    # Get the last synced message ID from state (if exists)
    state_key = f"last_message_id_{topic}"
    last_message_id_bytes = state.get(state_key)

    try:
        # Create a reader starting from the last checkpointed position
        reader = create_pulsar_reader(client, full_topic_name, last_message_id_bytes, topic)

        # Process all messages from the reader with checkpointing
        messages_processed, last_message_id = process_messages_from_reader(
            reader, table_name, topic, state, state_key
        )

        # Final checkpoint after processing all messages
        if last_message_id:
            serialized_id = last_message_id.serialize().hex()
            updated_state = {**state, state_key: serialized_id}
            op.checkpoint(updated_state)
            state[state_key] = serialized_id

        log.info(f"✓ Synced {messages_processed} messages from topic {topic}")
        reader.close()

    except Exception as e:
        log.error(f"Error syncing topic {topic}: {str(e)}")
        raise


def parse_message(msg, topic: str) -> dict:
    """
    Parse a Pulsar message into a dictionary format suitable for upserting.

    Args:
        msg: Pulsar message object
        topic: Topic name

    Returns:
        A dictionary containing the parsed message data
    """
    # Get message ID (unique identifier)
    message_id = msg.message_id()
    # For non-partitioned topics, partition_index doesn't exist, use default
    partition_idx = getattr(message_id, "partition_index", lambda: _DEFAULT_PARTITION_INDEX)()
    message_id_str = f"{message_id.ledger_id()}:{message_id.entry_id()}:{partition_idx}"

    # Get publish time (when the message was published to Pulsar)
    publish_time = datetime.fromtimestamp(msg.publish_timestamp() / 1000.0).isoformat() + "Z"

    # Get event time (if set by producer, otherwise None)
    event_time = None
    if msg.event_timestamp() > 0:
        event_time = datetime.fromtimestamp(msg.event_timestamp() / 1000.0).isoformat() + "Z"

    # Parse message data as JSON
    try:
        data = json.loads(msg.data().decode("utf-8"))
    except json.JSONDecodeError:
        # If not JSON, store as string
        data = {"raw_data": msg.data().decode("utf-8")}
    except Exception:
        # If decoding fails, store as base64
        data = {"raw_data_base64": base64.b64encode(msg.data()).decode("utf-8")}

    # Get message properties (metadata)
    properties = msg.properties() if msg.properties() else {}

    # Construct the record
    record = {
        "message_id": message_id_str,
        "topic": topic,
        "publish_time": publish_time,
        "event_time": event_time,
        "message_key": msg.partition_key() if msg.partition_key() else None,
        "data": data,
        "properties": properties,
        "producer_name": msg.producer_name() if hasattr(msg, "producer_name") else None,
        "sequence_id": msg.sequence_id() if hasattr(msg, "sequence_id") else None,
        "synced_at": datetime.utcnow().isoformat() + "Z",
    }

    return record


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

# Main entry point for local testing
if __name__ == "__main__":
    # Open the configuration.json file and load its contents
    with open("configuration.json", "r") as f:
        configuration = json.load(f)

    # Test the connector locally
    connector.debug(configuration=configuration)
