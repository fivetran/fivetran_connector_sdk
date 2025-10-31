"""
Fivetran Connector SDK for RabbitMQ
Consumes messages from RabbitMQ queues and syncs to Fivetran destination
Supports multiple queues, message acknowledgment, and checkpointing
"""

import pika
import json
import hashlib
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

from examples.quickstart_examples.oop_example.connector import validate_configuration


def validate_configuration(configuration: Dict[str, Any]):
    """
    Validate the configuration dictionary for required fields.
    Raises:
        ValueError: If any required field is missing or invalid.
    """
    if not configuration.get('host'):
        raise ValueError("host is required in configuration")

    if not configuration.get('username'):
        raise ValueError("username is required in configuration")

    if not configuration.get('password'):
        raise ValueError("password is required in configuration")

    if not configuration.get('queues'):
        raise ValueError("queues is required in configuration")

# Define the schema function, which lets you configure the schema your connector delivers.
# See the technical reference documentation for more details on the schema function:
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
# The schema function takes one parameter:
# - configuration: a dictionary that holds the configuration settings for the connector.
def schema(configuration: Dict[str, Any]) -> List[Dict[str, Any]]:
    # Validate required configuration
    validate_configuration(configuration)

    queues_str = configuration.get('queues', '')

    # Parse queue names
    queue_names = [q.strip() for q in queues_str.split(',') if q.strip()]

    log.info(f"Defining schema for {len(queue_names)} queues: {queue_names}")

    schema_response = []

    for queue_name in queue_names:
        # Create a table for each queue
        table_schema = {
            'table': queue_name,
            'primary_key': ['message_id'],
            'columns': {
                'message_id': "STRING",
                'delivery_tag': "LONG",
                'exchange': "STRING",
                'routing_key': "STRING",
                'timestamp': "UTC_DATETIME",
                'message_body': "JSON",
                'headers': "JSON",
                'content_type': "STRING",
                'content_encoding': "STRING",
                'delivery_mode': "INT",
                'priority': "INT",
                'correlation_id': "STRING",
                'reply_to': "STRING",
                'expiration': "STRING",
                'message_type': "STRING",
                'user_id': "STRING",
                'app_id': "STRING",
                'redelivered': "BOOLEAN",
                '_fivetran_synced': "UTC_DATETIME",
                '_fivetran_deleted': "BOOLEAN"
            }
        }

        schema_response.append(table_schema)

    log.info(f"Schema discovery complete: {len(schema_response)} tables defined")
    return schema_response

# Define the update function, which is a required function, and is called by Fivetran during each sync.
# See the technical reference documentation for more details on the update function
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
# The function takes two parameters:
# - configuration: a dictionary that contains any secrets or payloads you configure when deploying the connector
# - state: a dictionary that contains whatever state you have chosen to checkpoint during the prior sync.
# The state dictionary is empty for the first sync or for any full re-sync.
def update(configuration: Dict[str, Any], state: Dict[str, Any]):

    # Validate required configuration
    validate_configuration(configuration)

    host = configuration['host']
    port = int(configuration.get('port', 5672))
    virtual_host = configuration.get('virtual_host', '/')
    username = configuration['username']
    password = configuration['password']
    use_ssl = configuration.get('use_ssl', False)
    queues_str = configuration['queues']
    auto_create_queues = configuration.get('auto_create_queues', False)
    prefetch_count = 100
    batch_size = 1000
    auto_ack = configuration.get('auto_ack', False)
    message_ttl_hours = configuration.get('message_ttl_hours', 0)

    # Handle string conversions
    if isinstance(port, str):
        port = int(port)

    if isinstance(prefetch_count, str):
        prefetch_count = int(prefetch_count)

    if isinstance(batch_size, str):
        batch_size = int(batch_size)

    if isinstance(auto_ack, str):
        auto_ack = auto_ack.lower() in ('true', '1', 'yes')

    if isinstance(use_ssl, str):
        use_ssl = use_ssl.lower() in ('true', '1', 'yes')

    if isinstance(auto_create_queues, str):
        auto_create_queues = auto_create_queues.lower() in ('true', '1', 'yes')

    if isinstance(message_ttl_hours, str):
        message_ttl_hours = int(message_ttl_hours)

    # Parse queue names
    queue_names = [q.strip() for q in queues_str.split(',') if q.strip()]

    try:
        # Connect to RabbitMQ
        connection = get_connection(host, port, virtual_host, username, password, use_ssl)
        channel = connection.channel()

        # Set QoS
        channel.basic_qos(prefetch_count=prefetch_count)

        log.info(f"Connected to RabbitMQ, consuming from {len(queue_names)} queues")

        # Process each queue
        for queue_name in queue_names:
            log.info(f"Processing queue: {queue_name}")

            # Check if queue exists
            try:
                channel.queue_declare(queue=queue_name, passive=True)
                log.info(f"Queue '{queue_name}' exists")
            except Exception as e:
                if auto_create_queues:
                    log.warning(f"Queue '{queue_name}' not found, creating it")
                    try:
                        # Create queue with default settings
                        channel.queue_declare(
                            queue=queue_name,
                            durable=True,
                            exclusive=False,
                            auto_delete=False
                        )
                        log.info(f"Queue '{queue_name}' created successfully")
                    except Exception as create_error:
                        log.severe(f"Failed to create queue '{queue_name}': {create_error}")
                        continue
                else:
                    log.warning(f"Queue '{queue_name}' not found and auto_create_queues is disabled, skipping")
                    continue

            # Get queue state
            queue_state = state.get(queue_name, {})
            processed_messages = queue_state.get('processed_messages', set())
            if isinstance(processed_messages, list):
                processed_messages = set(processed_messages)

            # Consume messages from queue
            yield from consume_queue(
                channel, queue_name, batch_size, auto_ack,
                processed_messages, message_ttl_hours, state
            )

        # Close connection
        connection.close()
        log.info("RabbitMQ connection closed")

    except Exception as e:
        log.severe(f"Sync failed: {str(e)}")
        raise


def get_connection(host: str, port: int, virtual_host: str,
                   username: str, password: str, use_ssl: bool):
    """
    Create connection to RabbitMQ
    Args: param host: hostname or IP address of RabbitMQ server
          param port: port number of RabbitMQ server
          param virtual_host: virtual host to connect to
          param username: username for authentication
          param password: password for authentication
          param use_ssl: whether to use SSL/TLS for the connection
    Returns: pika.BlockingConnection: connection object to RabbitMQ
    """
    log.info(f"Connecting to RabbitMQ at {host}:{port}, vhost: {virtual_host}")

    try:
        # Create credentials
        credentials = pika.PlainCredentials(username, password)

        # Create connection parameters
        if use_ssl:
            import ssl

            # Create SSL context
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE

            # Create SSL options
            ssl_options = pika.SSLOptions(ssl_context)

            parameters = pika.ConnectionParameters(
                host=host,
                port=port,
                virtual_host=virtual_host,
                credentials=credentials,
                ssl_options=ssl_options
            )
        else:
            parameters = pika.ConnectionParameters(
                host=host,
                port=port,
                virtual_host=virtual_host,
                credentials=credentials
            )

        # Create connection
        connection = pika.BlockingConnection(parameters)
        log.info("Successfully connected to RabbitMQ")

        return connection

    except Exception as e:
        log.severe(f"Failed to connect to RabbitMQ: {str(e)}")
        raise


def consume_queue(channel, queue_name: str, batch_size: int, auto_ack: bool,
                  processed_messages: set, message_ttl_hours: int, state: Dict[str, Any]):
    """
    Consume messages from a RabbitMQ queue
    Args: param channel: pika channel object
    param queue_name: name of the RabbitMQ queue
    param batch_size: maximum number of messages to process in this sync
    param auto_ack: whether to automatically acknowledge messages
    param processed_messages: set of message IDs already processed
    param message_ttl_hours: message time-to-live in hours
    param state: current state dictionary
    Returns: generator yielding upsert and checkpoint operations
    """
    try:
        messages_processed = 0
        sync_time = datetime.now(timezone.utc)
        new_processed_messages = processed_messages.copy()

        # Consume messages
        for method_frame, properties, body in channel.consume(
                queue=queue_name,
                auto_ack=auto_ack,
                inactivity_timeout=5
        ):
            # Check for timeout (no more messages)
            if method_frame is None:
                log.info(f"No more messages in queue: {queue_name}")
                break

            try:
                # Parse message
                message_data = parse_message(
                    method_frame, properties, body, sync_time
                )

                message_id = message_data['message_id']

                # Check if already processed
                if message_id in processed_messages:
                    log.info(f"Message {message_id} already processed, skipping")
                    if not auto_ack:
                        channel.basic_ack(delivery_tag=method_frame.delivery_tag)
                    continue

                # Check message TTL
                if message_ttl_hours > 0:
                    message_age = sync_time - datetime.fromisoformat(message_data['timestamp'])
                    if message_age.total_seconds() > (message_ttl_hours * 3600):
                        log.info(f"Message {message_id} expired (age: {message_age}), skipping")
                        if not auto_ack:
                            channel.basic_ack(delivery_tag=method_frame.delivery_tag)
                        continue

                # The 'upsert' operation inserts the data into the destination.
                # The op.upsert method is called with two arguments:
                # - The first argument is the name of the table to upsert the data into, in this case, "timestamps".
                # - The second argument is a dictionary containing the data to be upserted.
                yield op.upsert(table=queue_name, data=message_data)

                # Mark as processed
                new_processed_messages.add(message_id)
                messages_processed += 1

                # Acknowledge message
                if not auto_ack:
                    channel.basic_ack(delivery_tag=method_frame.delivery_tag)

                # Check batch size
                if messages_processed >= batch_size:
                    log.info(f"Reached batch size limit ({batch_size}), stopping")
                    break

            except Exception as e:
                log.warning(f"Failed to process message: {e}")
                # Reject message (requeue it)
                if not auto_ack:
                    channel.basic_nack(delivery_tag=method_frame.delivery_tag, requeue=True)
                continue

        # Cancel consumer
        channel.cancel()

        log.info(f"Processed {messages_processed} messages from {queue_name}")

        # Checkpoint state
        new_state = {
            queue_name: {
                'processed_messages': list(new_processed_messages)[-10000:],  # Keep last 10k message IDs
                'last_sync': sync_time.isoformat(),
                'total_messages_processed': len(new_processed_messages)
            }
        }

        if state is None:
            state = {}
        if queue_name not in state:
            state[queue_name] = {}
        state[queue_name].update(new_state[queue_name])

        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        yield op.checkpoint(state=state)

    except Exception as e:
        log.severe(f"Failed to consume from queue {queue_name}: {e}")
        raise


def parse_message(method_frame, properties, body, sync_time: datetime) -> Dict[str, Any]:
    """
    Parse RabbitMQ message into dictionary
    Args: param method_frame: pika method frame
    param properties: pika basic properties
    param body: message body bytes
    param sync_time: current sync time
    Returns: dictionary representing the message
    """

    # Generate message ID
    message_id = generate_message_id(method_frame, properties, body)

    # Parse message body
    message_body = parse_body(body, properties.content_type)

    # Extract timestamp
    timestamp = sync_time
    if properties.timestamp:
        timestamp = datetime.fromtimestamp(properties.timestamp, tz=timezone.utc)

    # Build message data
    message_data = {
        'message_id': message_id,
        'delivery_tag': method_frame.delivery_tag,
        'exchange': method_frame.exchange or '',
        'routing_key': method_frame.routing_key or '',
        'timestamp': timestamp.isoformat(),
        'message_body': message_body,
        'headers': serialize_headers(properties.headers),
        'content_type': properties.content_type or '',
        'content_encoding': properties.content_encoding or '',
        'delivery_mode': properties.delivery_mode or 1,
        'priority': properties.priority or 0,
        'correlation_id': properties.correlation_id or '',
        'reply_to': properties.reply_to or '',
        'expiration': properties.expiration or '',
        'message_type': properties.type or '',
        'user_id': properties.user_id or '',
        'app_id': properties.app_id or '',
        'redelivered': method_frame.redelivered,
        '_fivetran_synced': sync_time.isoformat(),
        '_fivetran_deleted': False
    }

    return message_data


def generate_message_id(method_frame, properties, body) -> str:
    """
    Generate unique message ID
    Args: param method_frame: pika method frame
          param properties: pika basic properties
          param body: message body bytes
    Returns: unique message ID string
    """

    # Use message_id if available
    if properties.message_id:
        return properties.message_id

    # Otherwise, generate from content
    content = f"{method_frame.delivery_tag}:{method_frame.routing_key}:{body}"
    return hashlib.md5(content.encode()).hexdigest()


def parse_body(body: bytes, content_type: Optional[str]) -> Any:
    """
    Parse message body based on content type
    Args: param body: message body bytes
          param content_type: content type string
    """
    try:
        # Decode body
        body_str = body.decode('utf-8')

        # Try to parse as JSON
        if not content_type or 'json' in content_type.lower():
            try:
                return json.loads(body_str)
            except json.JSONDecodeError:
                pass

        # Return as string
        return body_str

    except UnicodeDecodeError:
        # Return hex string for binary data
        return body.hex()


def serialize_headers(headers: Optional[Dict]) -> str:
    """
    Serialize headers dictionary to JSON string
    Args: param headers: headers dictionary
    Returns: JSON string representation of headers
    """
    if not headers:
        return '{}'

    try:
        # Convert bytes values to strings
        serialized = {}
        for key, value in headers.items():
            if isinstance(value, bytes):
                serialized[key] = value.decode('utf-8')
            else:
                serialized[key] = value

        return json.dumps(serialized)
    except Exception:
        return '{}'


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    try:
        # Open the configuration.json file and load its contents
        with open("configuration.json", "r") as f:
            configuration = json.load(f)
    except FileNotFoundError:
        log.info("Using empty configuration!")
        configuration = {}

    # Test the connector locally
    connector.debug(configuration=configuration)