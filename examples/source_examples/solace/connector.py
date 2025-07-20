import json
import time

from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op
from solace.messaging.messaging_service import MessagingService, RetryStrategy
from solace.messaging.resources.topic_subscription import TopicSubscription

from ServiceEventHandler import ServiceEventHandler
from MessageHandlerImpl import MessageHandlerImpl

TOPIC_PREFIX = "solace/samples/python"

def update(configuration: dict, state: dict):
    """
        Main update function that fetches and processes weather data for configured ZIP codes.

        Args:
            configuration (dict): Configuration parameters including ZIP codes to process
            state (dict): Current state of the connector, including the last sync time

        Yields:
            Operation: Fivetran operations (upsert, checkpoint) for data synchronization
    """
    # Broker Config
    broker_props = {
        "solace.messaging.transport.host": configuration.get("solace_host"),
        "solace.messaging.service.vpn-name": configuration.get("solace_vpn"),
        "solace.messaging.authentication.scheme.basic.username": configuration.get("solace_username"),
        "solace.messaging.authentication.scheme.basic.password": configuration.get("solace_password"),
    }

    # Build A messaging service with a reconnection strategy of 20 retries over an interval of 3 seconds
    # Note: The reconnections strategy could also be configured using the broker properties object
    messaging_service = MessagingService.builder().from_properties(broker_props) \
        .with_reconnection_retry_strategy(RetryStrategy.parametrized_retry(20, 3)) \
        .build()

    # Blocking connect thread
    messaging_service.connect()
    print(f'Messaging Service connected? {messaging_service.is_connected}')

    # Error Handling for the messaging service
    service_handler = ServiceEventHandler()
    messaging_service.add_reconnection_listener(service_handler)
    messaging_service.add_reconnection_attempt_listener(service_handler)
    messaging_service.add_service_interruption_listener(service_handler)

    # Define a Topic subscriptions
    topics = [TOPIC_PREFIX + "/>"]
    topics_sub = []
    for t in topics:
        topics_sub.append(TopicSubscription.of(t))

    # Build a Receiver with the given topics and start it
    direct_receiver = messaging_service.create_direct_message_receiver_builder() \
        .with_subscriptions(topics_sub) \
        .build()

    direct_receiver.start()
    print(f'Direct Receiver is running? {direct_receiver.is_running()}')

    try:
        print(f"Subscribing to: {topics}")
        # Callback for received messages
        direct_receiver.receive_async(MessageHandlerImpl())
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print('\nDisconnecting Messaging Service')
    finally:
        print('\nTerminating receiver')
        direct_receiver.terminate()
        print('\nDisconnecting Messaging Service')
        messaging_service.disconnect()

# This creates the connector object that will use the update and schema functions defined in this connector.py file.
connector = Connector(update=update)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    try:
        # Try loading the configuration from the file
        with open("configuration.json", "r") as f:
            configuration = json.load(f)
    except FileNotFoundError:
        # Fallback to an empty configuration if the file is not found
        configuration = {}
    # Allows testing the connector directly
    connector.debug(configuration=configuration)