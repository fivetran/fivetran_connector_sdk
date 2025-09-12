# Solace Python API for messaging service
from solace.messaging.messaging_service import MessagingService

# Solace topic resource for publishing messages
from solace.messaging.resources.topic import Topic

# For timestamping messages with timezone-aware datetimes
from datetime import datetime, timezone

# Fivetran logging utility
from fivetran_connector_sdk import Logging as log

# For generating unique message IDs
import uuid

# For serializing message payloads
import json

# For adding delays between message publishes
import time


class SolacePublisher:
    """
    Publishes messages to a Solace broker using the Solace Messaging API.

    Attributes:
        host (str): Solace broker host address.
        vpn (str): Solace VPN name.
        username (str): Username for authentication.
        password (str): Password for authentication.
        topic_name (str): Name of the topic to publish messages to.
        messaging_service: Solace messaging service instance.
        publisher: Solace direct message publisher instance.
        topic: Solace topic resource.
    """

    def __init__(self, host: str, vpn: str, username: str, password: str, topic_name: str):
        """
        Initializes the SolacePublisher with connection and topic details.

        Args:
            host (str): Solace broker host address.
            vpn (str): Solace VPN name.
            username (str): Username for authentication.
            password (str): Password for authentication.
            topic_name (str): Name of the topic to publish messages to.
        """
        self.host = host
        self.vpn = vpn
        self.username = username
        self.password = password
        self.topic_name = topic_name
        self.messaging_service = None
        self.publisher = None
        self.topic = Topic.of(self.topic_name)

    def connect(self):
        """
        Establishes a connection to the Solace broker and starts the publisher.
        """
        self.messaging_service = (
            MessagingService.builder()
            .from_properties(
                {
                    "solace.messaging.transport.host": self.host,
                    "solace.messaging.service.vpn-name": self.vpn,
                    "solace.messaging.authentication.basic.username": self.username,
                    "solace.messaging.authentication.basic.password": self.password,
                }
            )
            .build()
        )
        self.messaging_service.connect()
        log.info("Connected to Solace broker.")

        self.publisher = self.messaging_service.create_direct_message_publisher_builder().build()
        self.publisher.start()
        log.info("Publisher started.")

    def publish_messages(self, number_of_messages_to_send=10, delay=0.5):
        """
        Publishes a specified number of test messages to the configured topic.

        Args:
            number_of_messages_to_send (int, optional): Number of messages to publish. Defaults to 10.
            delay (float, optional): Delay in seconds between messages. Defaults to 0.5.
        """
        for message_index in range(number_of_messages_to_send):
            event_data = {
                "message_id": str(uuid.uuid4()),
                "event_timestamp": datetime.now(timezone.utc).isoformat(),
                "type": "test_event",
                "details": "This is a generate message and the number is : " + str(message_index),
            }
            message = json.dumps(event_data)
            self.publisher.publish(message, self.topic)
            time.sleep(delay)
