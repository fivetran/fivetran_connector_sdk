from solace.messaging.messaging_service import MessagingService
from solace.messaging.resources.topic import Topic
from datetime import datetime, timezone
from fivetran_connector_sdk import Logging as log
import uuid
import json
import time


class SolacePublisher:
    def __init__(self, host: str, vpn: str, username: str, password: str, topic_name: str):
        self.host = host
        self.vpn = vpn
        self.username = username
        self.password = password
        self.topic_name = topic_name
        self.messaging_service = None
        self.publisher = None
        self.topic = Topic.of(self.topic_name)

    def connect(self):
        self.messaging_service = MessagingService.builder().from_properties({
            "solace.messaging.transport.host": self.host,
            "solace.messaging.service.vpn-name": self.vpn,
            "solace.messaging.authentication.basic.username": self.username,
            "solace.messaging.authentication.basic.password": self.password
        }).build()
        self.messaging_service.connect()
        log.info("Connected to Solace broker.")

        self.publisher = self.messaging_service.create_direct_message_publisher_builder().build()
        self.publisher.start()
        log.info("Publisher started.")

    def publish_messages(self, count=10, delay=0.5):
        for i in range(count):
            event_data = {
                "event_id": str(uuid.uuid4()),
                "event_timestamp": datetime.now(timezone.utc).isoformat(),
                "type": "test_event",
                "details": "This is a generate message and the number is : " + str(i)
            }
            message = json.dumps(event_data)
            self.publisher.publish(message, self.topic)
            log.info(f"Published message {i + 1}: {message}")
            time.sleep(delay)
