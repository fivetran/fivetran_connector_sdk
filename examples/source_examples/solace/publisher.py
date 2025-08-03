from solace.messaging.messaging_service import MessagingService
from solace.messaging.resources.topic import Topic
import time

try:
    service = MessagingService.builder().from_properties({
        "solace.messaging.transport.host": "localhost:55554",  # Try without tcp:// prefix
        "solace.messaging.service.vpn-name": "default",
        "solace.messaging.authentication.basic.username": "admin",
        "solace.messaging.authentication.basic.password": "admin"
    }).build()

    service.connect()
    print("Connected to Solace broker.")

    publisher = service.create_direct_message_publisher_builder().build()
    publisher.start()
    print("Publisher started.")

    topic = Topic.of("demo/topic")
    message = '{"event_timestamp": "2025-08-03T12:00:00Z", "type": "test"}'
    publisher.publish(message, topic)
    print(f"Published message to topic {topic}")  # Use str(topic), not topic.name

    time.sleep(1)

except Exception as e:
    print(f"Error: {e}")
