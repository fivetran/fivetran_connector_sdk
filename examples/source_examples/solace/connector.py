# This is a connector for fetching events from Solace using the Fivetran Connector SDK.
# It supports incremental sync by tracking the last processed event timestamp.
# The connector can work with Solace REST API or messaging APIs to fetch events.
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Operations as op
from fivetran_connector_sdk import Logging as log

# Import required libraries for Solace integration
import requests
import json
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Generator
import base64
from solace.messaging.messaging_service import MessagingService
from solace.messaging.receiver.inbound_message import InboundMessage
import pandas as pd


####################################################################################
# CONFIGURATION AND CONSTANTS
####################################################################################

DEFAULT_LAST_SYNC_DATE = datetime(2020, 1, 1, tzinfo=timezone.utc)
MAX_RETRIES = 3
MAX_BATCH_SIZE = 1000
DEFAULT_TIMEOUT = 30


####################################################################################
# SCHEMA DEFINITION
####################################################################################

def schema(configuration: dict):
    """
    Define the schema for the Solace events table.
    
    Args:
        configuration (dict): Configuration dictionary containing connector settings.
        
    Returns:
        list: List of table schema definitions.
    """
    # Validate required configuration
    required_keys = ["solace_host", "solace_username", "solace_password"]
    for key in required_keys:
        if key not in configuration:
            raise RuntimeError(f"Missing required configuration value: {key}")
    
    return [
        {
            "table": "solace_events",
            "primary_key": ["event_id", "timestamp"],
            "columns": {
                "event_id": "STRING",
                "timestamp": "UTC_DATETIME",
                "topic": "STRING",
                "message_payload": "STRING",
                "message_type": "STRING",
                "correlation_id": "STRING",
                "user_properties": "STRING",
                "source_system": "STRING",
                "processed_at": "UTC_DATETIME"
            }
        }
    ]


####################################################################################
# SOLACE AUTHENTICATION AND CONNECTION
####################################################################################

class SolaceAuth:
    """Handles authentication and connection to Solace messaging service."""
    
    def __init__(self, host: str, username: str, password: str, vpn_name: str = "default"):
        self.host = host
        self.username = username
        self.password = password
        self.vpn_name = vpn_name
        self.messaging_service = None
        
    def get_messaging_service(self) -> MessagingService:
        """Get or create a messaging service connection."""
        if self.messaging_service is None:
            try:
                # Create messaging service
                self.messaging_service = MessagingService.builder() \
                    .from_properties({
                        "solace.messaging.service.transport.host": self.host,
                        "solace.messaging.service.username": self.username,
                        "solace.messaging.service.password": self.password,
                        "solace.messaging.service.vpn-name": self.vpn_name,
                        "solace.messaging.service.ssl.trust-store": None,
                        "solace.messaging.service.ssl.validate-certificate": False
                    }) \
                    .build()
                
                # Connect to the messaging service
                self.messaging_service.connect()
                log.info(f"Successfully connected to Solace at {self.host}")
                
            except Exception as e:
                log.error(f"Failed to connect to Solace: {e}")
                raise RuntimeError(f"Solace connection failed: {e}")
        
        return self.messaging_service
    
    def get_rest_headers(self) -> Dict[str, str]:
        """Get headers for REST API calls."""
        credentials = f"{self.username}:{self.password}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()
        return {
            "Authorization": f"Basic {encoded_credentials}",
            "Content-Type": "application/json"
        }


####################################################################################
# EVENT FETCHING AND PROCESSING
####################################################################################

def fetch_events_rest(config: dict, last_sync_time: datetime, batch_size: int = MAX_BATCH_SIZE) -> List[Dict]:
    """
    Fetch events from Solace using REST API.
    
    Args:
        config (dict): Configuration dictionary
        last_sync_time (datetime): Last sync timestamp for incremental sync
        batch_size (int): Number of events to fetch per batch
        
    Returns:
        List[Dict]: List of event records
    """
    method_name = "fetch_events_rest"
    
    # Initialize authentication
    auth = SolaceAuth(
        host=config["solace_host"],
        username=config["solace_username"],
        password=config["solace_password"],
        vpn_name=config.get("solace_vpn", "default")
    )
    
    headers = auth.get_rest_headers()
    base_url = f"https://{config['solace_host']}/SEMP/v2"
    
    # Build query parameters for incremental sync
    params = {
        "count": batch_size,
        "select": "eventId,timestamp,topic,messagePayload,messageType,correlationId,userProperties"
    }
    
    # Add time filter for incremental sync
    if last_sync_time:
        params["where"] = f"timestamp > '{last_sync_time.isoformat()}'"
    
    events = []
    retries = MAX_RETRIES
    
    while retries > 0:
        try:
            log.info(f"{method_name}: Fetching events from Solace REST API (attempt {MAX_RETRIES - retries + 1})")
            
            response = requests.get(
                f"{base_url}/events",
                headers=headers,
                params=params,
                timeout=DEFAULT_TIMEOUT
            )
            
            if response.status_code == 200:
                data = response.json()
                events = data.get("data", [])
                log.info(f"{method_name}: Fetched {len(events)} events from REST API")
                break
            else:
                log.warning(f"{method_name}: REST API returned status {response.status_code}: {response.text}")
                response.raise_for_status()
                
        except Exception as e:
            log.error(f"{method_name}: Error fetching events via REST API: {e}")
            retries -= 1
            if retries > 0:
                time.sleep(2 ** (MAX_RETRIES - retries))  # Exponential backoff
    
    if retries == 0:
        log.error(f"{method_name}: Failed to fetch events after {MAX_RETRIES} attempts")
        return []
    
    return events


def fetch_events_messaging(config: dict, last_sync_time: datetime, batch_size: int = MAX_BATCH_SIZE) -> List[Dict]:
    """
    Fetch events from Solace using messaging API (queue/topic subscription).
    
    Args:
        config (dict): Configuration dictionary
        last_sync_time (datetime): Last sync timestamp for incremental sync
        batch_size (int): Number of events to fetch per batch
        
    Returns:
        List[Dict]: List of event records
    """
    method_name = "fetch_events_messaging"
    
    # Initialize authentication
    auth = SolaceAuth(
        host=config["solace_host"],
        username=config["solace_username"],
        password=config["solace_password"],
        vpn_name=config.get("solace_vpn", "default")
    )
    
    messaging_service = auth.get_messaging_service()
    events = []
    
    try:
        # Get queue name from config
        queue_name = config.get("solace_queue", "default_queue")
        
        # Create message receiver
        receiver = messaging_service.create_queue_message_receiver_builder() \
            .with_queue_name(queue_name) \
            .build()
        
        # Start receiving messages
        receiver.start()
        log.info(f"{method_name}: Started receiving messages from queue: {queue_name}")
        
        # Collect messages up to batch size
        message_count = 0
        timeout = time.time() + 30  # 30 second timeout
        
        while message_count < batch_size and time.time() < timeout:
            try:
                message = receiver.receive_message(timeout=1000)  # 1 second timeout
                if message:
                    event_record = process_message(message, last_sync_time)
                    if event_record:
                        events.append(event_record)
                        message_count += 1
                        
                        # Acknowledge the message
                        message.ack()
                else:
                    # No more messages available
                    break
                    
            except Exception as e:
                log.warning(f"{method_name}: Error processing message: {e}")
                continue
        
        log.info(f"{method_name}: Fetched {len(events)} events from messaging API")
        
    except Exception as e:
        log.error(f"{method_name}: Error fetching events via messaging API: {e}")
    finally:
        if 'receiver' in locals():
            receiver.stop()
    
    return events


def process_message(message: InboundMessage, last_sync_time: datetime) -> Optional[Dict]:
    """
    Process a Solace message into an event record.
    
    Args:
        message (InboundMessage): Solace message object
        last_sync_time (datetime): Last sync timestamp for filtering
        
    Returns:
        Optional[Dict]: Processed event record or None if filtered out
    """
    try:
        # Extract message properties
        payload = message.get_payload_as_string()
        topic = message.get_destination_name()
        timestamp = datetime.now(timezone.utc)
        
        # Check if message is newer than last sync time
        if last_sync_time and timestamp <= last_sync_time:
            return None
        
        # Parse payload as JSON if possible
        try:
            payload_json = json.loads(payload)
            message_type = payload_json.get("type", "unknown")
            correlation_id = payload_json.get("correlation_id", "")
        except (json.JSONDecodeError, TypeError):
            message_type = "raw"
            correlation_id = ""
        
        # Extract user properties
        user_properties = {}
        if hasattr(message, 'get_application_message_id'):
            user_properties["application_message_id"] = message.get_application_message_id()
        if hasattr(message, 'get_correlation_id'):
            user_properties["correlation_id"] = message.get_correlation_id()
        
        event_record = {
            "event_id": f"{topic}_{timestamp.timestamp()}_{hash(payload) % 1000000}",
            "timestamp": timestamp.isoformat(),
            "topic": topic,
            "message_payload": payload,
            "message_type": message_type,
            "correlation_id": correlation_id,
            "user_properties": json.dumps(user_properties),
            "source_system": "solace",
            "processed_at": datetime.now(timezone.utc).isoformat()
        }
        
        return event_record
        
    except Exception as e:
        log.error(f"Error processing message: {e}")
        return None


def clean_and_deduplicate_events(events: List[Dict]) -> List[Dict]:
    """
    Clean and deduplicate event records.
    
    Args:
        events (List[Dict]): Raw event records
        
    Returns:
        List[Dict]: Cleaned and deduplicated events
    """
    if not events:
        return []
    
    # Convert to DataFrame for easier processing
    df = pd.DataFrame(events)
    
    # Remove duplicates based on event_id
    df = df.drop_duplicates(subset=["event_id"], keep="first")
    
    # Sort by timestamp
    df = df.sort_values("timestamp")
    
    # Convert back to list of dictionaries
    return df.to_dict("records")


####################################################################################
# MAIN SYNC FUNCTIONS
####################################################################################

def sync_events(config: dict, state: dict) -> Generator:
    """
    Main function to sync events from Solace.
    
    Args:
        config (dict): Configuration dictionary
        state (dict): State dictionary for incremental sync
        
    Yields:
        Generator: Operations for upserting events and checkpointing state
    """
    method_name = "sync_events"
    
    # Get last sync time from state
    last_sync_time_str = state.get("last_sync_time")
    if last_sync_time_str:
        last_sync_time = datetime.fromisoformat(last_sync_time_str)
    else:
        last_sync_time = DEFAULT_LAST_SYNC_DATE
    
    log.info(f"{method_name}: Starting sync from {last_sync_time}")
    
    # Determine which API to use
    use_rest_api = config.get("use_rest_api", True)
    
    try:
        if use_rest_api:
            events = fetch_events_rest(config, last_sync_time)
        else:
            events = fetch_events_messaging(config, last_sync_time)
        
        # Clean and deduplicate events
        events = clean_and_deduplicate_events(events)
        
        if not events:
            log.info(f"{method_name}: No new events found")
            yield op.checkpoint(state)
            return
        
        log.info(f"{method_name}: Processing {len(events)} events")
        
        # Upsert events
        for event in events:
            try:
                yield op.upsert(table="solace_events", data=event)
            except Exception as e:
                log.error(f"{method_name}: Error upserting event {event.get('event_id')}: {e}")
        
        # Update state with latest timestamp
        latest_timestamp = max(event["timestamp"] for event in events)
        state["last_sync_time"] = latest_timestamp
        
        log.info(f"{method_name}: Successfully processed {len(events)} events")
        
    except Exception as e:
        log.error(f"{method_name}: Error during sync: {e}")
        raise
    
    # Checkpoint state
    yield op.checkpoint(state)


def update(configuration: dict, state: dict):
    """
    Main update function called by Fivetran during each sync.
    
    Args:
        configuration (dict): Configuration dictionary containing connector settings
        state (dict): State dictionary for incremental sync
        
    Yields:
        Generator: Operations for syncing events
    """
    method_name = "update"
    
    # Validate configuration
    required_keys = ["solace_host", "solace_username", "solace_password"]
    for key in required_keys:
        if key not in configuration:
            log.error(f"{method_name}: Missing required configuration key: {key}")
            raise ValueError(f"Missing configuration key: {key}")
    
    log.info(f"{method_name}: Starting Solace connector sync")
    
    # Sync events
    yield from sync_events(configuration, state)
    
    log.info(f"{method_name}: Solace connector sync completed")


####################################################################################
# CONNECTOR INSTANCE
####################################################################################

# Create the connector object
connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    # For local testing
    import os
    
    # Load configuration from file if it exists
    config_file = "configuration.json"
    if os.path.exists(config_file):
        with open(config_file, "r") as f:
            configuration = json.load(f)
    else:
        # Default test configuration
        configuration = {
            "solace_host": "localhost:8080",
            "solace_username": "test_user",
            "solace_password": "test_password",
            "solace_vpn": "default",
            "use_rest_api": True,
            "solace_queue": "test_queue"
        }
    
    # Run connector in debug mode
    connector.debug(configuration=configuration)