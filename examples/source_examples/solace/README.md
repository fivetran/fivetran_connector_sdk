# Solace Connector for Fivetran

This connector fetches events from Solace messaging platform and syncs them to your Fivetran destination using the Fivetran Connector SDK. It supports both REST API and messaging API approaches for fetching events.

## Features

- **Incremental Sync**: Tracks the last processed event timestamp to only fetch new events
- **Dual API Support**: Can use either Solace REST API or messaging API
- **Event Deduplication**: Automatically removes duplicate events
- **Error Handling**: Robust error handling with retry logic
- **Configurable Batching**: Configurable batch sizes for optimal performance

## Configuration

### Required Parameters

- `solace_host`: Solace host address (e.g., "localhost:8080")
- `solace_username`: Username for Solace authentication
- `solace_password`: Password for Solace authentication

### Optional Parameters

- `solace_vpn`: VPN name (default: "default")
- `use_rest_api`: Whether to use REST API or messaging API (default: true)
- `solace_queue`: Queue name for messaging API (required if use_rest_api=false)
- `batch_size`: Number of events to fetch per batch (default: 1000)
- `timeout`: Request timeout in seconds (default: 30)

## Schema

The connector creates a single table called `solace_events` with the following columns:

- `event_id`: Unique identifier for the event
- `timestamp`: Event timestamp in UTC
- `topic`: Solace topic name
- `message_payload`: Raw message payload
- `message_type`: Type of message (extracted from payload)
- `correlation_id`: Correlation ID from message
- `user_properties`: JSON string of user properties
- `source_system`: Always "solace"
- `processed_at`: Timestamp when event was processed

## Usage

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure the Connector

Create a `configuration.json` file with your Solace credentials:

```json
{
    "solace_host": "your-solace-host:8080",
    "solace_username": "your-username",
    "solace_password": "your-password",
    "solace_vpn": "default",
    "use_rest_api": true,
    "solace_queue": "your-queue-name",
    "batch_size": 1000,
    "timeout": 30
}
```

### 3. Test the Connector

```bash
python solace_connector.py
```

### 4. Deploy to Fivetran

Follow the Fivetran Connector SDK deployment guide to deploy your connector.

## API Modes

### REST API Mode (Default)

When `use_rest_api` is `true`, the connector uses Solace's REST API to fetch events. This is suitable for:

- Historical event retrieval
- Event replay scenarios
- When you have REST API access

### Messaging API Mode

When `use_rest_api` is `false`, the connector uses Solace's messaging API to consume messages from a queue. This is suitable for:

- Real-time event processing
- Queue-based message consumption
- When you need to acknowledge messages

## Incremental Sync

The connector implements incremental sync by:

1. Storing the last processed event timestamp in the state
2. Only fetching events newer than the last sync time
3. Updating the state with the latest timestamp after successful sync

This ensures efficient syncing and prevents duplicate data.

## Error Handling

The connector includes comprehensive error handling:

- **Retry Logic**: Automatic retries with exponential backoff
- **Connection Management**: Proper connection cleanup
- **Message Acknowledgment**: Messages are acknowledged after successful processing
- **Logging**: Detailed logging for debugging and monitoring

## Development

### Local Testing

1. Set up a local Solace instance or use a test environment
2. Update `configuration.json` with your test credentials
3. Run `python solace_connector.py` to test locally

### Debugging

The connector includes extensive logging. Check the logs for:
- Connection status
- Event processing details
- Error messages
- Sync progress

## Troubleshooting

### Common Issues

1. **Connection Failed**: Check host, username, and password
2. **No Events Found**: Verify queue/topic configuration
3. **Authentication Error**: Ensure VPN name is correct
4. **Timeout Errors**: Increase timeout value in configuration

### Logs

The connector logs important events:
- Connection attempts and status
- Event fetching progress
- Error details with stack traces
- Sync completion status

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This connector is provided as-is for use with Fivetran Connector SDK.