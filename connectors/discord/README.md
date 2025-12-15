# Discord API Connector

## Connector overview

This Discord API connector enables you to extract comprehensive data from Discord servers (guilds) using the Discord API. The connector fetches server information, channels, members, and messages, making it ideal for community analytics, moderation insights, and engagement tracking.

The connector supports both full and incremental syncs, with intelligent rate limiting and error handling to ensure reliable data extraction from Discord's API. It's particularly well-suited for AI/ML applications that need to analyze community interactions, sentiment analysis, and user behavior patterns.

## Requirements

- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)   
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

- Multi-guild support: Automatically discovers and processes all guilds the bot has access to (Refer to the `fetch_user_guilds` function)
- Guild filtering: Configure which guilds to sync with include/exclude lists (Refer to the `filter_guilds` function)
- Guild data: Complete server information including settings, features, and metadata
- Channel management: All channel types (text, voice, category, etc.) with permissions and settings
- Member analytics: User profiles, roles, join dates, and activity status
- Message history: Comprehensive message data with attachments, embeds, and reactions
- Incremental sync: Efficient updates using message timestamps and state management (Refer to the `process_single_guild` function)
- Rate limit handling: Intelligent retry logic with exponential backoff
- Error recovery: Robust error handling with detailed logging
- AI/ML optimized: Structured data perfect for community analysis and sentiment tracking

## Configuration file

The connector requires the following configuration parameters in `configuration.json`:

```json
{
  "bot_token": "<YOUR_DISCORD_BOT_TOKEN>"
}
```

### Configuration parameters

- `bot_token` (required): Your Discord bot token (with or without the "bot" prefix - the connector adds it automatically if missing)
- `sync_all_guilds` (optional): Specifies whether to sync all guilds the bot has access to (default: "true")
- `guild_ids` (optional): Comma-separated list of specific guild IDs to sync (default: "" - sync all)
- `exclude_guild_ids` (optional): Comma-separated list of guild IDs to exclude (default: "" - exclude none)
- `sync_messages` (optional): Specifies whether to sync message data (default: "true")
- `message_limit` (optional): Maximum messages per channel to sync (default: "1000")

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file

The `requirements.txt` file specifies the Python libraries required by the connector. This connector uses only the standard library and pre-installed packages

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication

This connector uses Discord bot token authentication (Refer to `validate_configuration` function, and `_normalize_bot_token` function). To set up authentication:

1. Create a Discord application:
   - Go to [Discord Developer Portal](https://discord.com/developers/applications).
   - Click New Application and give it a name.
   - Navigate to the Bot section in the left sidebar.
2. Create a bot:
   - Click Add Bot if no bot exists.
   - Make a note of the bot token. 
3. Set bot permissions:
   - In the Bot section, scroll down to Privileged Gateway Intents.
   - Enable Server Members Intent (required for member data).
   - Enable Message Content Intent (required for message content).
4. Invite bot to server:
   - Go to OAuth2 > URL Generator.
   - Select bot scope.
   - Select the Read Messages, Read Message History, View Channels, and Read Server Members permissions:
   - Use the generated URL to invite the bot to your server.

## Pagination

The connector handles pagination automatically for all Discord API endpoints (Refer to `fetch_channel_messages` function):

- Channels: Fetches all channels in a single request
- Members: Uses Discord's member endpoint with 1000 member limit per request
- Messages: Implements cursor-based pagination using message IDs for efficient incremental syncs

The connector processes data in batches and checkpoints progress every 50 records to ensure reliable sync resumption.

## Data handling

The connector processes and normalizes Discord data for optimal analysis (Refer to `process_guild_data`, `process_channel_data`, `process_member_data`, and `process_message_data` functions):

- JSON Serialization: Complex objects (mentions, attachments, embeds) are stored as JSON strings
- Timestamp Normalization: All timestamps are converted to ISO format with UTC timezone
- Data Type Consistency: Ensures consistent data types across all records
- Null Handling: Gracefully handles missing or null values from the API
- Schema Evolution: Automatically adapts to new Discord API fields

### Data Processing Pipeline

1. Fetch: Retrieve data from Discord API with rate limit handling
2. Normalize: Convert Discord data format to Fivetran schema
3. Validate: Ensure data integrity and required fields
4. Upsert: Insert or update records in destination tables
5. Checkpoint: Save sync state for incremental updates

## Error handling

The connector implements comprehensive error handling strategies (Refer to `make_discord_request` function):

- Rate Limiting: Automatic retry with exponential backoff when rate limited
- Server Errors: Retry logic for 5xx HTTP status codes
- Network Issues: Timeout handling and connection error recovery
- Data Validation: Graceful handling of malformed API responses
- State Recovery: Checkpoint-based state management for sync resumption

### Error Types Handled

- 429 Rate Limited: Waits for retry-after header duration
- 500-504 Server Errors: Exponential backoff retry strategy
- Network Timeouts: 30-second timeout with retry logic
- Invalid Responses: JSON parsing error handling
- Missing Permissions: Clear error messages for authentication issues

## Tables created

The connector creates four main tables with the following structure:

### GUILD
Primary key: `id`
Contains complete Discord server information including settings, features, member counts, and metadata.

### CHANNEL  
Primary key: `id`
All channel types (text, voice, category, etc.) with permissions, settings, and configuration.

### MEMBER
Primary key: `user_id` and `guild_id` (composite primary key)
User profiles with roles, join dates, activity status, and server-specific information.

### MESSAGE
Primary key: `id`
Complete message data including content, attachments, embeds, reactions, and metadata.

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.

### Discord API Considerations

- Rate Limits: Discord has strict rate limits; the connector implements intelligent retry logic
- Permissions: Ensure your bot has necessary intents and permissions enabled
- Data Volume: Large servers may require multiple sync runs for complete data extraction
- Message History: Discord API limits message history; consider your message_limit setting
- Privacy: Be mindful of Discord's Terms of Service and data privacy requirements

### Performance Optimization

- Batch Processing: Data is processed in configurable batches for optimal performance
- Incremental Sync: Only new/updated data is fetched on subsequent runs
- Memory Management: Large datasets are processed without loading everything into memory
- Checkpointing: Regular state saves ensure sync reliability and resumption

### AI/ML Use Cases

This connector is particularly well-suited for:
- Community Analytics: Server growth, engagement patterns, and activity trends
- Sentiment Analysis: Message content analysis and user sentiment tracking
- Moderation Insights: Automated moderation and content policy analysis
- User Behavior: Member activity patterns and interaction analysis
- Content Analysis: Attachment and embed analysis for content categorization
