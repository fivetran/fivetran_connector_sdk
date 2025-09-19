"""Discord API Connector for Fivetran.

This connector demonstrates how to fetch Discord server data including guilds,
channels, messages, and users using the Discord API.

See the Technical Reference documentation:
https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update

And the Best Practices documentation:
https://fivetran.com/docs/connectors/connector-sdk/best-practices
"""

# For reading configuration from a JSON file
import json

# For making HTTP requests to Discord API
import requests

# For handling time-based operations and rate limiting
import time

# For handling datetime operations
from datetime import datetime, timezone
from typing import Optional, Dict, List, Any

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and
# checkpoint()
from fivetran_connector_sdk import Operations as op


# Constants for API configuration and rate limiting
# USERS CAN MODIFY: Adjust these values based on your Discord API usage
# patterns and data volume
__MAX_RETRIES = 3  # Maximum number of retry attempts for API requests
__BASE_DELAY = 1  # Base delay in seconds for API request retries
__RATE_LIMIT_DELAY = 1  # Additional delay when rate limited
__BATCH_SIZE = 100  # Number of records to process in each batch
__CHECKPOINT_INTERVAL = 50  # Checkpoint every N records for large datasets

# Discord API base URL
__DISCORD_API_BASE = "https://discord.com/api/v10"


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.

    This function is called at the start of the update method to ensure that the
    connector has all necessary configuration values.

    Args:
        configuration: a dictionary that holds the configuration settings for the
            connector.

    Raises:
        ValueError: if any required configuration parameter is missing.
    """
    # Validate required configuration parameters
    required_configs = ["bot_token"]
    for key in required_configs:
        if key not in configuration:
            raise ValueError(f"Missing required configuration value: {key}")

    # Validate and normalize bot token format
    bot_token = configuration.get("bot_token", "")
    if not bot_token:
        raise ValueError("Bot token cannot be empty")

    # Check if token already has proper prefix
    if not bot_token.startswith(("Bot ", "Bearer ")):
        log.info("Bot token missing prefix, adding 'Bot ' prefix automatically")
        configuration["bot_token"] = f"Bot {bot_token}"
    else:
        log.info("Bot token already has proper prefix")

    # Set default values for optional parameters
    configuration.setdefault("sync_all_guilds", "true")
    configuration.setdefault("guild_ids", "")
    configuration.setdefault("exclude_guild_ids", "")
    configuration.setdefault("sync_messages", "true")
    configuration.setdefault("message_limit", "1000")


def get_discord_headers(bot_token: str) -> dict:
    """
    Create headers for Discord API requests.
    Args:
        bot_token: The Discord bot token for authentication
    Returns:
        dict: Headers dictionary for API requests
    """
    return {
        "Authorization": bot_token,
        "Content-Type": "application/json",
        "User-Agent": "Fivetran-Discord-Connector/1.0",
    }


def make_discord_request(
    url: str, headers: dict, params: Optional[dict] = None
) -> dict:
    """
    Make a request to the Discord API with retry logic and rate limit handling.
    Args:
        url: The Discord API endpoint URL
        headers: Headers for the request
        params: Query parameters for the request
    Returns:
        dict: JSON response from the API
    Raises:
        RuntimeError: If the API request fails after all retry attempts
    """
    for attempt in range(__MAX_RETRIES):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)

            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:  # Rate limited
                retry_after = float(
                    response.headers.get("Retry-After", __RATE_LIMIT_DELAY)
                )
                log.warning(
                    f"Rate limited. Waiting {retry_after} seconds before retry "
                    f"(attempt {attempt + 1}/{__MAX_RETRIES})"
                )
                time.sleep(retry_after)
                continue
            elif response.status_code in [500, 502, 503, 504]:  # Server errors
                if attempt < __MAX_RETRIES - 1:
                    delay = __BASE_DELAY * (2**attempt)  # Exponential backoff
                    log.warning(
                        f"Server error {response.status_code}, retrying in {delay} "
                        f"seconds (attempt {attempt + 1}/{__MAX_RETRIES})"
                    )
                    time.sleep(delay)
                    continue
                else:
                    log.severe(
                        f"Server error {response.status_code} after "
                        f"{__MAX_RETRIES} attempts: {response.text}"
                    )
                    raise RuntimeError(
                        f"Discord API returned {response.status_code} after "
                        f"{__MAX_RETRIES} attempts"
                    )
            else:
                log.severe(f"Discord API error {response.status_code}: {response.text}")
                raise RuntimeError(
                    f"Discord API returned {response.status_code}: {response.text}"
                )

        except requests.exceptions.RequestException as e:
            if attempt < __MAX_RETRIES - 1:
                delay = __BASE_DELAY * (2**attempt)
                log.warning(
                    f"Request failed: {str(e)}, retrying in {delay} seconds "
                    f"(attempt {attempt + 1}/{__MAX_RETRIES})"
                )
                time.sleep(delay)
                continue
            else:
                log.severe(f"Request failed after {__MAX_RETRIES} attempts: {str(e)}")
                raise RuntimeError(
                    f"Discord API request failed after {__MAX_RETRIES} attempts: "
                    f"{str(e)}"
                )

    raise RuntimeError(f"Discord API request failed after {__MAX_RETRIES} attempts")


def fetch_user_guilds(headers: dict) -> List[dict]:
    """
    Fetch all guilds the bot has access to.
    Args:
        headers: Headers for the API request
    Returns:
        List[dict]: List of guild data from Discord API
    """
    url = f"{__DISCORD_API_BASE}/users/@me/guilds"
    result = make_discord_request(url, headers)
    return result if isinstance(result, list) else []


def filter_guilds(guilds: List[dict], configuration: dict) -> List[dict]:
    """
    Filter guilds based on configuration settings.
    Args:
        guilds: List of guild data from Discord API
        configuration: Configuration dictionary
    Returns:
        List[dict]: Filtered list of guilds to process
    """
    sync_all_guilds = configuration.get("sync_all_guilds", "true").lower() == "true"
    specific_guild_ids = configuration.get("guild_ids", "")
    exclude_guild_ids = configuration.get("exclude_guild_ids", "")

    if not sync_all_guilds and not specific_guild_ids:
        log.warning(
            "sync_all_guilds is false but no specific guild_ids provided. "
            "Processing all guilds."
        )
        return guilds

    # Parse specific guild IDs if provided
    specific_ids = set()
    if specific_guild_ids:
        specific_ids = {
            gid.strip() for gid in specific_guild_ids.split(",") if gid.strip()
        }

    # Parse excluded guild IDs if provided
    excluded_ids = set()
    if exclude_guild_ids:
        excluded_ids = {
            gid.strip() for gid in exclude_guild_ids.split(",") if gid.strip()
        }

    filtered_guilds = []
    for guild in guilds:
        guild_id = guild.get("id")
        if not guild_id:
            continue

        # Skip if in excluded list
        if guild_id in excluded_ids:
            log.info(
                f"Excluding guild: {guild.get('name', 'Unknown')} (ID: {guild_id})"
            )
            continue

        # If specific guilds are specified, only include those
        if specific_ids and guild_id not in specific_ids:
            continue

        filtered_guilds.append(guild)

    log.info(
        f"Filtered {len(guilds)} guilds down to {len(filtered_guilds)} guilds "
        f"for processing"
    )
    return filtered_guilds


def fetch_guild_data(guild_id: str, headers: dict) -> dict:
    """
    Fetch guild (server) information from Discord API.
    Args:
        guild_id: The Discord guild ID
        headers: Headers for the API request
    Returns:
        dict: Guild data from Discord API
    """
    url = f"{__DISCORD_API_BASE}/guilds/{guild_id}"
    return make_discord_request(url, headers)


def fetch_guild_channels(guild_id: str, headers: dict) -> List[dict]:
    """
    Fetch all channels from a Discord guild.
    Args:
        guild_id: The Discord guild ID
        headers: Headers for the API request
    Returns:
        list: List of channel data from Discord API
    """
    url = f"{__DISCORD_API_BASE}/guilds/{guild_id}/channels"
    result = make_discord_request(url, headers)
    return result if isinstance(result, list) else []


def fetch_guild_members(guild_id: str, headers: dict, limit: int = 1000) -> List[dict]:
    """
    Fetch guild members from Discord API.
    Args:
        guild_id: The Discord guild ID
        headers: Headers for the API request
        limit: Maximum number of members to fetch
    Returns:
        list: List of member data from Discord API
    """
    url = f"{__DISCORD_API_BASE}/guilds/{guild_id}/members"
    params = {"limit": min(limit, 1000)}  # Discord API max limit is 1000
    result = make_discord_request(url, headers, params)
    return result if isinstance(result, list) else []


def fetch_channel_messages(
    channel_id: str, headers: dict, after: Optional[str] = None, limit: int = 100
) -> List[dict]:
    """
    Fetch messages from a Discord channel.
    Args:
        channel_id: The Discord channel ID
        headers: Headers for the API request
        after: Message ID to fetch messages after (for pagination)
        limit: Maximum number of messages to fetch (max 100)
    Returns:
        list: List of message data from Discord API
    """
    url = f"{__DISCORD_API_BASE}/channels/{channel_id}/messages"
    params: Dict[str, Any] = {"limit": min(limit, 100)}  # Discord API max limit is 100
    if after:
        params["after"] = after

    result = make_discord_request(url, headers, params)
    return result if isinstance(result, list) else []


def process_guild_data(guild_data: dict) -> dict:
    """
    Process and normalize guild data for Fivetran.
    Args:
        guild_data: Raw guild data from Discord API
    Returns:
        dict: Processed guild data
    """
    return {
        "id": guild_data.get("id"),
        "name": guild_data.get("name"),
        "description": guild_data.get("description"),
        "icon": guild_data.get("icon"),
        "splash": guild_data.get("splash"),
        "discovery_splash": guild_data.get("discovery_splash"),
        "owner_id": guild_data.get("owner_id"),
        "region": guild_data.get("region"),
        "afk_channel_id": guild_data.get("afk_channel_id"),
        "afk_timeout": guild_data.get("afk_timeout"),
        "verification_level": guild_data.get("verification_level"),
        "default_message_notifications": guild_data.get(
            "default_message_notifications"
        ),
        "explicit_content_filter": guild_data.get("explicit_content_filter"),
        "mfa_level": guild_data.get("mfa_level"),
        "application_id": guild_data.get("application_id"),
        "system_channel_id": guild_data.get("system_channel_id"),
        "system_channel_flags": guild_data.get("system_channel_flags"),
        "rules_channel_id": guild_data.get("rules_channel_id"),
        "max_presences": guild_data.get("max_presences"),
        "max_members": guild_data.get("max_members"),
        "vanity_url_code": guild_data.get("vanity_url_code"),
        "premium_tier": guild_data.get("premium_tier"),
        "premium_subscription_count": guild_data.get("premium_subscription_count"),
        "preferred_locale": guild_data.get("preferred_locale"),
        "public_updates_channel_id": guild_data.get("public_updates_channel_id"),
        "max_video_channel_users": guild_data.get("max_video_channel_users"),
        "approximate_member_count": guild_data.get("approximate_member_count"),
        "approximate_presence_count": guild_data.get("approximate_presence_count"),
        "welcome_screen": (
            json.dumps(guild_data.get("welcome_screen"))
            if guild_data.get("welcome_screen")
            else None
        ),
        "nsfw_level": guild_data.get("nsfw_level"),
        "stickers": (
            json.dumps(guild_data.get("stickers"))
            if guild_data.get("stickers")
            else None
        ),
        "premium_progress_bar_enabled": guild_data.get("premium_progress_bar_enabled"),
        "created_at": None,  # Discord doesn't provide creation date in guild data
        "synced_at": datetime.now(timezone.utc).isoformat(),
    }


def process_channel_data(channel_data: dict, guild_id: str) -> dict:
    """
    Process and normalize channel data for Fivetran.
    Args:
        channel_data: Raw channel data from Discord API
        guild_id: The Discord guild ID this channel belongs to
    Returns:
        dict: Processed channel data
    """
    return {
        "id": channel_data.get("id"),
        "guild_id": guild_id,
        "name": channel_data.get("name"),
        "type": channel_data.get("type"),
        "topic": channel_data.get("topic"),
        "bitrate": channel_data.get("bitrate"),
        "user_limit": channel_data.get("user_limit"),
        "rate_limit_per_user": channel_data.get("rate_limit_per_user"),
        "position": channel_data.get("position"),
        "permission_overwrites": (
            json.dumps(channel_data.get("permission_overwrites"))
            if channel_data.get("permission_overwrites")
            else None
        ),
        "parent_id": channel_data.get("parent_id"),
        "nsfw": channel_data.get("nsfw"),
        "rtc_region": channel_data.get("rtc_region"),
        "video_quality_mode": channel_data.get("video_quality_mode"),
        "default_auto_archive_duration": channel_data.get(
            "default_auto_archive_duration"
        ),
        "flags": channel_data.get("flags"),
        "available_tags": (
            json.dumps(channel_data.get("available_tags"))
            if channel_data.get("available_tags")
            else None
        ),
        "default_reaction_emoji": (
            json.dumps(channel_data.get("default_reaction_emoji"))
            if channel_data.get("default_reaction_emoji")
            else None
        ),
        "default_thread_rate_limit_per_user": channel_data.get(
            "default_thread_rate_limit_per_user"
        ),
        "default_sort_order": channel_data.get("default_sort_order"),
        "default_forum_layout": channel_data.get("default_forum_layout"),
        "created_at": None,  # Discord doesn't provide creation date in channel data
        "synced_at": datetime.now(timezone.utc).isoformat(),
    }


def process_member_data(member_data: dict, guild_id: str) -> dict:
    """
    Process and normalize member data for Fivetran.
    Args:
        member_data: Raw member data from Discord API
        guild_id: The Discord guild ID this member belongs to
    Returns:
        dict: Processed member data
    """
    user = member_data.get("user", {})
    return {
        "user_id": user.get("id"),
        "guild_id": guild_id,
        "username": user.get("username"),
        "discriminator": user.get("discriminator"),
        "global_name": user.get("global_name"),
        "avatar": user.get("avatar"),
        "bot": user.get("bot", False),
        "system": user.get("system", False),
        "mfa_enabled": user.get("mfa_enabled", False),
        "banner": user.get("banner"),
        "accent_color": user.get("accent_color"),
        "locale": user.get("locale"),
        "verified": user.get("verified", False),
        "email": user.get("email"),
        "flags": user.get("flags"),
        "premium_type": user.get("premium_type"),
        "public_flags": user.get("public_flags"),
        "nick": member_data.get("nick"),
        "avatar_decoration": user.get("avatar_decoration"),
        "roles": json.dumps(member_data.get("roles", [])),
        "joined_at": member_data.get("joined_at"),
        "premium_since": member_data.get("premium_since"),
        "deaf": member_data.get("deaf", False),
        "mute": member_data.get("mute", False),
        "flags_member": member_data.get("flags"),
        "pending": member_data.get("pending"),
        "permissions": member_data.get("permissions"),
        "communication_disabled_until": member_data.get("communication_disabled_until"),
        "synced_at": datetime.now(timezone.utc).isoformat(),
    }


def process_message_data(message_data: dict, channel_id: str) -> dict:
    """
    Process and normalize message data for Fivetran.
    Args:
        message_data: Raw message data from Discord API
        channel_id: The Discord channel ID this message belongs to
    Returns:
        dict: Processed message data
    """
    author = message_data.get("author", {})
    return {
        "id": message_data.get("id"),
        "channel_id": channel_id,
        "author_id": author.get("id"),
        "content": message_data.get("content"),
        "timestamp": message_data.get("timestamp"),
        "edited_timestamp": message_data.get("edited_timestamp"),
        "tts": message_data.get("tts", False),
        "mention_everyone": message_data.get("mention_everyone", False),
        "mentions": json.dumps(message_data.get("mentions", [])),
        "mention_roles": json.dumps(message_data.get("mention_roles", [])),
        "mention_channels": json.dumps(message_data.get("mention_channels", [])),
        "attachments": json.dumps(message_data.get("attachments", [])),
        "embeds": json.dumps(message_data.get("embeds", [])),
        "reactions": json.dumps(message_data.get("reactions", [])),
        "nonce": message_data.get("nonce"),
        "pinned": message_data.get("pinned", False),
        "webhook_id": message_data.get("webhook_id"),
        "type": message_data.get("type"),
        "activity": (
            json.dumps(message_data.get("activity"))
            if message_data.get("activity")
            else None
        ),
        "application": (
            json.dumps(message_data.get("application"))
            if message_data.get("application")
            else None
        ),
        "application_id": message_data.get("application_id"),
        "message_reference": (
            json.dumps(message_data.get("message_reference"))
            if message_data.get("message_reference")
            else None
        ),
        "flags": message_data.get("flags"),
        "referenced_message": (
            json.dumps(message_data.get("referenced_message"))
            if message_data.get("referenced_message")
            else None
        ),
        "interaction": (
            json.dumps(message_data.get("interaction"))
            if message_data.get("interaction")
            else None
        ),
        "thread": (
            json.dumps(message_data.get("thread"))
            if message_data.get("thread")
            else None
        ),
        "components": json.dumps(message_data.get("components", [])),
        "sticker_items": json.dumps(message_data.get("sticker_items", [])),
        "stickers": json.dumps(message_data.get("stickers", [])),
        "position": message_data.get("position"),
        "role_subscription_data": (
            json.dumps(message_data.get("role_subscription_data"))
            if message_data.get("role_subscription_data")
            else None
        ),
        "synced_at": datetime.now(timezone.utc).isoformat(),
    }


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector
    delivers.

    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema

    Args:
        configuration: a dictionary that holds the configuration settings for the
            connector.
    """
    return [
        {"table": "guild", "primary_key": ["id"]},
        {"table": "channel", "primary_key": ["id"]},
        {"table": "member", "primary_key": ["user_id", "guild_id"]},
        {"table": "message", "primary_key": ["id"]},
    ]


def process_single_guild(
    guild_data: dict, headers: dict, configuration: dict, state: dict
) -> dict:
    """
    Process a single guild and return updated state.
    Args:
        guild_data: Guild data from Discord API
        headers: Headers for API requests
        configuration: Configuration dictionary
        state: Current state dictionary
    Returns:
        dict: Updated state for this guild
    """
    guild_id = guild_data.get("id")
    guild_name = guild_data.get("name", "Unknown")

    log.info(f"Processing guild: {guild_name} (ID: {guild_id})")

    # Get guild-specific state
    guild_state = state.get("guilds", {}).get(guild_id, {})
    last_message_sync = guild_state.get("last_message_sync", {})
    processed_count = 0

    try:
        # Process guild data
        processed_guild = process_guild_data(guild_data)
        # The 'upsert' operation is used to insert or update data in the destination
        # table. This ensures that if a guild already exists, it will be updated with
        # the latest information.
        op.upsert(table="guild", data=processed_guild)
        processed_count += 1
        log.info(f"Processed guild: {processed_guild['name']}")

        # Fetch and process channels
        log.info(f"Fetching channel data for guild: {guild_name}")
        channels_data = fetch_guild_channels(guild_id, headers)

        for channel_data in channels_data:
            processed_channel = process_channel_data(channel_data, guild_id)
            # The 'upsert' operation is used to insert or update data in the destination
            # table. This ensures that if a channel already exists, it will be updated
            # with the latest information.
            op.upsert(table="channel", data=processed_channel)
            processed_count += 1

            # Checkpoint every N records for large datasets
            if processed_count % __CHECKPOINT_INTERVAL == 0:
                # Save the progress by checkpointing the state. This is important for
                # ensuring that the sync process can resume from the correct position in
                # case of next sync or interruptions.
                op.checkpoint(state=state)
                log.info(f"Checkpointed at {processed_count} records")

        log.info(f"Processed {len(channels_data)} channels for guild: {guild_name}")

        # Fetch and process members
        log.info(f"Fetching member data for guild: {guild_name}")
        members_data = fetch_guild_members(guild_id, headers)

        for member_data in members_data:
            processed_member = process_member_data(member_data, guild_id)
            # The 'upsert' operation is used to insert or update data in the destination
            # table. This ensures that if a member already exists, it will be updated
            # with the latest information.
            op.upsert(table="member", data=processed_member)
            processed_count += 1

            # Checkpoint every N records for large datasets
            if processed_count % __CHECKPOINT_INTERVAL == 0:
                # Save the progress by checkpointing the state. This is important for
                # ensuring that the sync process can resume from the correct position in
                # case of next sync or interruptions.
                op.checkpoint(state=state)
                log.info(f"Checkpointed at {processed_count} records")

        log.info(f"Processed {len(members_data)} members for guild: {guild_name}")

        # Fetch and process messages if enabled
        sync_messages = configuration.get("sync_messages", "true").lower() == "true"
        if sync_messages:
            log.info(f"Fetching message data for guild: {guild_name}")
            text_channels = [
                ch for ch in channels_data if ch.get("type") in [0, 5]
            ]  # Text and news channels
            message_limit = int(configuration.get("message_limit", "1000"))

            for channel in text_channels:
                channel_id = channel["id"]
                channel_name = channel.get("name", "unknown")

                log.info(
                    f"Processing messages for channel: {channel_name} in guild: "
                    f"{guild_name}"
                )

                # Get last message ID for this channel from state
                last_message_id = last_message_sync.get(channel_id)
                messages_fetched = 0

                # Pagination logic: Fetch messages in batches to avoid memory overflow
                # This ensures that the connector does not load all data into memory at
                # once
                while messages_fetched < message_limit:
                    try:
                        # Fetch messages for this channel using cursor-based pagination
                        # The 'after' parameter ensures we only get messages newer than
                        # the last processed message
                        messages_data = fetch_channel_messages(
                            channel_id,
                            headers,
                            after=last_message_id,
                            limit=min(100, message_limit - messages_fetched),
                        )

                        if not messages_data:
                            break

                        for message_data in messages_data:
                            processed_message = process_message_data(
                                message_data, channel_id
                            )
                            # The 'upsert' operation is used to insert or update data in
                            # the destination table. This ensures that if a message
                            # already exists, it will be updated with the latest
                            # information.
                            op.upsert(table="message", data=processed_message)
                            processed_count += 1
                            messages_fetched += 1

                            # Update last message ID for this channel
                            last_message_id = message_data["id"]

                            # Checkpoint every N records for large datasets
                            if processed_count % __CHECKPOINT_INTERVAL == 0:
                                # Save the progress by checkpointing the state. This is
                                # important for ensuring that the sync process can
                                # resume
                                # from the correct position in case of next sync or
                                # interruptions.
                                op.checkpoint(state=state)
                                log.info(f"Checkpointed at {processed_count} records")

                        # If we got fewer messages than requested, we've reached the end
                        if len(messages_data) < 100:
                            break

                    except Exception as e:
                        log.warning(
                            f"Error fetching messages for channel {channel_name} in "
                            f"guild {guild_name}: {str(e)}"
                        )
                        break

                # Update state with last message ID for this channel
                last_message_sync[channel_id] = last_message_id
                log.info(
                    f"Processed {messages_fetched} messages for channel: "
                    f"{channel_name} in guild: {guild_name}"
                )

        # Return updated guild state
        return {
            "last_message_sync": last_message_sync,
            "processed_count": processed_count,
            "last_sync_time": datetime.now(timezone.utc).isoformat(),
        }

    except Exception as e:
        log.severe(f"Error processing guild {guild_name} (ID: {guild_id}): {str(e)}")
        raise


def update(configuration: dict, state: dict):
    """
    Define the update function, which is a required function, and is called by
    Fivetran during each sync.

    See the technical reference documentation for more details on the update function
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    Args:
        configuration: A dictionary containing connection details
        state: A dictionary containing state information from previous runs
        The state dictionary is empty for the first sync or for any full re-sync
    """
    log.info("Discord Connector: Starting multi-guild sync process")
    log.warning("Example: Multi-Guild Discord Connector : Discord API Integration")

    # Validate the configuration to ensure it contains all required values
    validate_configuration(configuration=configuration)

    # Extract configuration parameters
    bot_token = configuration.get("bot_token", "")

    # Get state variables for multi-guild sync
    guilds_state = state.get("guilds", {})
    total_processed_count = 0

    try:
        # Create headers for Discord API requests
        headers = get_discord_headers(bot_token)

        # Fetch all guilds the bot has access to
        log.info("Fetching all guilds the bot has access to")
        all_guilds = fetch_user_guilds(headers)
        log.info(f"Found {len(all_guilds)} guilds the bot has access to")

        # Filter guilds based on configuration
        guilds_to_process = filter_guilds(all_guilds, configuration)
        log.info(f"Processing {len(guilds_to_process)} guilds after filtering")

        if not guilds_to_process:
            log.warning("No guilds to process after filtering")
            return

        # Process each guild
        for guild_data in guilds_to_process:
            guild_id = guild_data.get("id")
            guild_name = guild_data.get("name", "Unknown")

            try:
                log.info(f"Starting processing of guild: {guild_name} (ID: {guild_id})")

                # Process this guild
                guild_state = process_single_guild(
                    guild_data, headers, configuration, state
                )

                # Update guild-specific state
                guilds_state[guild_id] = guild_state
                total_processed_count += guild_state.get("processed_count", 0)

                log.info(
                    f"Completed processing guild: {guild_name} - "
                    f"{guild_state.get('processed_count', 0)} records"
                )

                # Checkpoint after each guild to ensure progress is saved.
                # Save the progress by checkpointing the state. This is important for
                # ensuring that the sync process can resume from the correct position in
                # case of next sync or interruptions.
                op.checkpoint(state=state)

            except Exception as e:
                log.severe(
                    f"Failed to process guild {guild_name} (ID: {guild_id}): {str(e)}"
                )
                # Continue with other guilds even if one fails
                continue

        # Update global state
        new_state = {
            "last_sync_time": datetime.now(timezone.utc).isoformat(),
            "guilds": guilds_state,
            "total_processed_count": total_processed_count,
        }

        # Final checkpoint with updated state
        # Save the progress by checkpointing the state. This is important for
        # ensuring that the sync process can resume from the correct position in
        # case of next sync or interruptions.
        op.checkpoint(state=new_state)
        log.info(
            f"Discord Connector: Multi-guild sync completed successfully. "
            f"Processed {total_processed_count} total records across "
            f"{len(guilds_to_process)} guilds"
        )

    except Exception as e:
        log.severe(f"Discord Connector: Multi-guild sync failed with error: {str(e)}")
        raise RuntimeError(f"Failed to sync Discord data: {str(e)}")


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run
# directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not
# called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying
# your connector.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents
    with open("configuration.json", "r") as f:
        configuration = json.load(f)

    # Test the connector locally
    connector.debug(configuration=configuration)
