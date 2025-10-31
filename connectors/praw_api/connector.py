"""
This connector demonstrates how to fetch data from PRAW (Python Reddit API Wrapper) API and upsert it into destination using the Fivetran Connector SDK.
You need to have your reddit app credentials for this connector to work.
See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

# For reading configuration from a JSON file
import json

# Import praw api library
import praw

# Importing datetime library to handle all datetime records to convert them to Epoch to maintain consistency across different timezones
import datetime
import time

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op


def iso_to_epoch(last_sync_time: str):
    """
    Converts ISO format datetime string to a Unix timestamp.    This function is converting timestamp of last_sync_time to epoch for system time consistency for each record.
    This function ensures system time consistency for each record.
    Args:
        last_sync_time: String in ISO 8601 format from the dictionary state.
    Raises:
        ValueError: if any required parameter last_sync_time is missing or invalid.
    """
    if not last_sync_time:
        raise ValueError("Missing requirement args/parameter: last_sync_time")

    try:
        dt = datetime.datetime.fromisoformat(last_sync_time)
        return int(dt.timestamp())
    except Exception as e:
        raise ValueError(
            f"Incorrect datetime format for last_sync_time: {last_sync_time}"
        ) from e


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    This function is called at the start of the update method to ensure that the connector has all necessary configuration values.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing.
    """

    required_configs = ["subreddits", "client_id", "client_secret", "user_agent"]
    for key in required_configs:
        if key not in configuration:
            raise ValueError(f"Missing configuration: {key}")


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    return [
        {
            "table": "posts_data",
            "primary_key": ["post_id"],
            "columns": {
                "post_id": "STRING",
                "post_text": "STRING",
                "post_author": "STRING",
                # For any columns whose names are not provided here, e.g. id, their data types will be inferred
            },
        },
        {
            "table": "comments_data",
            "primary_key": ["comment_id"],
            "columns": {
                "comment_id": "STRING",
                "comment_text": "STRING",
                "comment_author": "STRING",
                # For any columns whose names are not provided here, e.g. id, their data types will be inferred
            },
        },
    ]


def fetch_reddit_posts_and_comments(
    subreddit_str, reddit_creds, last_sync_epoch, batch_size=100, sleep_sec=2
):
    """
    def create_reddit_client(required_configs):

        WIP : 1) create client 2) gets the subreddits list from config
            3) for a topic in subreddit list, get the posts add to the posts table, reset posts list.
            4) Do same for comments.

        Create and return an authenticated Reddit client.
        This function will use the validated config parameters needed to setup/create the reddit client.
        Args:
            required_configs: List of reddit parameters needed by PRAW API, validated by Validate_Configuration method above.
        Raises:
            RunTimeError: if failed to create reddit client.

        try:
            return praw.Reddit(
                client_id=required_configs["client_id"],
                client_secret=required_configs["client_secret"],
                user_agent=required_configs["user_agent"],
            )
        except Exception as e:
            raise RuntimeError(f"Failed to create Reddit client:{e}")
    """

    reddit = praw.Reddit(
        client_id=reddit_creds["client_id"],
        client_secret=reddit_creds["client_secret"],
        user_agent=reddit_creds["user_agent"],
    )

    all_posts_with_comments = []
    max_epoch_batch = last_sync_epoch

    subreddits = [s.strip() for s in subreddit_str.split(",") if s.strip()]
    for subreddit_name in subreddits:
        fetched = 0
        for post in reddit.subreddit(subreddit_name).new(limit=batch_size):
            if post.created_utc > last_sync_epoch:
                post_data = {
                    "id": post.id,
                    "post_text": post.selftext or post.title or "",
                    "post_author": str(post.author) if post.author else "",
                    "comments": [],
                }
                if post.created_utc > max_epoch_batch:
                    max_epoch_batch = post.created_utc
                try:
                    post.comments.replace_more(limit=None)
                    comments_list = post.comments.list()
                    for comment in comments_list:
                        comment_data = {
                            "comment_id": comment.id,
                            "comment_text": comment.body or "",
                            "comment_author": str(comment.author)
                            if comment.author
                            else "",
                        }
                        post_data["comments"].append(comment_data)
                    time.sleep(sleep_sec)  # Sleep after each post
                except Exception as e:
                    log.warning(f"Failed comments: {e}")
                    continue
                all_posts_with_comments.append(post_data)
                fetched += 1
            if fetched >= batch_size:
                break
    return all_posts_with_comments, max_epoch_batch


def flatten_posts_with_comments(posts):
    rows = []
    i = 1
    for post in posts:
        post_id = post.get("id", "")
        post_text = post.get("post_text", "")
        post_author = post.get("post_author", "")
        if post.get("comments"):
            for comment in post["comments"]:
                rows.append(
                    {
                        "id": i,
                        "post_id": post_id,
                        "post_text": post_text,
                        "post_author": post_author,
                        "comment_id": comment.get("comment_id", ""),
                        "comment_author": comment.get("comment_author", ""),
                        "comment_text": comment.get("comment_text", ""),
                    }
                )
                i += 1
        else:
            rows.append(
                {
                    "id": i,
                    "post_id": post_id,
                    "post_text": post_text,
                    "post_author": post_author,
                    "comment_id": "",
                    "comment_author": "",
                    "comment_text": "",
                }
            )
            i += 1
    return rows


def update(configuration: dict, state: dict):
    log.info("Starting Reddit sync...")
    validate_configuration(configuration)
    subreddit_str = configuration.get("subreddits", "MultipleSclerosis")
    reddit_creds = {
        "client_id": configuration["client_id"],
        "client_secret": configuration["client_secret"],
        "user_agent": configuration["user_agent"],
    }
    last_sync_time = state.get("last_sync_time", "")
    last_sync_epoch = iso_to_epoch(last_sync_time)
    # new_sync_epoch = last_sync_epoch

    try:
        posts_and_comments, max_epoch_batch = fetch_reddit_posts_and_comments(
            subreddit_str, reddit_creds, last_sync_epoch, batch_size=100, sleep_sec=2
        )
        records = flatten_posts_with_comments(posts_and_comments)
        for record in records:
            op.upsert(table="reddit_data", data=record)

        if max_epoch_batch > last_sync_epoch:
            new_sync_time = datetime.datetime.fromtimestamp(
                max_epoch_batch, tz=datetime.timezone.utc
            ).isoformat()
        else:
            new_sync_time = last_sync_time
        new_state = {"last_sync_time": new_sync_time}
        op.checkpoint(new_state)
    except Exception as e:
        raise RuntimeError(f"Failed to sync data: {str(e)}")


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    with open("./my_connector/configuration.json") as f:
        configuration = json.load(f)
    connector.debug(configuration=configuration)
