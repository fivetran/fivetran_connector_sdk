import pandas as pd
import json
import praw
import datetime
import time
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op


def iso_to_epoch(last_sync_time):
    if not last_sync_time:
        return 0
    try:
        dt = datetime.datetime.fromisoformat(last_sync_time)
        return int(dt.timestamp())
    except Exception:
        return 0


def validate_configuration(configuration: dict):
    required_configs = ["subreddits", "client_id", "client_secret", "user_agent"]
    for key in required_configs:
        if key not in configuration:
            raise ValueError(f"Missing configuration: {key}")


def schema(configuration: dict):
    return [
        {
            "table": "reddit_data",
            "primary_key": ["id"],
            "columns": {
                "id": "INT",
                "post_id": "STRING",
                "post_text": "STRING",
                "post_author": "STRING",
                "comment_id": "STRING",
                "comment_text": "STRING",
                "comment_author": "STRING",
            },
        },
    ]


def fetch_reddit_posts_and_comments(
    subreddit_str, reddit_creds, last_sync_epoch, batch_size=100, sleep_sec=2
):
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
        pd.DataFrame(records).to_csv("reddit_data_export.csv", index=False)
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
