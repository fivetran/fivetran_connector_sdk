# This is an example for how to work with the fivetran_connector_sdk module.
# It demonstrates how to parse a JSON response into a POJO-style class and use it in the update function.
# It defines a simple 'update' method, which upserts Posts from JSON Placeholder's API into a Fivetran connector.
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details

# Import required classes from fivetran_connector_sdk
# For supporting Connector operations like Update() and Schema()
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# Import requests to make HTTP calls to API.
import requests

# Import dataclass for creating POJO-style classes
from dataclasses import dataclass


# Constants
API_URL = "https://jsonplaceholder.typicode.com/posts"


# POJO-style response class
@dataclass
class Post:
    userId: int
    id: int
    title: str
    body: str


def schema(configuration: dict):
    return [
        {
            "table": "posts",
            "primary_key": ["id"],
            "columns": {
                "id": "INT",
                "userId": "INT",
                "title": "STRING",
                "body": "STRING"
            }
        }
    ]


def update(configuration: dict, state: dict):
    log.info("Fetching posts from JSONPlaceholder")

    try:
        response = requests.get(API_URL)
        response.raise_for_status()
        post_list = response.json()
    except requests.RequestException as e:
        log.severe(f"Request failed: {e}")
        return

    for post_dict in post_list:
        post = Post(**post_dict)  # Deserialize into a POJO

        # Emit upsert operation
        yield op.upsert("posts", {
            "id": post.id,
            "userId": post.userId,
            "title": post.title,
            "body": post.body
        })

    log.info(f"Synced {len(post_list)} posts")


connector = Connector(update=update, schema=schema)


if __name__ == "__main__":
    connector.debug(configuration={})
