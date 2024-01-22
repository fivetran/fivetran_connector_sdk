
from fivetran_custom_sdk import Connector, upsert


def update(configuration: dict, state: dict):
    yield upsert("hello_world", {"id": 1, "message": "hello, world!"})


connector = Connector(update=update)


# The following code block optional, to be able to run the connector code in an IDE easily
if __name__ == "__main__":
    connector.debug()

