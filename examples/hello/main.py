
from fivetran_customer_sdk import Connector, upsert


def update(configuration: dict, state: dict):
    yield upsert("hello_world", {"id": 1, "message": "hello, world!"})


connector = Connector(update=update)


if __name__ == "__main__":
    connector.debug()

