from fivetran_customer_sdk import Connector
from fivetran_customer_sdk import Operations as op


def update(configuration: dict, state: dict):
    yield op.upsert("hello",
                    {"message": "hello, world!"})


connector = Connector(update=update)


if __name__ == "__main__":
    connector.debug()

