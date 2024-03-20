from fivetran_customer_sdk import Connector
from fivetran_customer_sdk import Operations as op

import uuid


def schema(configuration: dict):
    return [
            {
                "table": "triplet",
                "primary_key": ["id"],
            }
    ]


def update(configuration: dict, state: dict):
    ids = [uuid.uuid4(), uuid.uuid4(), uuid.uuid4()]

    for ii, id in enumerate(ids):
        print(f"adding {id}")
        yield op.upsert(table="three", data={"id": id, "val1": id, "val2": ii})

    print(f"updating {ids[1]} to 'abc'")
    yield op.update(table="three", modified={"id": ids[1], "val1": "abc"})

    print(f"deleting {ids[2]}")
    yield op.delete(table="three", keys={"id": ids[2]})


connector = Connector(update=update, schema=schema)


if __name__ == "__main__":
    connector.debug()
