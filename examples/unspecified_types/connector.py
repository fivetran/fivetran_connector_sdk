from fivetran_customer_sdk import Connector
from fivetran_customer_sdk import Operations as op


def schema(configuration: dict):
    return [
        {
            "table": "unspecified",
            "primary_key": ["id"],
        },
    ]


def update(configuration: dict, state: dict):
    yield op.upsert(table="unspecified",
                    data={
                        "id": 1,
                        "_bool": True,
                        "_short": 15,
                        "_long": 132353453453635,
                        "_dec": "105.34",
                        "_float": 10.4,
                        "_double": 1e-4,
                        "_ndate": "2007-12-03",
                        "_ndatetime": "2007-12-03T10:15:30",
                        "_utc": "2007-12-03T10:15:30.123Z",
                        "_binary": b"\x00\x01\x02\x03",
                        "_xml": "<tag>This is XML</tag>",
                        "_str": "This is a string",
                        "_json": {"a": 10},
                        "_null": None
                    })


connector = Connector(update=update, schema=schema)


if __name__ == "__main__":
    result = connector.debug()