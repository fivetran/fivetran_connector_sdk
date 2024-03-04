
from fivetran_customer_sdk import Connector, upsert, checkpoint


def schema(configuration: dict):
    return [
        {
            "table": "specified",
            "primary_key": ["_bool"],
            "columns": {
                "_bool": "BOOLEAN",
                "_short": "SHORT",
                "_long": "LONG",
                "_dec": {
                    "type": "DECIMAL",
                    "precision": 15,
                    "scale": 2
                },
                "_float": "FLOAT",
                "_double": "DOUBLE",
                "_ndate": "NAIVE_DATE",
                "_ndatetime": "NAIVE_DATETIME",
                "_utc": "UTC_DATETIME",
                "_binary": "BINARY",
                "_xml": "XML",
                "_str": "STRING",
                "_json": "JSON",
                "_null": "STRING"
            }
        }
    ]


def update(configuration: dict, state: dict):
    yield upsert(table="specified", data={
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