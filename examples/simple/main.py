
from fivetran_custom_sdk import Connector, upsert, checkpoint


SOURCE_DATA = [
    {"id": 10, "message": "Hello world"},
    {"id": 20, "message": "Hello again"},
    {"id": 30, "message": "Good bye"},
]


def schema(configuration: dict):
    return [
            {
                "table": "hello_world",
                "primary_key": ["id"],
                "columns": {
                    "message": "STRING",
                },
            }
    ]


def update(configuration: dict, state: dict):
    cursor = state['cursor'] if 'cursor' in state else 0

    row = SOURCE_DATA[cursor]

    yield upsert(table="hello_world", data=row)

    new_state = {
        "cursor": cursor + 1
    }

    yield checkpoint(new_state)


connector = Connector(update=update, schema=schema)


# The following code block optional, to be able to run the connector code in an IDE easily
if __name__ == "__main__":
    result = connector.debug()
    if result:
        print("Success! You can publish your code now if you like: "
              "`DEPLOY_KEY=XXXX python -m fivetran_custom_sdk main.py --deploy`")

