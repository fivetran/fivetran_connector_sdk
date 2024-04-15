from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Operations as op
from cryptography.fernet import Fernet
import json


encrypted_message = b'gAAAAABl-3QGKUHpdUhBNpnW1_SnSkQGrAwev-uBBJaZo4NmtylIMg8UX6usuG4Z-h80OvfJajW6HU56O5hofapEIh4W33vuMpJgq0q3qMQx6R3Ol4qZ3Wc2DyIIapxbK5BrQHshBF95'


def schema(configuration: dict):
    if 'my_key' not in configuration:
        raise ValueError("Could not find 'my_key'")

    return [
            {
                "table": "crypto",
                "primary_key": ["msg"],
            }
    ]


def update(configuration: dict, state: dict):
    key = configuration['my_key']
    f = Fernet(key.encode())

    message = f.decrypt(encrypted_message)

    yield op.upsert(table="crypto", data={
        'msg': message.decode()
    })


connector = Connector(update=update, schema=schema)


if __name__ == "__main__":
    with open("configuration.json", 'r') as f:
        configuration = json.load(f)
    connector.debug(configuration=configuration)

