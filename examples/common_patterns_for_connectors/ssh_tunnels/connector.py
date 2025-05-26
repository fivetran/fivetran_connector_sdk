import io
import json
from sshtunnel import SSHTunnelForwarder
import paramiko
from fivetran_connector_sdk import Logging as log, Connector  # For enabling Logs in your connector code
from fivetran_connector_sdk import Operations as op # For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()

import requests

REMOTE_PORT = 5000
LOCAL_PORT = 8000

def update(configuration: dict, state: dict):
    yield op.upsert(table="hello", data={"message": "hello, world!"})
    ssh_host = configuration.get("ssh_host")
    ssh_user = configuration.get("ssh_user")
    private_key_string = configuration.get("private_key")
    key_stream = io.StringIO(private_key_string)
    private_key = paramiko.RSAKey.from_private_key(key_stream)

    with SSHTunnelForwarder(
            (ssh_host, 22),
            ssh_username=ssh_user,
            ssh_pkey=private_key,
            remote_bind_address=('127.0.0.1', REMOTE_PORT),
            local_bind_address=('127.0.0.1', LOCAL_PORT)
    ) as tunnel:
        log.severe(f"Tunnel open at http://127.0.0.1:{LOCAL_PORT}")

        # Insert data
        add_resp = requests.post(
            f"http://127.0.0.1:{LOCAL_PORT}/add",
            json={"name": "Test Item"}
        )
        log.severe("Add response:", add_resp.json())

        # Fetch data
        fetch_resp = requests.get(f"http://127.0.0.1:{LOCAL_PORT}/fetch")
        log.severe("Fetch response:", fetch_resp.json())

connector = Connector(update=update)

if __name__ == "__main__":
    try:
        with open("configuration.json", 'r') as f:
            configuration = json.load(f)
    except FileNotFoundError:
        # Fallback to an empty configuration if the file is not found
        configuration = {}
    # Allows testing the connector directly
    connector.debug(configuration=configuration)