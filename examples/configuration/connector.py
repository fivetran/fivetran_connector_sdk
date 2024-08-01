# This is a simple example for how to work with the fivetran_connector_sdk module.
# It shows the use of a requirements.txt file and a configuration.json file to pass a credential key into the connector code and use it to decrypt a message
# to get the weather forecast data for Myrtle Beach in South Carolina, USA.
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details.

import json  # Import the json module to handle JSON data.

# Import the Fernet class from the cryptography module for encryption and decryption. The cryptography module is listed as a requirement in requirements.txt
from cryptography.fernet import Fernet
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Operations as op

# This is an encrypted message that will be decrypted using a key from the configuration.
encrypted_message = b'gAAAAABl-3QGKUHpdUhBNpnW1_SnSkQGrAwev-uBBJaZo4NmtylIMg8UX6usuG4Z-h80OvfJajW6HU56O5hofapEIh4W33vuMpJgq0q3qMQx6R3Ol4qZ3Wc2DyIIapxbK5BrQHshBF95'


# Define the schema function which lets you configure the schema your connector delivers.
# See the technical reference documentation for more details on the schema function:
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
# The schema function takes one parameter:
# - configuration: a dictionary that holds the configuration settings for the connector.
def schema(configuration: dict):
    # Check if the 'my_key' is present in the configuration.
    if 'my_key' not in configuration:
        raise ValueError("Could not find 'my_key'")

    return [
        {
            "table": "crypto",  # Name of the table in the destination.
            "primary_key": ["msg"],  # Primary key column(s) for the table.
        }
    ]


# Define the update function, which is a required function, and will be used to perform operations in the connector.
# See the technical reference documentation for more details on the update function:
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
# The function takes two parameters:
# - configuration: dictionary containing any secrets or payloads you configure when deploying the connector.
# - state: a dictionary containing the state checkpointed during the prior sync.
#   The state dictionary is empty for the first sync or for any full re-sync.
def update(configuration: dict, state: dict):
    # Retrieve the encryption key from the configuration.
    key = configuration['my_key']
    # Create a Fernet object for encryption and decryption using the provided key.
    f = Fernet(key.encode())

    # Decrypt the encrypted message using the Fernet object.
    message = f.decrypt(encrypted_message)

    # Yield an upsert operation to insert/update the decrypted message in the "crypto" table.
    yield op.upsert(table="crypto", data={
        'msg': message.decode()  # Decode the decrypted message to a string.
    })


# Instantiate a Connector object from the Connector class, passing the update and schema functions as parameters.
# This creates a new connector that will use these functions to define its behavior.
connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    # Open the configuration.json file and load its contents into a dictionary.
    with open("configuration.json", 'r') as f:
        configuration = json.load(f)
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from your IDE.
    connector.debug(configuration=configuration)
