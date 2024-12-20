# This is a simple example for how to work with the fivetran_connector_sdk module.
# It shows the use of a requirements.txt file and a configuration.json file to pass a credential key into the connector code and use it to decrypt a message.
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details.

import json  # Import the json module to handle JSON data.

# Import the Fernet class from the cryptography module for encryption and decryption. The cryptography module is listed as a requirement in requirements.txt
from cryptography.fernet import Fernet
# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
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
            # No columns are defined, meaning the types will be inferred.
        }
    ]


# Define the update function, which is a required function, and is called by Fivetran during each sync.
# See the technical reference documentation for more details on the update function:
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
# The function takes two parameters:
# - configuration: dictionary containing any secrets or payloads you configure when deploying the connector.
# - state: a dictionary containing the state checkpointed during the prior sync.
#   The state dictionary is empty for the first sync or for any full re-sync.
def update(configuration: dict, state: dict):
    log.warning("Example: QuickStart Examples - Configuration")

    # Retrieve the encryption key from the configuration.
    key = configuration['my_key']
    # Create a Fernet object for encryption and decryption using the provided key.
    f = Fernet(key.encode())

    # Decrypt the encrypted message using the Fernet object.
    log.fine("decrypting the message")
    message = f.decrypt(encrypted_message)

    # Yield an upsert operation to insert/update the decrypted message in the "crypto" table.
    yield op.upsert(table="crypto",
                    data={
                        'msg': message.decode()  # Decode the decrypted message to a string.
                    })

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
    yield op.checkpoint(state)


# This creates the connector object that will use the update and schema functions defined in this connector.py file.
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents into a dictionary.
    with open("configuration.json", 'r') as f:
        configuration = json.load(f)
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from your IDE.
    connector.debug(configuration=configuration)

# Resulting table:
# ┌────────────────────────────────────────────────┐
# │                      msg                       │
# │                    varchar                     │
# ├────────────────────────────────────────────────┤
# │ If you can read this, you have the correct key │
# └────────────────────────────────────────────────┘
