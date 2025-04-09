# This is a simple example for how to work with the fivetran_connector_sdk module.
# It defines a simple `update` method, which uses python-gnupg to sign messages with a private key.
# This example upserts the signed data to a table named "signed_message".
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector # For supporting Connector operations like Update() and Schema()
from fivetran_connector_sdk import Logging as log # For enabling Logs in your connector code
from fivetran_connector_sdk import Operations as op # For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()

# Importing the gnupg library for GPG encryption and decryption
import gnupg
import os
import json


# This method is used to set up the GPG home directory.
# This is required for the gnupg library to work properly.
# The GPG home directory is where the GPG keys and configuration files are stored.
# This method returns the path to the GPG home directory.
def setup_gnupghome():
    # Set up the GPG home directory
    # You can change this to a different directory if you want
    gnupghome = './gnupg'
    if not os.path.exists(gnupghome):
        log.info(f"Creating GPG home directory at {gnupghome}")
        os.makedirs(gnupghome)
    return gnupghome


# This method is used to import the private key into the GPG object.
# The private key is used to sign messages.
# The method takes the configuration dictionary and the GPG object as parameters.
# The method returns the imported key.
def get_gpg_key(configuration: dict, gpg):
    # Get the private key from the configuration
    # Ensure that the private key string in configuration is properly escaped as JSON string.
    private_key = configuration.get("private_key")
    key = gpg.import_keys(private_key)
    # Check if the key was imported successfully
    if key.count == 0:
        raise RuntimeError("Failed to import the private key.")
    else:
        log.info("Successfully imported the private key.")
        return key


# This method is used to sign a message using the GPG object.
# The method takes the configuration dictionary, the GPG object, the key, and the message to be signed as parameters.
# The method returns the signed message.
def sign_message(configuration, gpg, key, message):
    passphrase = configuration.get("passphrase")
    # Extract the key ID from the imported key and sign the message using the key ID and passphrase
    key_id = key.fingerprints[0]
    signed_message = gpg.sign(message, keyid=key_id, passphrase=passphrase)
    # return the signed message
    return signed_message.data


# Define the schema function which lets you configure the schema your connector delivers.
# See the technical reference documentation for more details on the schema function:
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
# The schema function takes one parameter:
# - configuration: a dictionary that holds the configuration settings for the connector.
def schema(configuration: dict):
    return [
        {
            "table": "signed_message", # Name of the table
            "primary_key": ["id"],
            "columns": { # Define the columns and their data types.
                "id": "INTEGER"
            } # For any columns whose names are not provided here, e.g. message, their data types will be inferred
        }
    ]


# Define the update function, which is a required function, and is called by Fivetran during each sync.
# See the technical reference documentation for more details on the update function
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
# The function takes two parameters:
# - configuration: dictionary contains any secrets or payloads you configure when deploying the connector
# - state: a dictionary contains whatever state you have chosen to checkpoint during the prior sync
# The state dictionary is empty for the first sync or for any full re-sync
def update(configuration: dict, state: dict):
    log.warning("Example: Common Pattern for Connectors Examples - GPG Private Keys")

    # get the path to the GPG home directory
    gnupghome = setup_gnupghome()
    # Initialize the GPG object with the GPG home directory
    gpg = gnupg.GPG(gnupghome=gnupghome)
    # Load the private key from the configuration and import it into the GPG object
    key = get_gpg_key(configuration, gpg)

    # This is the message to be signed
    # You can change this to any message you want to sign
    message = "This is a test message. This will be encrypted and signed"

    # Sign the message using the GPG object and the imported key
    signed_message = sign_message(configuration, gpg, key, message)

    # Verify if the signed message is valid
    verified = gpg.verify(signed_message)
    if verified:
        log.info("The message is valid and has been verified.")
    else:
        raise RuntimeError("The message is not valid and could not be verified.")

    # The yield statement returns a generator object.
    # This generator will yield an upsert operation to the Fivetran connector.
    # The op.upsert method is called with two arguments:
    # - The first argument is the name of the table to upsert the data into, in this case, "signed_message".
    # - The second argument is a dictionary containing the data to be upserted,
    log.fine("upserting to table 'signed_message'")
    yield op.upsert(table="signed_message", data={"id":1, "message": signed_message})

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
    yield op.checkpoint(state)


# This creates the connector object that will use the update function defined in this connector.py file.
# This example does not use the schema() function. If it did, it would need to be included in the connector object definition.
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    try:
        # Try loading the configuration from the file
        with open("configuration.json", 'r') as f:
            configuration = json.load(f)
    except FileNotFoundError:
        # Fallback to an empty configuration if the file is not found
        configuration = {}
    # Allows testing the connector directly
    connector.debug(configuration=configuration)
