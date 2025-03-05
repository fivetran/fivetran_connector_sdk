import ssl # Import required for SSL context
import tempfile # Import required for temporary file operations
import urllib.request # Import required for making HTTP requests
import json # Import required for JSON operations
import os
import re

from aws_client import S3Client

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector # For supporting Connector operations like Update() and Schema()
from fivetran_connector_sdk import Logging as log # For enabling Logs in your connector code
from fivetran_connector_sdk import Operations as op # For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()


BASE_URL = "https://client.badssl.com/"

# Define the schema function which lets you configure the schema your connector delivers.
# See the technical reference documentation for more details on the schema function:
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
# The schema function takes one parameter:
# - configuration: a dictionary that holds the configuration settings for the connector.
def schema(configuration: dict):
    return [
        {
            "table": "sample_data",  # Name of the table in the destination.
            "primary_key": ["id"],  # Primary key column(s) for the table.
        }
    ]


def get_urllib_context(cert_path: str, passkey: str):
    """
    Get an SSL context for urllib with the given certificate and key.
    Args:
        cert_path: temporary certificate file path
        passkey: password for the private key
    """
    context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    context.load_cert_chain(certfile=cert_path,
                            password=passkey)
    return context


def get_certificates(configuration: dict):
    """
    Get the certificates from the cloud storage.
    Args:
        configuration: configuration dictionary
    """
    aws_access_key_id = configuration["AWS_ACCESS_KEY_ID"]
    aws_secret_access_key = configuration["AWS_ACCESS_SECRET_KEY"]
    bucket_name = configuration["BUCKET_NAME"]
    object_key = configuration["OBJECT_KEY"]
    region = configuration["REGION"]

    client = S3Client(aws_access_key_id, aws_secret_access_key, region)

    try:
        log.info("Fetching certificates from cloud")

        content = client.get_object(bucket_name, object_key)
        with open("certificate.pem","w") as temp_cert_file:
            temp_cert_file.write(content.decode('utf-8'))
            cert_path = temp_cert_file.name
        return cert_path
    except Exception as e:
        raise RuntimeError(f"Could not fetch the certificates from cloud: {str(e)}")


def get_data_with_certificate(base_url: str, cert_path: str, passkey: str):
    """
    Get the data from the API using the certificate.
    Args:
        base_url: base URL of the API
        cert_path: certificate file path
        passkey: password for the private key
    """
    try:
        log.info(f"Fetching data from {base_url}")
        context = get_urllib_context(cert_path, passkey)
        with urllib.request.urlopen(base_url, context=context) as response:
            content = response.read()

        content = re.sub(r'<[^>]+>', '', content.decode('utf-8'))
        content = [line.strip() for line in content.splitlines() if line.strip()]
        return content

    except Exception as exception:
        raise ConnectionError(f"Failed to get data from {base_url}: {exception}")
    finally:
        log.info("Deleting downloaded certificate files")
        if cert_path:
            os.unlink(cert_path)


# Define the update function, which is a required function, and is called by Fivetran during each sync.
# See the technical reference documentation for more details on the update function
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
# The function takes two parameters:
# - configuration: dictionary contains any secrets or payloads you configure when deploying the connector
# - state: a dictionary contains whatever state you have chosen to checkpoint during the prior sync
# The state dictionary is empty for the first sync or for any full re-sync
def update(configuration: dict, state: dict):
    log.info("Example: Using certificates for API authentication")

    last_index = state['last_index'] if 'last_index' in state else -1

    cert_path = get_certificates(configuration)
    passkey = configuration["PASSKEY"]

    data = get_data_with_certificate(BASE_URL, cert_path, passkey)
    if not data:
        raise RuntimeError("No data received")

    for i in data:
        last_index += 1
        yield op.upsert(table="sample_data", data={"id":last_index,"content": i})
        if last_index%5 == 0: #checkpoint after every 5 record
            yield op.checkpoint({"last_index": last_index})

    yield op.checkpoint({"last_index": last_index}) #checkpoint after all records are processed


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
