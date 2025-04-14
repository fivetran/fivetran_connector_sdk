"""
This is a connector to retrieve data extracts from MasterTax API at https://api.adp.com.
It gets contents of certificate files from setup configurations and creates certificate files
inside containers every time the code runs.
It uses a helper file called constants.py to hold definitions of data extracts
and lists of column names.
Downloaded files do not have headers, so this code depends on the column names being
in the same order as the data extracts defined in MasterTax.
This connector does not make use of state.
"""

# Import requests to make HTTP calls to API
from time import sleep
import requests as rq
import traceback
import datetime
import time
import os
import json
import csv
import uuid
import zipfile

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector  # For supporting Connector operations like Update() and Schema()
from fivetran_connector_sdk import Operations as op  # For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Logging as log  # For enabling Logs in your connector code

from constants import column_names, data_extracts

cert_path = "SSL.crt"
key_path = "SSL_auth.key"

def schema(configuration: dict):
    """
    # Define the schema function which lets you configure the schema your connector delivers.
    # See the technical reference documentation for more details on the schema function:
    # https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    :param configuration: dictionary with secrets and certificate files (not used)
    :return: a list of tables with primary keys and any datatypes that we want to specify
    """
    return [{"table": "extract_01", "primary_key": ["id"]}]

def update(configuration: dict, state: dict):
    """
    Create headers and then iterate through extracts specified in data_extracts list
    :param configuration: dictionary with secrets and certificate files
    :param state: dictionary containing whatever state you have chosen to checkpoint during the prior sync (not used)
    :return:
    """
    try:
        token_header = make_headers(configuration)

        for e in data_extracts:
            yield from sync_items(configuration, token_header, e)

        # Yield a checkpoint operation to save the new state.
        yield op.checkpoint({})

    except Exception as e:
        # Return error response
        exception_message = str(e)
        stack_trace = traceback.format_exc()
        detailed_message = f"Error Message: {exception_message}\nStack Trace:\n{stack_trace}"
        raise RuntimeError(detailed_message)

def sync_items(configuration: dict, headers: dict, extract: dict):
    """
    For the specified extract, submit a request, then download the file once it is ready.
    Once the file is downloaded, call the upsert_rows() function to send the rows to Fivetran.
    :param configuration: dictionary with secrets and certificate files
    :param headers: authentication headers for API
    :param extract: dictionary defining the extract to pull from MasterTax
    :return:
    """
    # Get response from API call.
    complete = False
    conversation_id = str(uuid.uuid4())
    headers["ADP-ConversationID"] = conversation_id

    submit_endpoint = "/tax/v1/organization-tax-data/processing-jobs/actions/submit"
    base_url = "https://api.adp.com"
    status, resource_id = submit_process(base_url + submit_endpoint, headers, extract)
    log.info(status)
    status_endpoint = f"/tax/v1/organization-tax-data/processing-jobs/{resource_id}/processing-status?processName=DATA_EXTRACT"

    while not complete:
        sleep(10)
        log.info(f"checking export status for {conversation_id}")
        status, output_id = get_process_status(base_url + status_endpoint, headers)
        if status == "completed":
            complete = True
            log.info(f"process complete for {extract}")
            content_endpoint = f"/tax/v1/organization-tax-data/processing-job-outputs/{output_id}/content?processName=DATA_EXTRACT"
            download_file(base_url + content_endpoint, headers, "test")

    zip_path = "test.zip"
    extract_path = configuration["unzipDirectory"]

    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(extract_path)

    print(f"ZIP file extracted to: {extract_path}")

    layout_name = next(
        (tag["tagValues"][0] for tag in extract.get("processDefinitionTags", [])
         if tag.get("tagCode") == "LAYOUT_NAME"),
        None  # Default if not found
    )
    matching_files = [fn for fn in os.listdir(extract_path) if layout_name in fn]

    for m in matching_files:
        log.fine(f"processing {m}")
        yield from upsert_rows(f"{extract_path}{m}", layout_name)

    yield op.checkpoint({})

def upsert_rows(filename: str, layout_name: str):
    """
    Gets column names for current layout and zips to each row, then upserts
    :param filename: name of file to process
    :param layout_name: name of layout to get column names for
    :return:
    """

    colnames = column_names[layout_name]
    log.fine(f"upserting rows for {filename}")
    with open(filename, "r", newline="", encoding="utf-8") as file:
        reader = csv.reader(file, delimiter="\t")  # Tab-delimited

        for row in reader:
            yield op.upsert(table=layout_name, data=dict(zip(colnames, row)))

def submit_process(url: str, headers: dict, payload: dict):
    """
    Submits a data extract request and gets a resource_id to be used to check the status in subsequent calls
    :param url: The URL to which the API request is made.
    :param headers: A dictionary of headers for authorization
    :param payload: A dictionary with parameters for the extract to submit
    :return: status and resource_id
    """
    retry_wait_seconds = 300
    max_retries = 3

    for attempt in range(max_retries + 1):
        response = rq.post(url, headers=headers, data=json.dumps(payload), cert=(cert_path, key_path))

        if response.status_code == 400 and attempt < max_retries:
            log.info(f"Received 400 response, waiting {retry_wait_seconds} seconds to retry...")
            log.info(f"response: {response.json()}")
            time.sleep(retry_wait_seconds)
            continue  # Retry the request

        if response.status_code != 202:
            log.info(str(response.json()))

        response.raise_for_status()  # Ensure we raise an exception for HTTP errors.
        response_page = response.json()
        log.fine(response_page)
        status = response_page.get("_confirmMessage", {}).get("requestStatus")
        resource_id = response_page.get("_confirmMessage", {}).get("messages", [{}])[0].get("resourceID")

        return status, resource_id

def get_process_status(url: str, headers: dict):
    """
    Gets the status and output_id for submitted process specified in URL
    output_id is used to download files in the next step.

    :param url: The URL to which the API request is made, containing resource_id .
    :param headers: A dictionary of headers for authorization
    :return: status and output_id
    """
    response = rq.get(url, headers=headers, cert=(cert_path, key_path))
    if response.status_code != 200:
        log.info(str(response))
    response.raise_for_status()  # Ensure we raise an exception for HTTP errors.
    response_page = response.json()
    status = response_page.get("processingJob").get("processingJobStatusCode")
    output_id = response_page.get("processingJob").get("processOutputID")

    return status, output_id

def download_file(url: str, headers: dict, name: str):
    """
    Downloads zip file with specified name
    :param url: The URL to which the API request is made, containing ID of file to download
    :param headers: A dictionary of headers for authorization
    :param name: name of file to create
    :return:
    """
    headers["Range"] = "bytes=0-"
    response = rq.get(url, headers=headers, stream=True, cert=(cert_path, key_path))
    if response.status_code != 200:
        log.info(str(response))
    response.raise_for_status()  # Ensure we raise an exception for HTTP errors.

    with open(f"{name}.zip", "wb") as file:
        for chunk in response.iter_content(chunk_size=1024):
            file.write(chunk)

    log.info("ZIP file downloaded successfully!")

def make_headers(conf: dict):
    """
    Create authentication headers.

    :param conf: Dictionary containing authentication details.
    :param state: Dictionary storing token and expiration details.
    :return: headers
    """

    url = "https://api.adp.com/auth/oauth/v2/token"
    write_to_file(conf["crtFile"], cert_path)
    write_to_file(conf["keyFile"], key_path)
    payload = f"grant_type=client_credentials&client_id={conf['clientId']}&client_secret={conf['clientSecret']}"
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Accept': 'application/json'
    }

    # Request a new token
    try:
        auth_response = rq.request("POST", url, headers=headers, data=payload,
                                   cert=("SSL.crt", "SSL_auth.key"))
        auth_response.raise_for_status()
        auth_page = auth_response.json()

        # Extract token safely
        auth_token = auth_page.get("access_token")

        if not auth_token:
            raise ValueError("Authentication failed: accessToken missing in response")

        return {"Authorization": f"Bearer {auth_token}", "Content-Type": "application/json"}

    except rq.exceptions.RequestException as e:
        raise RuntimeError(f"❌ Failed to authenticate: {e}")

def write_to_file(text: str, filename: str):
    """
    Writes the given text to a .pem file.

    :param text: The text to be written to the file
    :param filename: The name of the file
    """
    try:
        with open(filename, "w") as pem_file:
            pem_file.write(text)
        log.fine(f"Successfully written to {filename}")
    except Exception as e:
        log.info(f"Error writing to file: {e}")

# This creates the connector object that will use the update function defined in this connector.py file.
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "main":
    # Open the configuration.json file and load its contents into a dictionary.
    with open("configuration.json", 'r') as f:
        configuration = json.load(f)
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from your IDE.
    connector.debug(configuration=configuration)

