# This is an example for how to work with the fivetran_connector_sdk module.
# The example demonstrates how to extract data from a PDF file stored in AWS S3 bucket using pdfplumber and regex.
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details


# Import required classes from fivetran_connector_sdk
# For supporting Connector operations like Update() and Schema()
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

import json  # For parsing JSON data
import os  # For file path operations
import boto3  # For AWS S3 operations
import tempfile  # For creating temporary files
from process_pdf import PDFInvoiceExtractor  # For extracting data from PDF files


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    This function is called at the start of the update method to ensure that the connector has all necessary configuration values.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing.
    """
    required_configs = ["aws_access_key_id", "aws_secret_access_key", "region_name", "bucket_name"]
    for key in required_configs:
        if key not in configuration:
            raise ValueError(f"Missing required configuration value: {key}")


def create_s3_client(configuration: dict):
    """
    This method is used to create an S3 client using the boto3 library.
    The method takes the configuration dictionary as a parameter and returns the S3 client.
    The S3 client is used to interact with the AWS S3 service.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector, including AWS credentials and region.
    Returns:
        client: An S3 client configured with the provided AWS credentials and region.
    """
    client = boto3.client(
        "s3",
        aws_access_key_id=configuration["aws_access_key_id"],
        aws_secret_access_key=configuration["aws_secret_access_key"],
        region_name=configuration["region_name"],
    )
    log.info("S3 client created successfully")
    return client


def get_invoice_file_pairs(s3_client, bucket_name: str, prefix: str):
    """
    This method is used to get the list of PDF invoices from the S3 bucket.
    The method takes the S3 client, bucket name, and prefix as parameters.
    You can modify this method to select and process only specific files based on your requirements.
    Args:
        s3_client: S3 client to interact with AWS S3 service.
        bucket_name: name of the S3 bucket to list files from.
        prefix: prefix in the S3 bucket to filter files.
    Returns:
        A list which contains the file key of the invoice pdf files.
    """
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    invoice_files = []
    if "Contents" in response:
        for obj in response["Contents"]:
            if obj["Key"].endswith(".pdf"):
                # If the file is a PDF, append the file key to the list
                invoice_files.append(obj["Key"])
    return invoice_files


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """

    return [
        {
            "table": "invoices",  # Name of the table in the destination, required.
            "primary_key": ["invoice_id"],  # Primary key column(s) for the table, optional.
            "columns": {  # Definition of columns and their types, optional.
                "invoice_id": "STRING",  # Contains a dictionary of column names and data types
                "total_amount": "FLOAT",
                "invoice_date": "NAIVE_DATE",
                "due_date": "NAIVE_DATE",
                "amount_in_words": "STRING",
            },  # For any columns whose names are not provided here, their data types will be inferred
        },
    ]


def get_prefix_from_configuration(configuration: dict) -> str:
    """
    Get the prefix from configuration or use default as root if not specified.
    This is useful for listing files in a specific folder in the S3 bucket.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Returns:
        The prefix to use for S3 file listing
    """
    if "prefix" not in configuration:
        log.warning("prefix is not set in the configuration, defaulting to '/' prefix.")
        return "/"
    return configuration.get("prefix")


def process_single_pdf(s3_client, bucket_name: str, file_key: str, pdf_processor):
    """
    This function downloads a PDF file from S3, processes it using the PDFInvoiceExtractor instance
    The extracted data is then upserted into the "invoices" table.
    It also handles the creation of a temporary file for the downloaded PDF and cleans up after processing
    Args:
        s3_client: S3 client to download the file
        bucket_name: Name of the S3 bucket
        file_key: Key of the file in S3
        pdf_processor: Initialized PDFInvoiceExtractor instance
    """
    # Create a temporary file for the downloaded PDF
    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as temp_file:
        temp_file_path = temp_file.name

    try:
        # Download the file from S3
        log.info(f"Downloading {file_key} from S3 bucket {bucket_name}")
        s3_client.download_file(bucket_name, file_key, temp_file_path)

        # Process the PDF file
        log.info(f"Processing {file_key}")
        result = pdf_processor.process_pdf(temp_file_path)
        # The op.upsert method is called with two arguments:
        # - The first argument is the name of the table to upsert the data into.
        # - The second argument is a dictionary containing the data to be upserted,
        op.upsert(table="invoices", data=result)

    except Exception as e:
        log.severe(f"Error processing {file_key}: {str(e)}")
    finally:
        # Delete the downloaded invoice file after processing
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)
            log.info(f"Deleted temporary file for {file_key}")


def process_all_pdfs(s3_client, bucket_name: str, prefix: str):
    """
    Process all PDF files found in the specified S3 bucket and prefix.
    Args:
        s3_client: S3 client to interact with AWS S3
        bucket_name: Name of the S3 bucket
        prefix: Prefix path in the S3 bucket
    """
    # Get the list of invoice PDF files
    invoice_files = get_invoice_file_pairs(s3_client, bucket_name, prefix)
    log.info(f"Found {len(invoice_files)} PDF invoices in {prefix} folder")

    # Initialize PDF processor
    pdf_processor = PDFInvoiceExtractor()

    # Process each PDF file
    for file_key in invoice_files:
        process_single_pdf(s3_client, bucket_name, file_key, pdf_processor)


def update(configuration: dict, state: dict):
    """
     Define the update function, which is a required function, and is called by Fivetran during each sync.
    See the technical reference documentation for more details on the update function
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    Args:
        configuration: A dictionary containing connection details
        state: A dictionary containing state information from previous runs
        The state dictionary is empty for the first sync or for any full re-sync
    """
    log.warning("Example: Common Pattern for Connectors Examples - Extracting Data from PDF")

    # Validate the configuration to ensure it contains all required values.
    validate_configuration(configuration=configuration)

    # create S3 client using the configuration
    s3_client = create_s3_client(configuration=configuration)
    bucket_name = configuration["bucket_name"]

    # get the prefix from the configuration or use default as root
    prefix = get_prefix_from_configuration(configuration=configuration)

    # Process all PDF invoices in the specified S3 bucket and prefix
    process_all_pdfs(s3_client=s3_client, bucket_name=bucket_name, prefix=prefix)

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
    op.checkpoint(state)


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents
    with open("configuration.json", "r") as f:
        configuration = json.load(f)

    # Test the connector locally
    connector.debug(configuration=configuration)
