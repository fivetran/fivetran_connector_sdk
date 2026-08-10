"""
This example demonstrates op.warning() and op.error() in one deterministic flow.
It emits two warnings and then one terminal error for an empty primary key.
"""

# For reading mock weather records from a CSV file
import csv

# For parsing optional date values
from datetime import datetime

# For resolving local file paths relative to this connector file
from pathlib import Path

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

__TABLE_NAME = "weather"
__MOCK_CSV_PATH = Path(__file__).with_name("mock_weather.csv")
__DATE_FORMAT = "%Y-%m-%d"


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connector-sdk/technical-reference/connector-sdk-code/connector-sdk-methods#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    return [
        {
            "table": __TABLE_NAME,
            "primary_key": ["zipcode"],
            "columns": {
                "zipcode": "STRING",
                "city": "STRING",
                "weather": "STRING",
                "date": "NAIVE_DATE",
            },
        }
    ]


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    This example does not require any configuration values.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    return


def is_valid_optional_date(date_value: str):
    """
    Validate optional date values from source rows.
    Args:
        date_value: Date value from source row.
    Returns:
        True when date is empty or in YYYY-MM-DD format; otherwise False.
    """
    if not date_value:
        return False

    try:
        datetime.strptime(date_value, __DATE_FORMAT)
        return True
    except ValueError:
        return False


def read_mock_weather_rows():
    """
    Read source-like weather rows from mock CSV.
    Returns:
        List of row dictionaries loaded from mock_weather.csv.
    """
    if not __MOCK_CSV_PATH.exists():
        op.error(message=f"Mock CSV file was not found: {__MOCK_CSV_PATH.name}")
        return []

    with __MOCK_CSV_PATH.open("r", encoding="utf-8", newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        return list(reader)


def update(configuration: dict, state: dict):
    """
    Define the update function, which is a required function, and is called by Fivetran during each sync.
    See the technical reference documentation for more details on the update function
    https://fivetran.com/docs/connector-sdk/technical-reference/connector-sdk-code/connector-sdk-methods#update
    Args:
        configuration: A dictionary containing connection details
        state: A dictionary containing state information from previous runs
        The state dictionary is empty for the first sync or for any full re-sync
    """
    log.warning("Example: COMMON PATTERNS : ERROR AND WARNING OPERATIONS")

    validate_configuration(configuration=configuration)
    source_rows = read_mock_weather_rows()

    row_number = 0
    for row in source_rows:
        row_number += 1
        zipcode = str(row.get("zipcode", "")).strip()
        city = str(row.get("city", "")).strip()
        weather = str(row.get("weather", "")).strip()
        date_value = str(row.get("date", "")).strip()

        # Warning 1:
        # Empty city is a recoverable row-level issue, so this row is skipped.
        if not city:
            op.warning(
                message=(f"Warning 1 of 2: city is empty for zipcode '{zipcode}'. " "Row skipped.")
            )
            continue

        # Warning 2:
        # Invalid optional date format is non-fatal, so this row is skipped.
        if not is_valid_optional_date(date_value=date_value):
            op.warning(
                message=(
                    f"Warning 2 of 2: invalid optional date format for zipcode '{zipcode}'. "
                    f"Expected YYYY-MM-DD, got '{date_value}'. Row skipped."
                )
            )
            continue

        # Final terminal error:
        # Empty primary key means record identity is invalid, so sync must stop.
        if not zipcode:
            op.error(
                message=(
                    "Primary key 'zipcode' is empty in source data. "
                    "Stopping sync to avoid writing non-identifiable rows."
                ),
                trace=(f"Empty zipcode check at row {row_number}."),
            )
            return

        output_record = {
            "zipcode": zipcode,
            "city": city,
            "weather": weather,
            "date": date_value,
        }

        # The 'upsert' operation is used to insert or update data in the destination table.
        # The first argument is the name of the destination table.
        # The second argument is a dictionary containing the record to be upserted.
        op.upsert(table=__TABLE_NAME, data=output_record)

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
    # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
    op.checkpoint(state=state)


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
#
# IMPORTANT: The recommended way to test your connector is using the Fivetran debug command:
#   fivetran debug
#
# This local testing block is provided as a convenience for quick debugging during development,
# such as using IDE debug tools (breakpoints, step-through debugging, etc.).
# Note: This method is not called by Fivetran when executing your connector in production.
# Always test using 'fivetran debug' prior to finalizing and deploying your connector.
if __name__ == "__main__":
    connector.debug()
