# This example implements a **connector** to fetch, process, and store data from the
# National Parks Service API (NPS API)(https://www.nps.gov/subjects/developer/index.htm).
# The process is built with an Object-Oriented Programming (OOP) approach, ensuring
# modular, maintainable, and reusable code.
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details.

# Import required classes from fivetran_connector_sdk
# For supporting Connector operations like Update() and Schema()
from fivetran_connector_sdk import Connector

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# Import required packages
import json

# Import self written modules
from alerts_table import ALERTS
from park_table import PARKS
from articles_table import ARTICLES
from people_table import PEOPLE

selected_table = [PARKS, PEOPLE, ALERTS, ARTICLES]

# Define the schema function which lets you configure the schema your connector delivers.
# See the technical reference documentation for more details on the schema function:
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
# The schema function takes one parameter:
# - configuration: a dictionary that holds the configuration settings for the connector.


def schema(configuration: dict):
    output = []
    for table in selected_table:
        con = table(configuration=configuration)
        schema_dict = con.assign_schema()
        output.append(schema_dict)
    return output


# Define the update function, which is a required function, and is called by Fivetran during each sync.
# See the technical reference documentation for more details on the update function:
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
# The function takes two parameters:
# - configuration: dictionary containing any secrets or payloads you configure when deploying the connector.
# - state: a dictionary containing the state checkpointed during the prior sync.
#   The state dictionary is empty for the first sync or for any full re-sync.


def update(configuration: dict, state: dict):
    for table in selected_table:
        con = table(configuration=configuration)
        data = con.process_data()
        for row in data:
            op.upsert(table.path(), row)


# Create the connector object for Fivetran.
connector = Connector(update=update, schema=schema)

# Run the connector in debug mode
if __name__ == "__main__":
    print("Running the NPS connector (Parks, Articles, People, and Alerts tables)...")

    # Open the configuration.json file and load its contents into a dictionary.
    with open("configuration.json", "r") as f:
        configuration = json.load(f)
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from your IDE.
    connector.debug(configuration=configuration)
    print("Connector run complete.")
