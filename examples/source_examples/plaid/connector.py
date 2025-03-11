# This is a simple example for how to work with the fivetran_connector_sdk module.
# This is an example to show how we can sync records using Plaid API via Connector SDK.
# You need to provide your credentials for this example to work.
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
import json

from plaid import ApiClient, Configuration, Environment
from plaid.api import plaid_api
from plaid.model.accounts_get_request import AccountsGetRequest
from fivetran_connector_sdk import Connector, Operations as op


def initialize_plaid_client(configuration):
    plaid_configuration = Configuration(
        host=Environment.Production,
        api_key={
            "clientId": configuration["client_id"],
            "secret": configuration["client_secret"],
        },
    )
    plaid_client = ApiClient(plaid_configuration)
    return plaid_api.PlaidApi(plaid_client)


# Define the schema function, which lets you configure the schema your connector delivers.
# See the technical reference documentation for more details on the schema function:
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
# The schema function takes one parameter:
# - configuration: a dictionary that holds the configuration settings for the connector.
def schema(configuration: dict):
    return [
        {
            "table": "accounts",  # Name of the table in the destination.
            "primary_key": ["account_id"],  # Primary key column(s) for the table.
            "columns": {  # Define the columns and their data types.
                "account_id": "STRING",  # String column for the account_id.
                "holder_category": "STRING",  # String column for the holder_category.
                "mask": "STRING",  # String column for the mask.
                "name": "STRING",  # String column for the name.
                "official_name": "STRING",  # String column for the official_name.
                "persistent_account_id": "STRING",  # String column for the persistent_account_id.
                "subtype": "STRING",  # String column for the subtype.
                "type": "STRING",  # String column for the type.
            },
        }
    ]


# Define the update function, which is a required function, and is called by Fivetran during each sync.
# See the technical reference documentation for more details on the update function
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
# The function takes two parameters:
# - configuration: a dictionary that contains any secrets or payloads you configure when deploying the connector
# - state: a dictionary that contains whatever state you have chosen to checkpoint during the prior sync.
# The state dictionary is empty for the first sync or for any full re-sync.
def update(configuration: dict, state: dict):
    plaid_api_client = initialize_plaid_client(configuration)
    try:
        plaid_access_token = configuration["plaid_access_token"]

        if not plaid_access_token:
            raise ValueError("Missing plaid_access_token in configuration")

        request = AccountsGetRequest(access_token=plaid_access_token)
        response = plaid_api_client.accounts_get(request)

        for account in response["accounts"]:
            yield op.upsert(
                table="accounts",
                data={
                    "account_id": account.get("account_id"),
                    "holder_category": account.get("holder_category"),
                    "mask": account.get("mask"),
                    "name": account.get("name"),
                    "official_name": account.get("official_name"),
                    "persistent_account_id": account.get("persistent_account_id"),
                    "subtype": account.get("subtype"),
                    "type": account.get("type")
                }
            )
    except Exception as e:
        print(f"Error fetching accounts: {e}")


# This creates the connector object that will use the update function defined in this connector.py file.
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Test it by using the `debug` command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents into a dictionary.
    with open("configuration.json", 'r') as f:
        configuration = json.load(f)
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from your IDE:
    connector.debug(configuration=configuration)
