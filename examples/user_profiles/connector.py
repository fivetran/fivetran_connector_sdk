# This is a simple example for how to work with the fivetran_connector_sdk module.
# It shows the use of a requirements.txt file and a connector that calls a publicly available API to get random profile data
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Operations as op
import requests as rq # Import the requests module for making HTTP requests, aliased as rq.
import pandas as pd

# Define the schema function which lets you configure the schema your connector delivers.
# See the technical reference documentation for more details on the schema function:
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
# The schema function takes one parameter:
# - configuration: a dictionary that holds the configuration settings for the connector.
def schema(configuration: dict):
    return [
        {
            "table": "profile",
            "primary_key": ["id"],
            # Columns and data types will be inferred by Fivetran
        },
        {
            "table": "location",
            "primary_key": ["profileId"],
            # Columns and data types will be inferred by Fivetran
        },
        {
            "table": "login",
            "primary_key": ["profileId"],
            # Columns and data types will be inferred by Fivetran
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
    # Retrieve the last processed profile ID from state or set it to 0 if not present
    profile_cursor = state["profile_cursor"] if "profile_cursor" in state else 0

    # Fetch new data and return DataFrames for profile, location, and login tables
    profile_df, location_df, login_df, cursor = get_data(profile_cursor)


    # Approaches to upsert the records from dataFrame

    # APPROACH - 1
    # UPSERT all profile table data, checkpoint periodically to save progress. In this example every 5 records.
    for index, row in profile_df.iterrows():
        yield op.upsert("profile", {col: row[col] for col in profile_df.columns})
        if index % 5 == 0:
            state["profile_cursor"] = row["id"]
            yield op.checkpoint(state)
    
    # Checkpointing at the end of the "profile" table data processing
    state["profile_cursor"] = cursor
    yield op.checkpoint(state)

    # APPROACH - 2
    # UPSERT all location table data.
    # Iterate over each row in the DataFrame, converting it to a dictionary
    for row in location_df.to_dict("records"):
        yield op.upsert("location", row)

    # Checkpointing at the end of the "location" table data processing
    state["location_cursor"] = cursor
    yield op.checkpoint(state)

    # APPROACH - 3
    # UPSERT all login table data.
    # Iterate over the values of the dictionary (which are the DataFrame rows)
    for value in login_df.to_dict("index").values():
        yield op.upsert("login", value)
    
    # Checkpointing at the end of the "login" table data processing
    state["login_cursor"] = cursor
    yield op.checkpoint(state)


# Function to fetch data from an API and process it into DataFrames
def get_data(cursor):
    # Initialize empty DataFrames for profile, location, and login tables
    profile_df = pd.DataFrame([])
    location_df = pd.DataFrame([])
    login_df = pd.DataFrame([])

    # Fetch data for 10 iterations, each with a new profile
    for i in range(2):
        # Increment primary key
        cursor += 1

        # Request data from an external API
        response = rq.get("https://randomuser.me/api/")
        data = response.json()
        data = data["results"][0]

        # Process and store profile data
        profile_data = {
            "id": cursor,
            "fullName": data["name"]["title"] + " " + data["name"]["first"] + " " + data["name"]["last"],
            "gender": data["gender"],
            "email": data["email"],
            "age": data["dob"]["age"],
            "date": data["dob"]["date"],
            "mobile": data["cell"],
            "nationality": data["nat"],
            "picture": data["picture"] # Stores the JSON data e.g. {"large":"https://randomuser.me/api/portraits/women/67.jpg","medium":"https://randomuser.me/api/portraits/med/women/67.jpg","thumbnail":"https://randomuser.me/api/portraits/thumb/women/67.jpg"}
        }
        profile_df = pd.concat([profile_df, pd.DataFrame([profile_data])], ignore_index=True)

        # Process and store location data
        location_details = data["location"]
        location_data = {
            "profileId": cursor,
            "street": str(location_details["street"]["number"]) + " " + location_details["street"]["name"],
            "city": location_details["city"],
            "state": location_details["state"],
            "country": location_details["country"],
            "postcode": location_details["postcode"]
        }
        location_df = pd.concat([location_df, pd.DataFrame([location_data])], ignore_index=True)

        # Process and store login data
        login_details = data["login"]
        login_data = {
            "profileId": cursor,
            "uuid": login_details["uuid"],
            "username": login_details["username"],
            "password": login_details["password"]
        }
        login_df = pd.concat([login_df, pd.DataFrame([login_data])], ignore_index=True)

    return profile_df, location_df, login_df, cursor

# This creates the connector object that will use the update and schema functions defined in this connector.py file.
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the "fivetran debug" command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from your IDE.
    connector.debug()
