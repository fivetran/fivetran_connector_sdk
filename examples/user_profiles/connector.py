from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Operations as op
from fivetran_connector_sdk import Logging as log
import requests as rq
import pandas as pd


def schema(configuration: dict):
    return [
        {
            "table": "profile",
            "primary_key": ["id"],
            "columns": {
                "id": "INT",
                "gender": "STRING",
                "fullName": "STRING",
                "email": "STRING",
                "age": "INT",
                "date": "STRING",
                "mobile": "STRING",
                "nationality": "STRING"
            }

        },
        {
            "table": "location",
            "primary_key": ["profileId"],
            "columns": {
                "profileId": "INT",
                "street": "STRING",
                "city": "STRING",
                "state": "STRING",
                "country": "STRING",
                "postcode": "STRING"
            }
        },
        {
            "table": "Login",
            "primary_key": ["profileId"],
            "columns": {
                "profileId": "INT",
                "uuid": "STRING",
                "username": "STRING",
                "password": "STRING"
            }
        }
    ]


def update(configuration: dict, state: dict):
    profile_cursor = state["profile_cursor"] if "profile_cursor" in state else 0

    profile_df, location_df, login_df, cursor = get_data(profile_cursor)

    # UPSERT all profile table data and checkpointing after every 5 records
    for index, row in profile_df.iterrows():
        yield op.upsert("profile", {col: row[col] for col in profile_df.columns})
        if index % 5 == 0:
            state["profile_cursor"] = row["id"]
            yield op.checkpoint(state)
    # Checkpointing at last
    state["profile_cursor"] = cursor
    yield op.checkpoint(state)

    # Upserting data in "location" table in below loop
    for index, row in location_df.iterrows():
        yield op.upsert("location", {col: row[col] for col in location_df.columns})
        if index % 5 == 0:
            state["location_cursor"] = row["profileId"]
            yield op.checkpoint(state)
    # Checkpointing at last
    state["location_cursor"] = cursor
    yield op.checkpoint(state)

    # Upserting data in "login" table in below loop
    for index, row in login_df.iterrows():
        yield op.upsert("login", {col: row[col] for col in login_df.columns})
        if index % 5 == 0:
            state["login_cursor"] = row["profileId"]
            yield op.checkpoint(state)
    # Checkpointing at last
    state["login_cursor"] = cursor
    yield op.checkpoint(state)


def get_data(cursor):
    profile_df = pd.DataFrame([])
    location_df = pd.DataFrame([])
    login_df = pd.DataFrame([])

    for i in range(10):
        # Increment primary key
        cursor += 1

        # Request for fetching profile data
        response = rq.get("https://randomuser.me/api/")

        # extract and process data
        data = response.json()
        data = data["results"][0]

        # get "profile" table data
        profile_data = {
            "id": cursor,
            "fullName": data["name"]["title"] + " " + data["name"]["first"] + " " + data["name"]["last"],
            "gender": data["gender"],
            "email": data["email"],
            "age": data["dob"]["age"],
            "date": data["dob"]["date"],
            "mobile": data["cell"],
            "nationality": data["nat"]
        }
        profile_df = pd.concat([profile_df, pd.DataFrame([profile_data])], ignore_index=True)

        # get "location" table data
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

        # get "Login" table data
        login_details = data["login"]
        login_data = {
            "profileId": cursor,
            "uuid": login_details["uuid"],
            "username": login_details["username"],
            "password": login_details["password"]
        }
        login_df = pd.concat([login_df, pd.DataFrame([login_data])], ignore_index=True)

    return profile_df, location_df, login_df, cursor


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    connector.debug()
