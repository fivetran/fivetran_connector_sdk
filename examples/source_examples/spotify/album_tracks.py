# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details

# Import requests to make HTTP calls to API
import requests as rq
import traceback
import json
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

# Define the schema function which lets you configure the schema your connector delivers.
# See the technical reference documentation for more details on the schema function:
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
# The schema function takes one parameter:
# - configuration: a dictionary that holds the configuration settings for the connector.
def schema(configuration: dict):

    return [
        {
            "table": "album",
            "primary_key": ["id"]
        },
        {
            "table": "track",
            "primary_key": ["id"]
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
    # business_cursor = state["business_cursor"] if "business_cursor" in state else '0001-01-01T00:00:00Z'
    # department_cursor = state["department_cursor"] if "department_cursor" in state else {}

    try:
        conf = configuration
        client_id = conf['client_id']
        client_secret = conf['client_secret']
        artist_url = conf['artist_url']
        album_params = {"artist_id": artist_url}
        auth_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
        sp = spotipy.Spotify(auth_manager=auth_manager)

        yield from sync_items(sp, album_params)

    except Exception as e:
        # Return error response
        exception_message = str(e)
        stack_trace = traceback.format_exc()
        detailed_message = f"Error Message: {exception_message}\nStack Trace:\n{stack_trace}"
        raise RuntimeError(detailed_message)


# The function takes three parameters:
# - obj: The spotify connection object.
# - payload: A dictionary of query parameters send with the method, if needed
def sync_items(obj, payload):

    try:
    # For this artist, get one page of albums in a response from the API call.
        albums_page = get_api_response(obj, "artist_albums", payload)
        albums = albums_page["items"]
        for a in albums:
            album_data = remove_lists(a)
            album_name = album_data["name"]
            log.fine(f"adding album {album_name}")
            yield op.upsert(table="album", data=album_data)

            # For the current album, get one page of tracks in a response from the API call.
            track_params = {"album_id": a["id"]}
            tracks_page = get_api_response(obj, "album_tracks", track_params)
            tracks = tracks_page["items"]
            for t in tracks:
                track_data = remove_lists(t)
                yield op.upsert(table="track", data=track_data)

        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of interruptions.
        yield op.checkpoint({})

    except Exception as e:
        # Return error response
        exception_message = str(e)
        stack_trace = traceback.format_exc()
        detailed_message = f"Error Message: {exception_message}\nStack Trace:\n{stack_trace}"
        raise RuntimeError(detailed_message)

# The get_api_response function uses the Spotipy python library to get API response from Spotify.
#
# The function takes three parameters:
# - obj: The spotify connection object.
# - method_name: The spotipy method to use
# - payload: a dictionary of parameters to send with the method, if needed
#
# Returns:
# - response_page: A dictionary containing the parsed JSON response from the API.
def get_api_response(obj, method_name, payload=None):
    if payload is None:
        payload = {}
    method = getattr(obj, method_name)
    response_page = method(**payload)
    return response_page

# The remove_lists function removes keys from a dictionary if the value is a list
#
# The function takes one parameter:
# - d: a dictionary
#
# Returns:
# - new_dict: A dictionary without any values that are lists
def remove_lists(d):
    new_dict = {}
    for key, value in d.items():
        if isinstance(value, list):
            pass
        else:
            new_dict[key] = value

    return new_dict

# This creates the connector object that will use the update function defined in this connector.py file.
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module. This is Python's standard entry method allowing your script to
# be run directly from the command line or IDE 'run' button. This is useful for debugging while you write your code.
# Note this method is not called by Fivetran when executing your connector in production. Please test using the
# Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "main":
    # Open the configuration.json file and load its contents into a dictionary.
    with open("configuration.json", 'r') as f:
        configuration = json.load(f)
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from your IDE.
    connector.debug(configuration=configuration)

