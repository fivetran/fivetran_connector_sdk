
from fivetran_connector_sdk import Logging as log
import random
import string
from datetime import datetime, timedelta, timezone

import connector

mock_id = 1

# The get_api_response function sends an HTTP GET request to the provided URL with the specified parameters.
# It performs the following tasks:
# 1. Logs the URL and query parameters used for the API call for debugging and tracking purposes.
# 2. Makes the API request using the 'requests' library, passing the URL and parameters.
# 3. Parses the JSON response from the API and returns it as a dictionary.
#
# The function takes two parameters:
# - base_url: The URL to which the API request is made.
# - params: A dictionary of query parameters to be included in the API request.
#
# Returns:
# - response_page: A dictionary containing the parsed JSON response from the API.
def get_mock_api_response(base_url, params):
    log.info(f"Making API call to url: {base_url} with params: {params}")
    global mock_id
    updated_since = datetime.fromisoformat(params['updated_since']).astimezone(timezone.utc)
    until = updated_since + timedelta(days=1)
    response_page = {
        "data": []
    }
    while updated_since <= until:
        response_page['data'].append(
            {
                "mock_id": str(mock_id),
                "name": string_generator(),
                "email": string_generator(),
                "address": string_generator(),
                "company": string_generator(),
                "job": string_generator(),
                "updatedAt": updated_since.isoformat(),
                "createdAt": connector.formatIsoDatetime(updated_since),
            }
        )
        mock_id += 1
        updated_since = updated_since + timedelta(hours=1)
    if updated_since < connector.syncStart:
        response_page['has_more'] = True
    return response_page

def string_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))