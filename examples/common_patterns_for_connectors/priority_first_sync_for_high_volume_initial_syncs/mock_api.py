
from fivetran_connector_sdk import Logging as log
import random
import string
from datetime import datetime, timedelta, timezone

import connector

mock_id = 1

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
                "id": str(mock_id),
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