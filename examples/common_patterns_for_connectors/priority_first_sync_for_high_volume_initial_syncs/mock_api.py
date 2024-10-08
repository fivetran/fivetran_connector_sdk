
from fivetran_connector_sdk import Logging as log
import random
import string
from datetime import datetime, timedelta, timezone

import connector

def get_mock_api_response(base_url, params):
    log.info(f"Making API call to url: {base_url} with params: {params}")
    updated_since = datetime.fromisoformat(params['updated_since']).astimezone(timezone.utc)
    until = updated_since + timedelta(days=1)
    response_page = {
        "data": []
    }
    while updated_since <= until:
        response_page['data'].append(
            {
                "id": string_generator(),
                "name": string_generator(chars=string.ascii_lowercase),
                "email": string_generator(chars=string.ascii_lowercase),
                "address": string_generator(chars=string.ascii_lowercase),
                "company": string_generator(chars=string.ascii_lowercase),
                "job": string_generator(chars=string.ascii_lowercase),
                "updated_at": updated_since.isoformat(),
                "created_at": connector.formatIsoDatetime(updated_since),
            }
        )
        updated_since = updated_since + timedelta(hours=1)
    if updated_since < connector.syncStart:
        response_page['has_more'] = True
    return response_page

def string_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))

def should_continue_pagination(response_page):
    return response_page.get("has_more")