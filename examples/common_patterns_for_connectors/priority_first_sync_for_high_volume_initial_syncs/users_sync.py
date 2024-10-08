from datetime import datetime, timezone

from fivetran_connector_sdk import Operations as op
import mock_api
import connector

def sync_users(base_url, params, state, is_historical_sync):
    more_data = True
    last_updated_at = params['updated_since']

    while more_data:
        # Get response from API call.
        response_page = mock_api.get_mock_api_response(base_url, params)

        # Process the items.
        items = response_page.get("data", [])
        if not items:
            break  # End pagination if there are no records in response.

        for user in items:
            yield op.upsert(table="user", data=user)
            last_updated_at = user["updated_at"] # Assuming the API returns the data in ascending order

        if is_historical_sync and connector.get_datetime_object(last_updated_at) >= connector.get_datetime_object(connector.get_pfs_historical_cursor(state, 'user')).astimezone(timezone.utc):
            connector.set_pfs_historical_cursor(state, 'user', params['updated_since'])
            break
        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of interruptions.
        yield op.checkpoint(state)

        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of interruptions.
        more_data = mock_api.should_continue_pagination(response_page)
        if more_data:
            params['updated_since'] = last_updated_at

    if is_historical_sync:
        connector.set_pfs_historical_cursor(state, 'user', params['updated_since'])
    else:
        connector.set_pfs_incremental_cursor(state, 'user', last_updated_at)