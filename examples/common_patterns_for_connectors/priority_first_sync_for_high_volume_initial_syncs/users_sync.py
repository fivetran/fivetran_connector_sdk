from datetime import datetime, timezone

from fivetran_connector_sdk import Operations as op
import connector
import mock_api

def sync_users(base_url, params, state, is_backward_sync):
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
            last_updated_at = user["updatedAt"] # Assuming the API returns the data in ascending order

        if is_backward_sync and datetime.fromisoformat(last_updated_at) >= datetime.fromisoformat(state['user']['backward_cursor']).astimezone(timezone.utc):
            state['user']['backward_cursor'] = params['updated_since']
            break
        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of interruptions.
        yield op.checkpoint(state)

        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of interruptions.
        more_data = connector.should_continue_pagination(response_page)
        if more_data:
            params['updated_since'] = last_updated_at

    if is_backward_sync:
        state['user']['backward_cursor'] = params['updated_since']
    else:
        state['user']['forward_cursor'] = last_updated_at