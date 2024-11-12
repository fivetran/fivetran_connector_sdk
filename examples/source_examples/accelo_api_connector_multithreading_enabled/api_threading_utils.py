# ***Multithreading Guidelines***
#
# Follow below points to avoid data integrity issues and race conditions which are hard to debug!!!
# * Never call fivetran_connector_sdk Operations inside multithreaded function
#   as this requires synchronising operations like checkpoint.
#   This is hard to do and debug, so avoid it.
# * Use multithreading only to make parallel api calls and get the responses,
#   else multithreading can result in race conditions which are hard to debug and may not throw an error.
#
# This is a simple e.g. how to do multithreading to make parallel api calls to improve sync performance.
from fivetran_connector_sdk import Logging as log
import constants
import requests
import time
from concurrent.futures import ThreadPoolExecutor

executor = ThreadPoolExecutor(max_workers=constants.MAX_WORKERS)

# Util function to make api calls in parallel using threadPoolExecutor
def make_api_calls_in_parallel(page, fetch_page):
    futures = [executor.submit(fetch_page, p) for p in range(page, page + constants.MAX_WORKERS)]
    results = [future.result() for future in futures]
    return results

# Util function to make an api call and return response
def fetch_data(endpoint, access_token, params=None, timeout=constants.REQUEST_TIMEOUT, retries=constants.RETRIES):
    """
    Fetch data from the Accelo API with retry logic.

    Args:
        endpoint (str): The API endpoint to fetch data from.
        access_token (str): The OAuth 2.0 access token.
        params (dict, optional): Query parameters for the API request.
        timeout (int, optional): Request timeout in seconds.
        retries (int, optional): Number of retry attempts.

    Returns:
        list: The JSON response data or None if all retries fail.
    """
    url = f"{constants.BASE_URL}/{endpoint}"
    headers = {"Authorization": f"Bearer {access_token}"}

    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=timeout)
            response.raise_for_status()
            return response.json().get("response", [])
        except requests.exceptions.RequestException as e:
            log.warning(f"Error fetching data from {endpoint} (attempt {attempt + 1}/{retries}): {str(e)}")
            if attempt == retries - 1:
                log.severe(f"Failed to fetch data from {endpoint} after {retries} retries")
                return None
            time.sleep(2 ** attempt)  # Exponential backoff
