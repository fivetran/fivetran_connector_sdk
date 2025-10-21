import requests  # For making HTTP requests
from urllib.parse import urlencode  # For encoding query parameters


class HarnessAPI:
    """A client for interacting with the Harness.io API."""

    def __init__(self, api_key, account_id, base_url="https://app.harness.io"):
        """
        Initialize the Harness API client.
        Args:
            api_key: The Harness API key
            account_id: The Harness account identifier
            base_url: The base URL for the Harness API
        """
        self.api_key = api_key
        self.account_id = account_id
        self.base_url = base_url
        self.headers = {
            "x-api-key": api_key,
        }

    def build_url(self, endpoint, query_params=None):
        """
        Build a URL with the given endpoint and query parameters.
        Args:
            endpoint: The API endpoint
            query_params: A dictionary of query parameters to include in the URL
        """
        if not endpoint.startswith("/"):
            endpoint = f"/{endpoint}"

        url = f"{self.base_url}{endpoint}"

        # Add query parameters
        if query_params:
            url = f"{url}?{urlencode(query_params)}"

        return url

    def get(self, endpoint, query_params=None):
        """
        Make a GET request to the Harness API.
        Args:
            endpoint: The API endpoint
            query_params: A dictionary of query parameters to include in the request
        """
        url = self.build_url(endpoint, query_params)
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        return response.json()

    def post(self, endpoint, data, query_params=None):
        """
        Make a POST request to the Harness API.
        Args:
            endpoint: The API endpoint
            data: The JSON payload to include in the request body
            query_params: A dictionary of query parameters to include in the request
        """
        url = self.build_url(endpoint, query_params)
        response = requests.post(url, headers=self.headers, json=data)
        response.raise_for_status()
        return response.json()
