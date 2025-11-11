"""Configuration validation utilities for the SERP connector."""


def validate_configuration(configuration: dict) -> None:
    """Ensure required configuration values are present."""
    api_token = configuration.get("api_token")
    search_query = configuration.get("search_query")

    if not api_token:
        raise ValueError("Missing required configuration value: api_token")

    if not search_query:
        raise ValueError("search_query cannot be empty")
