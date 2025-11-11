"""Configuration validation utilities."""


def validate_configuration(configuration: dict) -> None:
    """
    Validate the configuration dictionary to ensure it contains all required parameters.

    This function is called at the start of the update method to ensure that the connector
    has all necessary configuration values.

    Args:
        configuration: A dictionary that holds the configuration settings for the connector.

    Raises:
        ValueError: If any required configuration parameter is missing.
    """
    # Check that at least one endpoint is configured
    has_search = "search_query" in configuration and configuration.get("search_query")
    has_scrape = "scrape_url" in configuration and configuration.get("scrape_url")
    has_unlocker = "unlocker_url" in configuration and configuration.get("unlocker_url")

    if not any((has_search, has_scrape, has_unlocker)):
        raise ValueError(
            "At least one of 'search_query', 'scrape_url', or 'unlocker_url' must be provided in configuration"
        )

    # Validate API token
    if "api_token" not in configuration or not configuration.get("api_token"):
        raise ValueError("Missing required configuration value: api_token")
