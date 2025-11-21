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
    # Validate API token
    if "api_token" not in configuration or not configuration.get("api_token"):
        raise ValueError("Missing required configuration value: api_token")

    # Validate dataset_id
    if "dataset_id" not in configuration or not configuration.get("dataset_id"):
        raise ValueError("Missing required configuration value: dataset_id")

    # Validate scrape_url
    scrape_url = configuration.get("scrape_url")
    if not scrape_url:
        raise ValueError("scrape_url cannot be empty")
