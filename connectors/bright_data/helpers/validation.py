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
    has_unlocker = "unlocker_url" in configuration and configuration.get("unlocker_url")

    if not has_unlocker:
        raise ValueError("unlocker_url must be provided in configuration")

    # Validate API token
    if "api_token" not in configuration or not configuration.get("api_token"):
        raise ValueError("Missing required configuration value: api_token")
