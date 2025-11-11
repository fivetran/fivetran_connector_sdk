"""Configuration validation utilities for the Bright Data scrape connector."""


def validate_configuration(configuration: dict) -> None:
    """Ensure the configuration contains the required values."""
    if not configuration.get("api_token"):
        raise ValueError("Missing required configuration value: api_token")

    if not configuration.get("scrape_url"):
        raise ValueError("scrape_url cannot be empty")
