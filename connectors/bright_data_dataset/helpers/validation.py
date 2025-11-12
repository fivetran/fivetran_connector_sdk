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

    # Validate filter_name
    if "filter_name" not in configuration or not configuration.get("filter_name"):
        raise ValueError("Missing required configuration value: filter_name")

    # Validate filter_operator
    if "filter_operator" not in configuration or not configuration.get("filter_operator"):
        raise ValueError("Missing required configuration value: filter_operator")

    # Validate filter_value (can be None for is_null/is_not_null operators)
    filter_operator = configuration.get("filter_operator", "").lower()
    if filter_operator not in ("is_null", "is_not_null"):
        filter_value = configuration.get("filter_value")
        if filter_value is None:
            raise ValueError("Missing required configuration value: filter_value")
        if isinstance(filter_value, str) and not filter_value.strip():
            raise ValueError("filter_value cannot be empty")

    # Validate records_limit if provided (must be positive integer)
    records_limit = configuration.get("records_limit")
    if records_limit is not None:
        try:
            limit = int(records_limit)
            if limit <= 0:
                raise ValueError("records_limit must be a positive integer")
        except (ValueError, TypeError) as e:
            raise ValueError(
                f"records_limit must be a positive integer: {str(e)}"
            ) from e
