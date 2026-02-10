"""
You can define complex or reusable constant values in a separate Python file and import them into your connector.
This is useful for keeping your connector code clean and organized.

Important: Constant values defined this way cannot be viewed or modified from the Fivetran dashboard.
To change them, you must update the file and redeploy the connector.
If you need configuration options that you can edit from the Fivetran dashboard, define them in configuration.json instead.

Do not store secrets here. Always use configuration.json for sensitive values such as API keys or credentials.
"""

API_CONFIGURATION = {
    "regions": ["us-east-1", "us-east-4", "us-central-1"],
    "api_quota": 12345,
    "use_bulk_api": True,
    "currencies": [{"From": "USD", "To": "EUR"}, {"From": "USD", "To": "GBP"}],
    "complex_constant": {
        "level_1": {
            "level_2": {
                "level_3": "This is a complex value that can be defined in conf.py and imported into the connector."
            },
            "list_of_dicts": [{"name": "item1", "value": 1}, {"name": "item2", "value": 2}],
        }
    },
}
