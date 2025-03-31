# Fivetran Connector SDK Development Guidelines

## Documentation
- [Technical Reference](https://fivetran.com/docs/connector-sdk/technical-reference)
- [Working with Connector SDK](https://fivetran.com/docs/connector-sdk/working-with-connector-sdk)

## Commands
```bash
# Install SDK
pip install fivetran-connector-sdk

# Debug connector locally (creates warehouse.db)
fivetran debug [--configuration configuration.json]

# Reset local state for fresh debug run
fivetran reset

# Deploy connector to Fivetran
fivetran deploy --api-key <API-KEY> --destination <DEST> --connection <CONN> [--configuration configuration.json] [--force] [--python-version X.Y]

# Check SDK version
fivetran version

# Get help
fivetran --help
```

## Code Structure Requirements
- **Required Imports**: `from fivetran_connector_sdk import Connector, Operations as op, Logging as log`
- **Required Methods**: `update(configuration: dict, state: dict)` must yield operations
- **Optional Methods**: `schema(configuration: dict)` returns JSON structure with tables, columns, primary keys
- **Connector Object**: Must declare `connector = Connector(update=update, schema=schema)`
- **Entry Point**: Include `if __name__ == "__main__": connector.debug()` for local testing

## Operations
- **Upsert**: `op.upsert(table="name", data=data_dict)` - Creates or updates rows
- **Update**: `op.update(table="name", data=data_dict)` - Only updates existing rows
- **Delete**: `op.delete(table="name", keys=keys_dict)` - Marks rows as deleted
- **Checkpoint**: `op.checkpoint(state=new_state)` - Saves state and processes data

## Best Practices
- **Primary Keys**: Define in schema to prevent data duplication
- **Logging**: Use `log.info()`, `log.warning()`, `log.severe()` (and `log.fine()` for debugging)
- **Checkpoints**: Use regularly with large datasets (incremental syncs)
- **Data Types**: Supported types include BOOLEAN, INT, STRING, JSON, DECIMAL, FLOAT, UTC_DATETIME, etc.
- **Error Handling**: Use specific exceptions with descriptive messages
- **Configuration**: Store credentials and settings in configuration.json (securely encrypted)
  - **IMPORTANT**: configuration.json can only contain string values (convert numbers/booleans to strings)
- **Type Hints**: Use Python type hints (Dict, List, Any) for clarity
- **Docstrings**: Include detailed docstrings for all functions
- **Examples**: Many connector implementation examples are available in the examples/ directory

## Runtime Environment
- 1 GB RAM, 0.5 vCPUs
- Python versions 3.9.21 through 3.12.8
- Pre-installed packages: requests, fivetran_connector_sdk
- Use requirements.txt for additional dependencies

## Schema Definition Example
```python
def schema(configuration):
    return {
        "tables": {
            "my_table": {
                "primary_key": ["id"],
                "columns": {
                    "id": "STRING",
                    "name": "STRING",
                    "created_at": "UTC_DATETIME"
                }
            }
        }
    }
```