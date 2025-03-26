# Fivetran Customer SDK Development Guidelines

## Commands
```bash
# Install SDK
pip install fivetran-connector-sdk

# Debug connector locally (creates warehouse.db)
fivetran debug [--configuration config.json]

# Reset local state for fresh debug run
fivetran reset

# Deploy connector to Fivetran
fivetran deploy --api-key <API-KEY> --destination <DEST> --connection <CONN> [--configuration config.json] [--force] [--python-version X.Y]

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

## Operations
- **Upsert**: `op.upsert(table="name", data=data_dict)` - Creates or updates rows
- **Update**: `op.update(table="name", data=data_dict)` - Only updates existing rows
- **Delete**: `op.delete(table="name", keys=keys_dict)` - Marks rows as deleted
- **Checkpoint**: `op.checkpoint(state=new_state)` - Saves state and processes data

## Best Practices
- **Primary Keys**: Always define in schema to prevent data duplication
- **Logging**: Use `log.info()`, `log.warning()`, `log.severe()` (and `log.fine()` for debugging)
- **Checkpoints**: Use regularly with large datasets (incremental syncs)
- **Data Types**: Specify in schema (BOOLEAN, INT, STRING, JSON, etc.) when needed
- **Error Handling**: Use specific exceptions with descriptive messages
- **History Mode**: Create composite keys with timestamp column to mimic history mode

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