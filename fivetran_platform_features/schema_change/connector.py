# This connector demonstrates schema changes by returning data with different types
# First sync: int_to_string field is an integer (42)
# Second sync: int_to_string field is a float (42.42)
# Third sync: int_to_string field is a non-numeric string ("forty-two point forty-two")
# Fourth+ syncs: int_to_string field is a float again (42.43)

from fivetran_connector_sdk import Connector, Operations as op, Logging as log

def update(configuration: dict, state: dict):
    sync_count = state.get('sync_count', 0)
    values = [42, 42.42, "forty-two point forty-two", 42.43]
    
    log.warning("Schema change demo: field type evolves across syncs")
    log.info(f"Sync #{sync_count + 1}: inserting value '{values[min(sync_count, 3)]}' ({type(values[min(sync_count, 3)]).__name__})")
    
    descriptions = [
        "First sync with integer",
        "Second sync with float", 
        "Third sync with non-numeric string",
        "Fourth sync with float again"
    ]
    
    yield op.upsert(table="int_to_string", data={
        "id": 1,
        "int_to_string": values[min(sync_count, 3)],
        "description": descriptions[min(sync_count, 3)]
    })
    yield op.checkpoint({"sync_count": sync_count + 1})

connector = Connector(update=update)

if __name__ == "__main__":
    connector.debug()
