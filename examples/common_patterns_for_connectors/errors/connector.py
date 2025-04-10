from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Operations as op
from fivetran_connector_sdk import Logging as log
import time
import json
import sys
import random

# Initialize logging level
log.LOG_LEVEL = log.Level.INFO

def handle_critical_error(error_message, error_details=None):
    """
    Handle critical errors by logging them as severe and stopping Connector SDK execution.
    """
    log.severe(f"CRITICAL ERROR: {error_message}")
    if error_details:
        log.severe(f"Error details: {error_details}")
    raise Exception(f"CRITICAL ERROR: {error_message}")

def schema(configuration: dict):
    """
    Define the schema for the connector.
    """
    return [
        {"table": "demo_response_test", "primary_key": ["key"]}
    ]

def update(configuration: dict, state: dict):
    """
    Main update function that simulates data processing with various error scenarios.
    
    Error simulation types:
    0: No error simulation (normal operation)
    1: Missing Configuration error
    2: Invalid State Format error
    3: Data Generation Error
    4: Processing Error
    5: Database Operation Error
    """
    log.info("Starting update process")
    
    # Get error simulation type from configuration
    error_simulation_type = str(configuration.get('error_simulation_type', "0"))
    log.info(f"Error simulation type: {error_simulation_type}")

    if error_simulation_type == "0":
        log.info("Running in normal mode - no error simulation")
    
    # =====================================================================
    # SECTION 1: CONFIGURATION VALIDATION
    # =====================================================================
    try:
        # ERROR SIMULATION 1: Missing Configuration
        if error_simulation_type == "1":
            log.info("Simulating missing configuration error")
            log.info("BEST PRACTICE: Always validate required configuration parameters early")
            raise KeyError("demo_config missing from configuration")
        
        demo_config = configuration.get('demo_config', "default")
        log.info(f"Demo configuration: {demo_config}")
    except KeyError as e:
        handle_critical_error("Missing required configuration", str(e))
    except Exception as e:
        handle_critical_error("Unexpected error during configuration validation", str(e))
    
    # =====================================================================
    # SECTION 2: STATE MANAGEMENT
    # =====================================================================
    start = time.time()
    
    # Get current cursor from state
    current_cursor = state.get('cursor', None)
    log.info(f'Current cursor: {current_cursor}')
    
    # ERROR SIMULATION 2: Invalid State Format
    if error_simulation_type == "2":
        log.info("Simulating invalid state format error")
        log.info("BEST PRACTICE: Validate state format before using it")
        handle_critical_error("Invalid state format detected")
    
    # =====================================================================
    # SECTION 3: DATA GENERATION
    # =====================================================================
    try:
        log.info("Generating simulated data")
        
        # ERROR SIMULATION 3: Data Generation Error
        if error_simulation_type == "3":
            log.info("Simulating data generation error")
            log.info("BEST PRACTICE: Handle errors during data generation")
            handle_critical_error("Failed to generate data", "Simulated data generation error")
        
        # Simulate data generation
        simulated_data = {
            "response": f"Simulated response for config: {demo_config}",
            "timestamp": time.time(),
            "random_value": random.randint(1, 100)
        }
        log.info("Successfully generated simulated data")
    except Exception as e:
        handle_critical_error("Error during data generation", str(e))
    
    # =====================================================================
    # SECTION 4: DATA PROCESSING
    # =====================================================================
    try:
        # Process the simulated data
        processed_data = {
            "key": start,
            "request": demo_config,
            "result": simulated_data["response"],
            "metadata": json.dumps({
                "timestamp": simulated_data["timestamp"],
                "random_value": simulated_data["random_value"]
            })
        }
        
        # ERROR SIMULATION 4: Processing Error
        if error_simulation_type == "4":
            log.info("Simulating data processing error")
            log.info("BEST PRACTICE: Validate data before processing")
            handle_critical_error("Data processing error", "Simulated error during processing")
        
        log.info(f'Processed data: {processed_data["result"][:30]}...')
        
        # Create new state
        new_cursor = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        new_state = {'cursor': new_cursor}
        log.info(f'Generated new cursor: {new_cursor}')
    except Exception as e:
        handle_critical_error("Error processing data", str(e))
    
    # =====================================================================
    # SECTION 5: DATA UPSERTION
    # =====================================================================
    try:
        # ERROR SIMULATION 5: Database Operation Error
        if error_simulation_type == "5":
            log.info("Simulating database operation error")
            log.info("BEST PRACTICE: Handle database operation errors gracefully")
            handle_critical_error("Database operation failed", "Simulated database error")
        
        # Upsert the data
        yield op.upsert("demo_response_test", processed_data)
        log.info('Data successfully upserted to target database')
    except Exception as e:
        handle_critical_error("Error during upsert operation", str(e))
    
    # =====================================================================
    # SECTION 6: CHECKPOINTING
    # =====================================================================
    try:
        # Only yield checkpoint after a successful run
        yield op.checkpoint(new_state)
        log.info(f'Successfully created checkpoint with cursor: {new_cursor}')
    except Exception as e:
        handle_critical_error("Error during checkpoint", str(e))

# Create connector instance
connector = Connector(update=update, schema=schema)

# Main execution for local debugging
if __name__ == "__main__":
    try:
        with open("/Users/elijah.davis/Documents/code/sdk/tests/sdk_v1/config.json", 'r') as f:
            configuration = json.load(f)
        log.info("Starting connector in debug mode")
        connector.debug(configuration=configuration)
    except Exception as e:
        log.severe(f"Error during connector execution: {str(e)}")
        sys.exit(1) 
