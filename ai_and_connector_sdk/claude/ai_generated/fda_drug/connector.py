"""FDA Drug NDC Connector
This connector fetches data from the FDA Drug NDC API and creates three tables:
1. pharm_class_counts: Counts of drugs by pharmacological class
2. top_drugs_by_class: Top 2 drugs from each pharmacological class
3. api_history: Tracks API calls and rate limits

Note: The FDA API has the following disclaimer that applies to all data:
"Do not rely on openFDA to make decisions regarding medical care. While we make every effort to ensure that data is accurate, 
you should assume all results are unvalidated. We may limit or otherwise restrict your access to the API in line with our Terms of Service."
"""

from fivetran_connector_sdk import Connector, Logging as log, Operations as op
import json
import requests
from datetime import datetime, timedelta
from dateutil import parser
import time
from typing import Dict, List, Any, Generator

# Constants
RATE_LIMIT_WINDOW = 60  # seconds
API_CALLS_PER_MINUTE = 240
API_CALLS_PER_DAY = 1000

def validate_configuration(configuration: dict) -> None:
    """Validate the configuration dictionary to ensure it contains all required parameters."""
    required_configs = ["base_url", "rate_limit_per_minute", "rate_limit_per_day"]
    for key in required_configs:
        if key not in configuration:
            raise ValueError(f"Missing required configuration value: {key}")

def schema(configuration: dict) -> List[Dict[str, Any]]:
    """Define the schema for the connector's tables."""
    return [
        {
            "table": "pharm_class_counts",
            "primary_key": ["pharm_class", "count_date"]
        },
        {
            "table": "top_drugs_by_class",
            "primary_key": ["pharm_class", "product_ndc", "spl_id"]
        },
        {
            "table": "api_history",
            "primary_key": ["request_timestamp", "endpoint", "params"]
        }
    ]

def flatten_dict(d: Dict[str, Any], parent_key: str = '', sep: str = '_') -> Dict[str, Any]:
    """Flatten a nested dictionary into a single level dictionary."""
    items: List[tuple] = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            # Convert lists to strings for simplicity
            items.append((new_key, json.dumps(v)))
        else:
            items.append((new_key, v))
    return dict(items)

def make_api_request(url: str, params: Dict[str, Any], configuration: dict) -> Dict[str, Any]:
    """Make an API request to the FDA API with rate limiting."""
    api_key = configuration.get("api_key")
    if api_key:
        params["api_key"] = api_key

    response = requests.get(url, params=params)
    response.raise_for_status()
    
    # Log API call with unique params
    api_call_data = {
        "request_timestamp": datetime.utcnow().isoformat(),
        "endpoint": url,
        "status_code": str(response.status_code),
        "params": json.dumps(params, sort_keys=True)  # Sort keys to ensure consistent string representation
    }
    
    return response.json(), api_call_data

def get_pharm_class_counts(configuration: dict) -> Generator[Dict[str, Any], None, None]:
    """Get counts of drugs by pharmacological class."""
    url = configuration["base_url"]
    params = {"count": "pharm_class.exact"}
    
    try:
        data, api_call = make_api_request(url, params, configuration)
        yield op.upsert("api_history", api_call)
        
        for result in data.get("results", []):
            count_data = {
                "pharm_class": result["term"],
                "count": str(result["count"]),
                "count_date": datetime.utcnow().date().isoformat()
            }
            yield op.upsert("pharm_class_counts", count_data)
            
    except Exception as e:
        log.severe(f"Error fetching pharm class counts: {str(e)}")
        raise

def get_top_drugs_by_class(configuration: dict) -> Generator[Dict[str, Any], None, None]:
    """Get top 2 drugs from each pharmacological class."""
    url = configuration["base_url"]
    
    # First get all pharm classes
    params = {"count": "pharm_class.exact"}
    try:
        data, api_call = make_api_request(url, params, configuration)
        yield op.upsert("api_history", api_call)
        
        for result in data.get("results", []):
            pharm_class = result["term"]
            # Get top 2 drugs for this class
            search_params = {
                "search": f"pharm_class:\"{pharm_class}\"",
                "limit": 2
            }
            
            try:
                drugs_data, api_call = make_api_request(url, search_params, configuration)
                yield op.upsert("api_history", api_call)
                
                for drug in drugs_data.get("results", []):
                    # Flatten the drug data
                    flat_drug = flatten_dict(drug)
                    flat_drug["pharm_class"] = pharm_class
                    yield op.upsert("top_drugs_by_class", flat_drug)
                    
                # Respect rate limits
                time.sleep(1)  # Simple rate limiting
                
            except Exception as e:
                log.warning(f"Error fetching drugs for class {pharm_class}: {str(e)}")
                continue
                
    except Exception as e:
        log.severe(f"Error fetching pharm classes: {str(e)}")
        raise

def update(configuration: dict, state: dict) -> Generator[Dict[str, Any], None, None]:
    """Main update function that orchestrates the data sync process."""
    log.info("Starting FDA Drug NDC sync")
    
    # Validate configuration
    validate_configuration(configuration)
    
    try:
        # Get pharm class counts
        yield from get_pharm_class_counts(configuration)
        
        # Get top drugs by class
        yield from get_top_drugs_by_class(configuration)
        
        # Update state with sync timestamp
        new_state = {
            "last_sync_time": datetime.utcnow().isoformat()
        }
        yield op.checkpoint(new_state)
        
        log.info("FDA Drug NDC sync completed successfully")
        
    except Exception as e:
        log.severe(f"Sync failed: {str(e)}")
        raise

# Initialize the connector
connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    with open("configuration.json", 'r') as f:
        configuration = json.load(f)
    connector.debug(configuration=configuration)
