# Pokemon API Connector Tutorial

## What This Tutorial Does

This tutorial walks you through building a **Fivetran Connector** that automatically syncs Pokemon data from the PokeAPI. You'll learn how to:

- Connect to PokeAPI's public Pokemon database (no API key required)
- Fetch specific Pokemon data (Ditto in this example) with field extraction
- Process and flatten complex JSON structures into tabular format
- Handle API responses with robust error handling
- Convert nested data structures to JSON strings for Fivetran compatibility
- Implement primary key generation for reliable data upserts

**Perfect for**: Data engineers, analysts, and developers who want to build reliable data pipelines for Pokemon data or learn the basics of API integration with Fivetran.

## Prerequisites

### Technical Requirements
- **Python 3.9 - 3.12+** installed on your machine
- **Basic Python knowledge** (functions, dictionaries, loops)
- **Fivetran Connector SDK** (we'll install this)
- **Git** (optional, for version control)

### API Access
- **PokeAPI** (completely free, no authentication required)
  - Public API endpoint: https://pokeapi.co/api/v2/pokemon/
  - No rate limits for basic usage
  - Comprehensive Pokemon data available

### Development Environment
- **Text editor** (Claude.ai or Claude code.)
- **Terminal/Command line** access
- **Internet connection** for API calls

## Required Files

You'll need these 3 core files to get started:

### 1. `connector.py` - Main Connector Logic
```python
# This is your main connector file
# Contains all the Pokemon API integration logic
# Handles data fetching, processing, and flattening
```

### 2. `config.json` - Settings
```json
{
    "ditto_endpoint": "https://pokeapi.co/api/v2/pokemon/ditto"
}
```

### 3. `requirements.txt` - Dependencies
```
fivetran-connector-sdk
requests
```

## The Prompt

Here's the original prompt that created this connector:

> "Create a Fivetran Connector SDK solution for the Pokemon API that includes:
> 
> - Fetching specific Pokemon data (Ditto) from PokeAPI
> - Extracting the first 10 fields from the API response
> - Converting complex nested structures (lists and dictionaries) to JSON strings
> - Implementing primary key generation for Fivetran compatibility
> - Robust error handling with proper logging
> - Simple configuration with just the API endpoint
> - Support for upserting data into a single table

> The connector should work with the PokeAPI endpoint to fetch Pokemon data and make it available for analytics."

## Quick Start

1. **Create your project folder**:
   ```bash
   mkdir pokemon-connector
   cd pokemon-connector
   ```

2. **Copy the three files** above into your folder

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Test the connector**:
   ```bash
   fivetran debug --configuration config.json
   ```

5. **Check your data**:
   ```bash
   duckdb warehouse.db ".tables"
   duckdb warehouse.db "SELECT * FROM pokemon_ditto;"
   ```

## What You'll Learn

- **API Integration**: How to work with REST APIs in data connectors
- **Data Extraction**: Selecting specific fields from complex API responses
- **Data Transformation**: Converting nested structures for tabular storage
- **Error Handling**: Building resilient connectors with proper logging
- **Primary Key Management**: Creating unique identifiers for data integrity
- **JSON Processing**: Handling complex data structures in Fivetran
- **Configuration Management**: Simple setup with minimal configuration

## Key Features

### 1. **Simple API Integration**
- Direct connection to PokeAPI with no authentication required
- Configurable endpoint for different Pokemon
- Timeout handling for reliable API calls

### 2. **Smart Data Extraction**
- Extracts only the first 10 fields from the API response
- Reduces data complexity while maintaining key information
- Configurable field selection for different use cases

### 3. **Data Type Conversion**
- Automatically converts lists to JSON strings
- Converts dictionaries to JSON strings
- Ensures Fivetran compatibility for complex data types

### 4. **Primary Key Generation**
- Uses Pokemon 'id' if available, falls back to 'name'
- Creates a 'key' field for reliable primary key identification
- Ensures data integrity and prevents duplicates

### 5. **Robust Error Handling**
- HTTP error handling with detailed logging
- Exception handling for unexpected errors
- Graceful failure with informative error messages

## Configuration Options

| Parameter | Description | Default | Required |
|-----------|-------------|---------|----------|
| `ditto_endpoint` | PokeAPI endpoint for specific Pokemon | PokeAPI URL | Yes |

## Data Schema

The connector creates a single table: `pokemon_ditto`

**Primary Keys**: `key`

**Sample Fields** (first 10 from API response):
- `id`: Pokemon ID number
- `name`: Pokemon name
- `base_experience`: Base experience points
- `height`: Pokemon height
- `weight`: Pokemon weight
- `abilities`: JSON string of abilities
- `forms`: JSON string of forms
- `game_indices`: JSON string of game indices
- `held_items`: JSON string of held items
- `is_default`: Whether this is the default form

## Code Walkthrough

### 1. **Update Function**
```python
def update(configuration: dict, state: dict):
    """
    Main function that fetches and processes Pokemon data
    """
    url = configuration["ditto_endpoint"]
    # Fetches data from PokeAPI
    # Extracts first 10 fields
    # Converts complex types to JSON strings
    # Generates primary key
    # Upserts data to Fivetran
```

### 2. **Data Processing Logic**
```python
# Extract first 10 fields
first_10_items = list(data.items())[:10]
processed_data = {k: v for k, v in first_10_items}

# Convert complex types to JSON strings
for key, value in processed_data.items():
    if isinstance(value, list):
        processed_data[key] = json.dumps(value)
    elif isinstance(value, dict):
        processed_data[key] = json.dumps(value)
```

### 3. **Primary Key Generation**
```python
# Use 'id' if available, else 'name', else 'ditto'
primary_key = processed_data.get("id", processed_data.get("name", "ditto"))
processed_data["key"] = primary_key
```

## Customization Options

### 1. **Change Pokemon**
Update the `ditto_endpoint` in `config.json`:
```json
{
    "ditto_endpoint": "https://pokeapi.co/api/v2/pokemon/pikachu"
}
```

### 2. **Extract More Fields**
Modify the field extraction in `connector.py`:
```python
# Change from 10 to any number
first_10_items = list(data.items())[:20]  # Extract 20 fields
```

### 3. **Add Custom Data Processing**
Add your own data transformations:
```python
# Add custom field processing
if "stats" in processed_data:
    processed_data["total_stats"] = sum(processed_data["stats"])
```

## Next Steps

Once you've got the basic connector working:

1. **Try different Pokemon** by changing the endpoint
2. **Extract more fields** for comprehensive data
3. **Add custom data transformations** for your specific use case
4. **Implement batch processing** for multiple Pokemon
5. **Add data validation** for quality assurance
6. **Deploy to production** with proper monitoring

## Troubleshooting

### Common Issues

1. **API Timeout**: Increase timeout value in requests.get()
2. **Field Extraction Errors**: Check API response structure
3. **JSON Conversion Issues**: Verify data types before conversion
4. **Primary Key Conflicts**: Ensure unique key generation

### Debug Mode

Run the connector in debug mode to see detailed logs:
```bash
fivetran debug --configuration config.json --verbose
```

### API Response Structure

The PokeAPI returns complex nested JSON. Our connector:
- Extracts only the top-level fields
- Converts nested structures to JSON strings
- Maintains data integrity while simplifying storage

## Advanced Customizations

### 1. **Multiple Pokemon Processing**
```python
pokemon_list = ["ditto", "pikachu", "charizard"]
for pokemon in pokemon_list:
    url = f"https://pokeapi.co/api/v2/pokemon/{pokemon}"
    # Process each Pokemon
```

### 2. **Custom Field Mapping**
```python
field_mapping = {
    "name": "pokemon_name",
    "height": "pokemon_height",
    "weight": "pokemon_weight"
}
# Apply custom field names
```

### 3. **Data Validation**
```python
def validate_pokemon_data(data):
    required_fields = ["id", "name"]
    for field in required_fields:
        if field not in data:
            raise ValueError(f"Missing required field: {field}")
```

## Need Help?

- **PokeAPI Documentation**: https://pokeapi.co/docs/v2
- **Fivetran SDK Docs**: https://fivetran.com/docs/connector-sdk
- **Pokemon Database**: https://pokeapi.co/api/v2/pokemon/
- **API Status**: https://pokeapi.co/

## Example Output

After running the connector, you'll see data like:
```sql
SELECT * FROM pokemon_ditto;
```

| key | id | name | base_experience | height | weight | abilities | forms | game_indices | held_items | is_default |
|-----|----|------|----------------|--------|--------|-----------|-------|--------------|------------|------------|
| 132 | 132 | ditto | 101 | 3 | 40 | [{"ability": {...}}] | [{"name": "ditto", "url": "..."}] | [{"game_index": 76, "version": {...}}] | [] | true |

Happy building! 🎮 
