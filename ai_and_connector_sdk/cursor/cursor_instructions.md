# Cursor AI Development Best Practices

A structured guide for Connector SDK development using Cursor AI.

## Core Capabilities

1. **Code Generation & Modification**
   - Create new code from scratch
   - Modify existing codebases
   - Follow established patterns
   - Support both simple and complex scenarios

2. **Technical Expertise**
   - Language-specific best practices
   - Common design patterns
   - Error handling strategies
   - Testing approaches

## Prompt Engineering Template

```markdown
Task: [Specific task description]

Technical Details:
- Language: [Programming language and version]
- Connector SDK: [Framework name and version]
- API Integration: [API details if applicable]
- Data Requirements: [Data structure and types]

Requirements:
1. Core Functionality:
   - Data fetching approach
   - Processing logic
   - Output format
   - State management

2. Technical Requirements:
   - Error handling strategy
   - Logging requirements
   - Schema definition
   - Data validation

3. Implementation Details:
   - Required functions
   - Class structure
   - Dependencies
   - Configuration needs

4. Expected Output:
   - File structure
   - Function signatures
   - Data models
   - Configuration format

Additional Context:
- Rate limiting considerations
- Authentication needs
- Performance requirements
- Error scenarios to handle

Dependencies:
- Required packages
- Version constraints
- External services
```

## Example Implementation: Pokemon API Connector

```markdown
Task: Create a Fivetran connector for the Pokemon API that fetches and stores Pokemon data

Technical Details:
- Language: Python 3.12.8
- Framework: Fivetran Connector SDK
- API: PokeAPI (https://pokeapi.co/api/v2)
- Data Target: Pokemon information table

Requirements:
1. Core Functionality:
   - Fetch Pokemon data from PokeAPI
   - Transform response into tabular format
   - Store in 'pokemon' table
   - Support incremental updates

2. Technical Requirements:
   - Handle HTTP errors gracefully
   - Log sync progress
   - Define schema with proper types
   - Validate response data

3. Implementation Details:
   Required Functions:
   - schema(): Define table structure
   - update(): Fetch and process data
   
   Schema Definition:
   - Table: "pokemon"
   - Primary Key: ["name"]
   - Columns:
     - name: STRING
     - base_experience: INT
     - height: INT
     - weight: INT
     - order: INT
     - is_default: BOOLEAN

4. Expected Output:
   - connector.py with update and schema functions
   - Proper SDK imports
   - Error handling for API calls
   - Logging implementation

Additional Context:
- PokeAPI is rate-limited
- No authentication required
- Response includes nested JSON
- Handle HTTP 404 and 429 errors

Dependencies:
- fivetran_connector_sdk
- requests
```

## Development Workflow

### 1. Planning Phase
- Define data requirements
- Map API response to schema. Define the Primary Key and let Fivetran infer the rest.
- Plan error handling at strategic points in the code.
- Design state management

### 2. Implementation Phase
- Write clear, specific prompts
- Review generated code
- Test API integration
- Validate data types

### 3. Quality Assurance
- Test error scenarios
- Verify data accuracy
- Review logging
- Check performance

## Best Practices Checklist

### Before Coding
- [ ] API documentation reviewed
- [ ] Schema designed
- [ ] Error cases identified
- [ ] Dependencies listed

### Model options in Cursor as of 5/2/2025
- [ ] claude-3.5-sonnet
- [ ] claude-3.7-sonnet
- [ ] claude-3.7-sonnet MAX
- [ ] gemini-2.5-pro-exp-03-25
- [ ] gemini-2.5-pro
- [ ] gpt-4.1
- [ ] gpt-4o
- [ ] o4-mini
- [ ] o3
- [ ] cursor-small

### During Development
- [ ] Following SDK patterns
- [ ] Handling errors properly
- [ ] Adding detailed logging
- [ ] Testing edge cases

### After Implementation
- [ ] All tests passing
- [ ] Logging verified
- [ ] Error handling tested
- [ ] Code documented

## Common Pitfalls

1. **API Integration**
   - Missing error handling
   - Incorrect data types
   - Poor rate limit handling
   - Incomplete schema definition

2. **Implementation Issues**
   - Insufficient logging
   - Missing state management
   - Improper SDK usage
   - Incomplete testing

## Success Patterns

1. **Code Structure**
   - Clear function separation
   - Proper SDK utilization
   - Comprehensive error handling
   - Complete logging

2. **Quality Focus**
   - Full test coverage
   - Proper error handling
   - Performance optimization
   - Clear documentation

## Resources

- [Fivetran SDK Documentation](https://fivetran.com/docs/connectors/connector-sdk)
- [Python Best Practices](https://python-guide.org)
- [API Integration Guidelines](https://api-guidelines-link)

---
**Note**: Adapt these templates based on your specific API integration needs and data requirements. The Pokemon API example demonstrates a simple implementation that can be extended for more complex use cases. 

---
## Code produced from the prompt
```python
from fivetran_connector_sdk import Connector, Logging as log, Operations as op
import requests as rq

def schema(configuration: dict):
    return [
        {
            "table": "pokemon",
            "primary_key": ["name"]
        }
    ]

def update(configuration: dict, state: dict):
    log.info("Starting sync for Pokémon data")

    url = "https://pokeapi.co/api/v2/pokemon/ditto"
    response = rq.get(url)
    response.raise_for_status()

    pokemon_data = response.json()
    data = {
        "name": pokemon_data["name"],
        "base_experience": pokemon_data["base_experience"],
        "height": pokemon_data["height"],
        "weight": pokemon_data["weight"],
        "order": pokemon_data["order"],
        "is_default": pokemon_data["is_default"]
    }

    yield op.upsert(table="pokemon", data=data)
    yield op.checkpoint(state)

connector = Connector(update=update, schema=schema)
if __name__ == "__main__":
    connector.debug()
```
