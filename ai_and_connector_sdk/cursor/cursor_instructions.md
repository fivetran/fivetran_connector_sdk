# Cursor AI: Connector SDK development guide

## Introduction

This is a developer chat thread that was converted into a README using Cursor AI and the gpt-4o model. Ultimately, the model produced a working Connector SDK with a single prompt.

## Understanding Cursor AI's core capabilities

**Developer**: I need to build a Connector SDK for a new datasource in Fivetran. I've heard Cursor AI can help with this process. Where should I start?

**Cursor**: Great question! Connector SDK development works best when you understand both the capabilities of AI models and structured development practices. Let's walk through the entire process together, from planning to implementation to testing. I can assist with several aspects of connector development:

1. **Code generation & modification**
   - Creating new Connector SDK code from scratch
   - Modifying existing connector implementations
   - Implement patterns in your codebase
   - Support both simple and complex integration scenarios

2. **Technical implementation guidance**
   - Language-specific best practices for Python connectors
   - Common connector design patterns
   - Error handling strategies for API integrations
   - Testing approaches for data connectors

**Developer**: That sounds helpful. What models are currently available in Cursor?

**Cursor**: As of May 2025, Cursor offers these AI models:

- claude-3.5-sonnet
- claude-3.7-sonnet
- claude-3.7-sonnet MAX
- gemini-2.5-pro-exp-03-25
- gemini-2.5-pro
- gpt-4.1
- gpt-4o
- o4-mini
- o3
- cursor-small

For connector development, I recommend using either Claude 3.7 Sonnet or GPT-4o as they handle complex API integration requirements well.

## Development workflow

**Developer**: How should I structure my development process when working with Cursor AI?

**Cursor**: Let's break it down into three key phases:

### 1. Planning phase

**Developer**: What should I prepare before writing any code?

**Cursor**: Before coding, complete these planning steps:

- **API research**: Review the API documentation thoroughly
- **Schema design**: Define your data requirements and table structure
- **Error handling strategy**: Identify potential failure points
- **State management**: Plan how to track sync progress
- **Dependencies**: List required packages and version constraints

The more comprehensive your planning, the more effective your prompts will be.

**Developer**: That makes sense. Is there a checklist I can use during this phase?

**Cursor**: Absolutely. Use this planning checklist:

- [ ] API documentation reviewed and understood
- [ ] Project folder created with relevant files
- [ ] Schema designed with proper data types
- [ ] Primary keys identified for each table
- [ ] Error cases and handling approach identified
- [ ] Dependencies listed with version requirements
- [ ] Rate limiting considerations documented
- [ ] Authentication requirements understood

### 2. Prompt engineering

**Developer**: Once I've done the planning, how do I write effective prompts for Cursor AI?

**Cursor**: Structured prompts are crucial for quality code generation. With cursor AI you can provide additional context by adding @file_name or @project_folder to the prompt. Here's a template specifically designed for connector development:

```markdown
Task: [Specific connector task description]

Technical Details:
- Language: [Programming language and version]
- Connector SDK: [Framework name and version]
- API Integration: [API details with endpoints]
- Data Requirements: [Data structure and types]

Requirements:
1. Core functionality:
   - Data fetching approach
   - Processing logic
   - Output format
   - State management

2. Technical requirements:
   - Error handling strategy
   - Logging requirements
   - Schema definition
   - Data validation

3. Implementation details:
   - Required functions
   - Class structure
   - Dependencies
   - Configuration needs

4. Expected output:
   - File structure
   - Function signatures
   - Data models
   - Configuration format

Additional context:
- Rate limiting considerations
- Authentication needs
- Performance requirements
- Error scenarios to handle

Dependencies:
- Required packages
- Version constraints
- External services
```

**Developer**: Could you show me a real example of using this template?

**Cursor**: Here's an example for a Pokemon API connector using the Fivetran Connector SDK:

```markdown
Task: Create a Fivetran connector for the Pokemon API that fetches and stores Pokemon data. Sample response data is in @data.csv, and the documentation is in @api_docs.pdf. Build the solution in @project_folder.

Technical details:
- Language: Python 3.12.8
- Framework: Fivetran Connector SDK
- API: PokeAPI (https://pokeapi.co/api/v2)
- Data Target: Pokemon information table

Requirements:
1. Core functionality:
   - Fetch Pokemon data from PokeAPI
   - Transform response into tabular format
   - Store in 'pokemon' table
   - Support incremental updates

2. Technical requirements:
   - Handle HTTP errors gracefully
   - Log sync progress
   - Define schema with proper types
   - Validate response data

3. Implementation details:
   Required Functions:
   - schema(): Define table structure
   - update(): Fetch and process data
   
   Schema definition:
   - Table: "pokemon"
   - Primary Key: ["name"]

4. Expected output:
   - connector.py with update and schema functions
   - requirements.txt with the necesary python libraries. Do not include fivetran-connector-sdk or requests.
   - configuration.json with the key:value pair structure necessary to execute connector.py successfully.
   - state management by table
   - Error handling for API calls
   - Logging implementation

Additional context:
- PokeAPI is rate-limited
- No authentication required
- Response includes nested JSON
- Handle HTTP 404 and 429 errors

Dependencies:
- 
- 
```

### 3. Implementation phase

**Developer**: Now that I have a structured prompt, what's the most effective way to work with Cursor AI for implementation?

**Cursor**: During implementation, follow these interactive steps:

1. **Initial code generation**:
   - Paste your structured prompt into Cursor
   - Review the generated code for correctness
   - Identify any missing components or errors

2. **Iterative refinement**:
   - Ask for specific modifications to address gaps
   - Request explanations for complex sections
   - Clarify implementation details when needed

3. **Error handling expansion**:
   - Prompt for additional error cases
   - Request more robust exception handling
   - Ask for logging enhancements

4. **Testing assistance**:
   - Get test case suggestions
   - Request mock implementations
   - Ask for validation approaches

**Developer**: What does the code output typically look like?

**Cursor**: Here's the Python code for the Pokemon API connector example we discussed:

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

**Developer**: This is a good start, but it only fetches one Pokemon. How would I prompt Cursor to enhance this implementation?

**Cursor**: Great observation! Here's how you could prompt for an enhancement:

```
Enhance the Pokemon connector to fetch multiple Pokemon with pagination instead of just 'ditto'. 
The implementation should:
1. Start with the base endpoint: https://pokeapi.co/api/v2/pokemon
2. Handle the pagination using the 'next' URL in the response
3. Process each Pokemon in the results array
4. Implement proper state management to support incremental updates
5. Add robust error handling for network issues and rate limits
```

**Cursor**: Here is the code you requested. For each Pokémon, it fetches full details and yields a row to the pokemon table.

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
    log.info("Starting sync for all Pokémon data")

    # Use state to resume from last URL, or start from the beginning
    url = state.get("next_url", "https://pokeapi.co/api/v2/pokemon?limit=100&offset=0")
    while url:
        response = rq.get(url)
        response.raise_for_status()
        page = response.json()
        for pokemon in page["results"]:
            # Fetch full details for each Pokémon
            poke_resp = rq.get(pokemon["url"])
            poke_resp.raise_for_status()
            pokemon_data = poke_resp.json()
            data = {
                "name": pokemon_data["name"],
                "base_experience": pokemon_data["base_experience"],
                "height": pokemon_data["height"],
                "weight": pokemon_data["weight"],
                "order": pokemon_data["order"],
                "is_default": pokemon_data["is_default"]
            }
            yield op.upsert(table="pokemon", data=data)
        # Save the next page URL in the state for checkpointing
        url = page.get("next")
        yield op.checkpoint({"next_url": url} if url else {})

connector = Connector(update=update, schema=schema)
if __name__ == "__main__":
    connector.debug()
```

### 4. Quality assurance

**Developer**: Once I have a working implementation, what QA steps should I take?

**Cursor**: For thorough QA, follow these verification steps:

1. **Functionality testing**:
   - Test the connector with real API calls
   - Verify data is correctly upserted using dbeaver
   - Check incremental sync behavior

2. **Error handling verification**:
   - Simulate network failures
   - Test with invalid responses
   - Verify rate limit handling

3. **Code quality review**:
   - Check for proper logging
   - Ensure consistent error handling
   - Verify Connector SDK usage follows best practices

4. **Documentation**:
   - Ask Cursor AI to document complex sections
   - Request usage examples
   - Get configuration documentation

## Common pitfalls & success patterns

**Developer**: What are the most common mistakes developers make when building connectors with AI assistance?

**Cursor**: Watch out for these common pitfalls:

1. **API integration issues**:
   - Missing error handling for network failures
   - Incorrect data type mappings
   - Poor rate limit handling strategies
   - Incomplete pagination implementation

2. **Implementation shortcomings**:
   - Insufficient or excessive logging (especially for debugging)
   - Missing or incomplete state management
   - Improper Connector SDK usage patterns
   - Inadequate testing of edge cases

**Developer**: And what patterns lead to successful connector development?

**Cursor AI**: These success patterns lead to robust connectors:

1. **Structured code organization**:
   - Clear separation of concerns (fetching, transforming, storing)
   - Proper Connector SDK utilization following documentation
   - Comprehensive error handling at all levels
   - Complete logging for operational visibility

2. **Quality focused development**:
   - Full test coverage for happy and error paths
   - Robust error recovery mechanisms
   - Performance optimization for large datasets
   - Clear documentation for maintenance

## Iterative improvement

**Developer**: How can I iteratively improve my connector implementation?

**Cursor**: Use these iterative prompts to enhance your connector:

1. **Performance optimization**:
   ```
   Review the connector code in @connector.py for performance bottlenecks, particularly:
   1. Upsert processing opportunities
   2. Memory usage optimization
   3. Network call reduction strategies
   4. Error retry optimization
   ```

2. **Logging enhancement**:
   ```
   Enhance the logging implementation to include:
   1. Detailed progress information
   2. Performance metrics
   3. Error context for troubleshooting
   4. Data validation results
   ```

3. **Error handling robustness**:
   ```
   Expand error handling to cover:
   1. Transient network failures with backoff
   2. API-specific error codes
   3. Data validation errors
   4. Configuration validation
   ```

## Resources

- [Fivetran SDK Documentation](https://fivetran.com/docs/connectors/connector-sdk)
- [Python Best Practices](https://python-guide.org)
- [API Integration Guidelines](https://api-guidelines-link)

## Conclusion

**Remember these key principles:**

1. **Planning is crucial** - The more detailed your prompt, the better your results.
2. **Iterate frequently** - Start simple and enhance progressively.
3. **Test thoroughly** - Especially edge cases and error scenarios.
4. **Document clearly** - Both for users and future developers.

By following this guide, you'll be able to leverage Cursor AI effectively to develop robust, maintainable connectors for your data integration needs. The combination of structured prompting and iterative development leads to high-quality results that follow best practices.
