# Cursor AI Development Best Practices

A comprehensive guide for Fivetran Connector SDK development using Cursor AI, optimized for developer experience and ease of use.

## Getting Started with Cursor

### Installation & Setup
1. **Download Cursor**: Visit [cursor.sh](https://cursor.sh) and download for your OS
2. **Install Cursor**: Follow the installation wizard for macOS, Windows, or Linux
3. **Sign In**: Create an account or sign in with GitHub/Google
4. **Install Extensions**: 
   - Python extension (built-in)
   - Git integration (built-in)
   - Fivetran Connector SDK snippets (recommended)

### Initial Configuration
```bash
# Clone the Fivetran Connector SDK examples
git clone https://github.com/fivetran/fivetran_connector_sdk.git
cd fivetran_connector_sdk

# Set up Python environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install SDK and dependencies
pip install fivetran_connector_sdk
```

### Cursor AI Features for Connector Development
- **Chat Interface**: Press `Cmd/Ctrl + K` to access AI assistance
- **Code Generation**: Use `Cmd/Ctrl + L` for inline code generation
- **File Operations**: Right-click files for AI-powered refactoring
- **Terminal Integration**: Built-in terminal for testing connectors

## Core Capabilities

1. **Code Generation & Modification**
   - Create new connectors from scratch
   - Modify existing codebases following Fivetran patterns
   - Follow established SDK patterns and best practices
   - Support both simple and complex API integrations

2. **Technical Expertise**
   - Fivetran Connector SDK v1.0+ expertise
   - Python 3.9-3.12 best practices
   - API integration patterns
   - Error handling and logging strategies
   - Testing and validation approaches

## Enhanced Prompt Engineering Template

```markdown
Task: [Specific connector development task]

Technical Details:
- Language: Python 3.9+
- Framework: Fivetran Connector SDK v1.0+
- API Integration: [API details and documentation URL]
- Data Requirements: [Data structure and types]

Requirements:
1. Core Functionality:
   - Data fetching approach (pagination, rate limiting)
   - Processing logic and transformations
   - Output format and schema definition
   - State management and checkpointing

2. Technical Requirements:
   - Error handling strategy (HTTP errors, timeouts, rate limits)
   - Logging requirements (INFO, WARNING, SEVERE levels)
   - Schema definition (tables and primary keys only)
   - Data validation and type checking

3. Implementation Details:
   Required Functions:
   - schema(): Define table structure with primary keys only
   - update(): Fetch, process, and yield operations
   - Standard connector initialization pattern
   
   Required Operations:
   - yield op.upsert(table, data) for creating/updating records
   - yield op.update(table, modified) for updating existing records
   - yield op.delete(table, keys) for marking records as deleted
   - yield op.checkpoint(state) for incremental syncs

4. Expected Output:
   - connector.py with complete implementation
   - requirements.txt (exclude fivetran_connector_sdk and requests)
   - configuration.json template
   - README.md with setup and testing instructions

Additional Context:
- Rate limiting considerations and handling
- Authentication requirements and security
- Performance requirements and optimization
- Error scenarios and recovery strategies

Dependencies:
- Required packages with specific versions
- Version constraints and compatibility
- External services and API endpoints
```

## Example Implementation: Pokemon API Connector

```markdown
Task: Create a Fivetran connector for the Pokemon API that fetches and stores Pokemon data

Technical Details:
- Language: Python 3.9+
- Framework: Fivetran Connector SDK v1.0+
- API: PokeAPI (https://pokeapi.co/api/v2)
- Data Target: Pokemon information table

Requirements:
1. Core Functionality:
   - Fetch Pokemon data from PokeAPI with pagination
   - Transform response into tabular format
   - Store in 'pokemon' table with proper schema
   - Support incremental updates with checkpointing

2. Technical Requirements:
   - Handle HTTP errors (404, 429, 500) gracefully
   - Log sync progress and errors appropriately
   - Define schema with primary key only
   - Validate response data and handle malformed responses

3. Implementation Details:
   Required Functions:
   - schema(): Define table structure with primary key
   - update(): Fetch and process data with proper operations
   - Standard connector initialization
   
   Schema Definition:
   - Table: "pokemon"
   - Primary Key: ["name"]
   - Columns will be auto-detected from data

4. Expected Output:
   - connector.py with update and schema functions
   - Proper SDK imports and error handling
   - Logging implementation with appropriate levels
   - Requirements.txt with necessary libraries
   - Configuration.json template
   - README.md with testing instructions

Additional Context:
- PokeAPI is rate-limited (100 requests/minute)
- No authentication required
- Response includes nested JSON structures
- Handle HTTP 404 and 429 errors with retries

Dependencies:
- fivetran_connector_sdk (included in base environment)
- requests (included in base environment)
- Additional packages as needed
```

## Development Workflow with Cursor

### 1. Planning Phase
- **API Documentation Review**: Use Cursor's web search to review API docs
- **Schema Design**: Plan table structure and primary keys
- **Error Handling Strategy**: Identify potential failure points
- **State Management**: Design checkpoint and cursor logic

### 2. Implementation Phase
- **Code Generation**: Use Cursor AI to generate initial connector structure
- **Pattern Following**: Ensure code follows Fivetran SDK patterns
- **API Integration**: Test API calls and response handling
- **Data Validation**: Verify data types and transformations

### 3. Quality Assurance
- **Testing**: Use Cursor's terminal for connector testing
- **Error Scenarios**: Test various failure conditions
- **Logging Verification**: Check log output and levels
- **Performance Review**: Monitor sync performance and resource usage

## Best Practices Checklist

### Before Coding
- [ ] API documentation reviewed and bookmarked
- [ ] Schema designed with primary keys identified
- [ ] Error cases mapped and handling strategy planned
- [ ] Dependencies listed with version constraints
- [ ] Cursor workspace configured with proper extensions

### During Development
- [ ] Following Fivetran SDK patterns and examples
- [ ] Implementing proper error handling and logging
- [ ] Using appropriate operation types (upsert, update, delete, checkpoint)
- [ ] Testing API integration and data transformations
- [ ] Adding comprehensive logging with proper levels

### After Implementation
- [ ] All tests passing in Cursor terminal
- [ ] Logging verified with appropriate levels
- [ ] Error handling tested with various scenarios
- [ ] Code documented with clear comments and docstrings
- [ ] Configuration template created and validated

## Common Pitfalls and Solutions

### 1. API Integration Issues
- **Problem**: Missing error handling for rate limits
- **Solution**: Implement exponential backoff and retry logic
- **Cursor Tip**: Use AI to generate robust error handling patterns

- **Problem**: Incorrect data type handling
- **Solution**: Validate and transform data before yielding operations
- **Cursor Tip**: Use AI to suggest data validation patterns

### 2. Implementation Issues
- **Problem**: Insufficient logging
- **Solution**: Use appropriate log levels (INFO, WARNING, SEVERE)
- **Cursor Tip**: Ask AI to add comprehensive logging throughout code

- **Problem**: Missing state management
- **Solution**: Implement proper checkpointing after each batch
- **Cursor Tip**: Use AI to generate checkpoint state management patterns

## Success Patterns

### 1. Code Structure
```python
# Standard imports
from fivetran_connector_sdk import Connector, Logging as log, Operations as op
import json

# Schema definition (primary keys only)
def schema(configuration: dict):
    return [
        {"table": "table_name", "primary_key": ["key"]}
    ]

# Update function with proper operations
def update(configuration: dict, state: dict):
    # Fetch data with error handling
    # Process and validate data
    # Yield operations with proper patterns
    yield op.upsert("table_name", processed_data)
    yield op.checkpoint(state=new_state)

# Standard connector initialization
connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    with open("/configuration.json", 'r') as f:
        configuration = json.load(f)
    connector.debug(configuration=configuration)
```

### 2. Quality Focus
- **Complete Test Coverage**: Test all error scenarios and edge cases
- **Proper Error Handling**: Implement comprehensive error catching and recovery
- **Performance Optimization**: Use efficient data fetching and processing
- **Clear Documentation**: Document all functions, parameters, and usage

## Cursor-Specific Tips

### 1. AI Assistance
- **Chat Interface**: Use `Cmd/Ctrl + K` for complex questions about SDK patterns
- **Code Generation**: Use `Cmd/Ctrl + L` for generating boilerplate code
- **Refactoring**: Right-click code for AI-powered improvements
- **Debugging**: Ask AI to help debug connector issues

### 2. Terminal Integration
- **Testing**: Use built-in terminal for running connector tests
- **Debug Mode**: Run `fivetran debug --configuration config.json`
- **Log Analysis**: Monitor logs in real-time during testing
- **Environment Management**: Manage Python environments directly in Cursor

### 3. File Management
- **Project Structure**: Organize connector files logically
- **Version Control**: Use built-in Git integration for version management
- **Configuration**: Keep configuration files separate and secure
- **Documentation**: Maintain README files with setup instructions

## Resources and References

### Official Documentation
- [Fivetran Connector SDK Documentation](https://fivetran.com/docs/connector-sdk)
- [SDK Examples Repository](https://github.com/fivetran/fivetran_connector_sdk/tree/main/examples)
- [Technical Reference](https://fivetran.com/docs/connector-sdk/technical-reference)
- [Best Practices Guide](https://fivetran.com/docs/connector-sdk/best-practices)

### Cursor Resources
- [Cursor Documentation](https://cursor.sh/docs)
- [Cursor AI Features](https://cursor.sh/features)
- [Cursor Keyboard Shortcuts](https://cursor.sh/docs/keyboard-shortcuts)

### Development Tools
- [Python Documentation](https://docs.python.org/3/)
- [Requests Library](https://requests.readthedocs.io/)
- [JSON Schema Validation](https://json-schema.org/)

## Quick Start Commands

```bash
# Set up new connector project
mkdir my_connector && cd my_connector
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install fivetran_connector_sdk

# Test connector
fivetran debug --configuration configuration.json

# Check logs and output
ls -la warehouse.db
```

---
**Note**: This guide is optimized for Cursor AI development workflow. Adapt these patterns based on your specific API integration needs and data requirements. The FDA Food API tutorial demonstrates a simple implementation that can be extended for more complex use cases. Always refer to the official Fivetran documentation for the most up-to-date SDK information and best practices. 
