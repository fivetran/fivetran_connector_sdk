# Visual Studio Code Development Best Practices

A comprehensive guide for Fivetran Connector SDK development using Visual Studio Code, optimized for developer experience and productivity.

## Getting Started with Visual Studio Code

### Installation & Setup
1. **Download VS Code**: Visit [code.visualstudio.com](https://code.visualstudio.com) and download for your OS
2. **Install VS Code**: Follow the installation wizard for macOS, Windows, or Linux
3. **Install Extensions**: 
   - Python extension (ms-python.python)
   - Git integration (built-in)
   - Fivetran Connector SDK snippets (recommended)
   - AI coding assistants (GitHub Copilot, Cursor AI, etc.)
   - REST Client for API testing
   - JSON Tools for configuration management

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

### VS Code Features for Connector Development
- **Integrated Terminal**: Press `` Ctrl+` `` to access terminal
- **Command Palette**: Use `Cmd/Ctrl + Shift + P` for quick commands
- **IntelliSense**: Advanced code completion and suggestions
- **Debugging**: Built-in debugger with breakpoints and variable inspection
- **Git Integration**: Source control management within the editor
- **Extensions Marketplace**: Rich ecosystem of development tools

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

## Development Workflow with VS Code

### 1. Planning Phase
- **API Documentation Review**: Use VS Code's web search or REST Client for API testing
- **Schema Design**: Plan table structure and primary keys using JSON Tools
- **Error Handling Strategy**: Identify potential failure points
- **State Management**: Design checkpoint and cursor logic

### 2. Implementation Phase
- **Code Generation**: Use AI extensions or snippets for initial connector structure
- **Pattern Following**: Ensure code follows Fivetran SDK patterns
- **API Integration**: Test API calls using REST Client extension
- **Data Validation**: Verify data types and transformations

### 3. Quality Assurance
- **Testing**: Use integrated terminal for connector testing
- **Error Scenarios**: Test various failure conditions
- **Logging Verification**: Check log output and levels
- **Performance Review**: Monitor sync performance and resource usage

## Best Practices Checklist

### Before Coding
- [ ] API documentation reviewed and bookmarked
- [ ] Schema designed with primary keys identified
- [ ] Error cases mapped and handling strategy planned
- [ ] Dependencies listed with version constraints
- [ ] VS Code workspace configured with proper extensions

### During Development
- [ ] Following Fivetran SDK patterns and examples
- [ ] Implementing proper error handling and logging
- [ ] Using appropriate operation types (upsert, update, delete, checkpoint)
- [ ] Testing API integration and data transformations
- [ ] Adding comprehensive logging with proper levels

### After Implementation
- [ ] All tests passing in integrated terminal
- [ ] Logging verified with appropriate levels
- [ ] Error handling tested with various scenarios
- [ ] Code documented with clear comments and docstrings
- [ ] Configuration template created and validated

## Common Pitfalls and Solutions

### 1. API Integration Issues
- **Problem**: Missing error handling for rate limits
- **Solution**: Implement exponential backoff and retry logic
- **VS Code Tip**: Use REST Client extension to test API endpoints before implementation

- **Problem**: Incorrect data type handling
- **Solution**: Validate and transform data before yielding operations
- **VS Code Tip**: Use Python extension's type checking features

### 2. Implementation Issues
- **Problem**: Insufficient logging
- **Solution**: Use appropriate log levels (INFO, WARNING, SEVERE)
- **VS Code Tip**: Use snippets or AI extensions to add comprehensive logging

- **Problem**: Missing state management
- **Solution**: Implement proper checkpointing after each batch
- **VS Code Tip**: Use Git integration to track state management changes

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

## VS Code-Specific Tips

### 1. Essential Extensions
- **Python (ms-python.python)**: Core Python support with IntelliSense
- **GitLens**: Enhanced Git capabilities and history
- **REST Client**: Test API endpoints directly in VS Code
- **JSON Tools**: Format and validate JSON configuration files
- **Python Docstring Generator**: Auto-generate documentation
- **Python Test Explorer**: Run and debug tests efficiently

### 2. Terminal Integration
- **Testing**: Use integrated terminal for running connector tests
- **Debug Mode**: Run `fivetran debug --configuration config.json`
- **Log Analysis**: Monitor logs in real-time during testing
- **Environment Management**: Manage Python environments directly in VS Code

### 3. File Management
- **Project Structure**: Organize connector files logically
- **Version Control**: Use built-in Git integration for version management
- **Configuration**: Keep configuration files separate and secure
- **Documentation**: Maintain README files with setup instructions

### 4. Debugging Features
- **Breakpoints**: Set breakpoints by clicking in the gutter
- **Variable Inspection**: Hover over variables to see values
- **Call Stack**: Navigate through function calls during debugging
- **Watch Expressions**: Monitor specific variables during execution

## Advanced VS Code Features

### 1. IntelliSense and Code Completion
- **Context-aware Suggestions**: Get relevant code suggestions based on context
- **Auto-imports**: Automatically import required modules
- **Type Hints**: Enhanced type checking and suggestions
- **Refactoring Tools**: Rename variables, extract methods, and more

### 2. Integrated Development Environment
- **Multi-file Editing**: Work with multiple files simultaneously
- **Split Views**: Compare files side by side
- **Minimap**: Navigate large files quickly
- **Folding**: Collapse code sections for better readability

### 3. Source Control Integration
- **Git Status**: See file changes in the file explorer
- **Diff View**: Compare changes before committing
- **Branch Management**: Switch between branches easily
- **Commit History**: View and search through commit history

### 4. Customization and Productivity
- **Keyboard Shortcuts**: Customize shortcuts for your workflow
- **Snippets**: Create reusable code templates
- **Themes**: Customize the editor appearance
- **Settings Sync**: Sync settings across multiple machines

## Workspace Configuration

### 1. settings.json
```json
{
    "python.defaultInterpreterPath": "./venv/bin/python",
    "python.linting.enabled": true,
    "python.linting.pylintEnabled": true,
    "python.formatting.provider": "black",
    "editor.formatOnSave": true,
    "editor.rulers": [88],
    "files.exclude": {
        "**/__pycache__": true,
        "**/*.pyc": true
    },
    "python.testing.pytestEnabled": true,
    "python.testing.unittestEnabled": false
}
```

### 2. launch.json for Debugging
```json
{
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Debug Connector",
            "type": "python",
            "request": "launch",
            "program": "${workspaceFolder}/connector.py",
            "console": "integratedTerminal",
            "env": {
                "PYTHONPATH": "${workspaceFolder}"
            }
        }
    ]
}
```

### 3. tasks.json for Automation
```json
{
    "version": "2.0.0",
    "tasks": [
        {
            "label": "Test Connector",
            "type": "shell",
            "command": "fivetran",
            "args": ["debug", "--configuration", "configuration.json"],
            "group": "test",
            "presentation": {
                "echo": true,
                "reveal": "always",
                "focus": false,
                "panel": "shared"
            }
        }
    ]
}
```

## AI Integration Options

### 1. GitHub Copilot
- **Installation**: Available in VS Code extensions marketplace
- **Features**: Inline code suggestions and completions
- **Best Practices**: Review suggestions before accepting
- **Configuration**: Customize behavior in settings

### 2. Cursor AI
- **Installation**: Download from cursor.sh
- **Features**: Advanced AI code generation and editing
- **Integration**: Works alongside VS Code
- **Use Cases**: Complex code generation and refactoring

### 3. Other AI Extensions
- **Tabnine**: AI-powered code completion
- **Kite**: Python-specific AI assistance
- **IntelliCode**: Microsoft's AI-powered IntelliSense

## Performance Optimization

### 1. Extension Management
- **Disable Unused Extensions**: Reduce startup time and memory usage
- **Extension Settings**: Configure extensions for optimal performance
- **Workspace-specific Extensions**: Only load extensions needed for current project

### 2. File Management
- **Exclude Patterns**: Add unnecessary files to .gitignore and files.exclude
- **Large File Handling**: Use .gitattributes for large files
- **Search Optimization**: Configure search.exclude for better performance

### 3. Memory Management
- **Close Unused Tabs**: Reduce memory usage
- **Restart VS Code**: Clear memory when experiencing slowdowns
- **Monitor Resources**: Use Activity Monitor/Task Manager to check resource usage

## Collaboration Features

### 1. Live Share
- **Installation**: Install Live Share extension
- **Features**: Real-time collaboration and pair programming
- **Security**: Secure sharing with access controls
- **Use Cases**: Code reviews and collaborative debugging

### 2. Git Integration
- **Pull Requests**: Create and review PRs directly in VS Code
- **Code Reviews**: Use built-in diff viewer for reviews
- **Branch Management**: Switch and create branches easily
- **Conflict Resolution**: Built-in merge conflict resolution tools

### 3. Team Settings
- **Workspace Settings**: Share settings with team members
- **Extension Recommendations**: Suggest extensions for the project
- **Code Style**: Enforce consistent formatting across the team

## Troubleshooting Common Issues

### 1. Python Environment Issues
- **Interpreter Selection**: Use Command Palette to select correct Python interpreter
- **Virtual Environment**: Ensure venv is activated in integrated terminal
- **Path Issues**: Check PYTHONPATH and system PATH settings
- **Extension Conflicts**: Disable conflicting Python extensions

### 2. Performance Issues
- **Extension Overload**: Disable unnecessary extensions
- **Large Workspaces**: Use workspace folders to organize large projects
- **Memory Usage**: Monitor and restart VS Code when needed
- **File Watching**: Configure files.watcherExclude for large directories

### 3. Git Integration Issues
- **Authentication**: Configure Git credentials properly
- **SSH Keys**: Set up SSH keys for secure repository access
- **Large Repositories**: Use Git LFS for large files
- **Network Issues**: Check proxy settings and firewall configuration

## Resources and References

### Official Documentation
- [Fivetran Connector SDK Documentation](https://fivetran.com/docs/connector-sdk)
- [SDK Examples Repository](https://github.com/fivetran/fivetran_connector_sdk/tree/main/examples)
- [Technical Reference](https://fivetran.com/docs/connector-sdk/technical-reference)
- [Best Practices Guide](https://fivetran.com/docs/connector-sdk/best-practices)

### VS Code Resources
- [VS Code Documentation](https://code.visualstudio.com/docs)
- [VS Code Extensions Marketplace](https://marketplace.visualstudio.com/vscode)
- [VS Code Keyboard Shortcuts](https://code.visualstudio.com/docs/getstarted/keybindings)
- [VS Code Python Tutorial](https://code.visualstudio.com/docs/python/python-tutorial)

### Development Tools
- [Python Documentation](https://docs.python.org/3/)
- [Requests Library](https://requests.readthedocs.io/)
- [JSON Schema Validation](https://json-schema.org/)
- [Black Code Formatter](https://black.readthedocs.io/)

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

## VS Code vs Other IDEs

### Advantages of VS Code
- **Lightweight**: Fast startup and low resource usage
- **Extensible**: Rich ecosystem of extensions
- **Cross-platform**: Consistent experience across operating systems
- **Free and Open Source**: No licensing costs
- **Active Community**: Regular updates and community support

### Migration from Other IDEs
- **From PyCharm**: Import project and configure Python interpreter
- **From Sublime Text**: Similar workflow with enhanced features
- **From Atom**: Familiar interface with better performance
- **From Vim/Emacs**: Maintain productivity with modern features

## Extension Recommendations

### Essential Extensions
- **Python (ms-python.python)**: Core Python support
- **GitLens**: Enhanced Git capabilities
- **REST Client**: API testing and documentation
- **JSON Tools**: JSON formatting and validation
- **Python Docstring Generator**: Documentation generation

### Productivity Extensions
- **Auto Rename Tag**: HTML/XML tag renaming
- **Bracket Pair Colorizer**: Visual bracket matching
- **Indent Rainbow**: Visual indentation guides
- **Path Intellisense**: File path autocompletion
- **Thunder Client**: Lightweight REST client

### Theme and UI Extensions
- **Material Icon Theme**: File and folder icons
- **One Dark Pro**: Popular dark theme
- **Dracula Official**: High contrast theme
- **Fira Code**: Programming font with ligatures

---
**Note**: This guide is optimized for Visual Studio Code development workflow. Adapt these patterns based on your specific API integration needs and data requirements. The Pokemon API tutorial demonstrates a simple implementation that can be extended for more complex use cases. Always refer to the official Fivetran documentation for the most up-to-date SDK information and best practices. VS Code's extensible architecture and rich ecosystem make it particularly well-suited for Fivetran connector development, offering excellent performance, customization options, and integration capabilities compared to traditional IDEs. 
