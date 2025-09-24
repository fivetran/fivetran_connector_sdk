# Cursor Agent for Fivetran Connector SDK

This folder is designed to help you use **Cursor** with the Fivetran Connector SDK to build custom connectors efficiently.

## Getting Started with Cursor

1. **Download and Install Cursor** - Visit [cursor.sh](https://cursor.sh) and download for your OS
2. **Start Cursor in this folder** - This ensures you have access to all necessary examples and instructions
3. **Use the AGENTS.md file** - Copy its contents into Cursor's chat for AI assistance with connector development
4. **Browse connector examples** - Check ../../examples/ for connector patterns and implementations

## Folder Structure

```
cursor/
├── AGENTS.md          # AI instructions for Cursor
└── README.md          # This file - instructions for humans

# Examples are located at: ../../examples/
```

## How to Use with Cursor

### 1. Initial Setup
```bash
# Install Cursor from cursor.sh
# Open this folder in Cursor
cd AI_agents/cursor
cursor .
```

### 2. Configure AI Context
- Copy the contents of `AGENTS.md` into Cursor's chat
- This provides Cursor with Fivetran SDK expertise
- Use `Cmd/Ctrl + K` to access Cursor's AI chat interface

### 3. Build Your Connector
Use Cursor's AI features:
- **Chat Interface** (`Cmd/Ctrl + K`): Ask questions about connector development
- **Code Generation** (`Cmd/Ctrl + L`): Generate connector code inline
- **Compose** (`Cmd/Ctrl + I`): Create complete files and implementations
- **Terminal Integration**: Test connectors directly in Cursor

### 4. Example Prompts
Try these with Cursor:
- "Create a connector for [API name] using the Fivetran SDK patterns"
- "Help me implement pagination for my connector"
- "Debug this connector code and fix any issues"
- "Add incremental sync capability to my connector"

## Key Features

- **No Yield Required**: Modern SDK patterns without yield statements
- **Complete Solutions**: Get connector.py, requirements.txt, and configuration.json
- **Best Practices**: Automatic implementation of Fivetran coding standards
- **Error Handling**: Comprehensive error catching and logging
- **Enterprise Ready**: Production-quality code generation

## Video Tutorials

[Video tutorials will be added here]

## Development Workflow

### 1. Planning Phase
- Review API documentation
- Design table structure and primary keys
- Plan error handling strategy
- Design state management approach

### 2. Implementation Phase
- Use Cursor AI to generate initial connector structure
- Follow Fivetran SDK patterns from ../../examples/
- Test API integration and response handling
- Validate data types and transformations

### 3. Testing Phase
- Use Cursor's terminal for connector testing
- Run `fivetran debug --configuration config.json`
- Verify logs and data output
- Test error scenarios

## Best Practices Checklist

### Before Coding
- [ ] API documentation reviewed
- [ ] Schema designed with primary keys
- [ ] Error handling strategy planned
- [ ] Cursor configured with proper context

### During Development
- [ ] Following Fivetran SDK patterns
- [ ] Implementing proper error handling
- [ ] Using direct operation calls (no yield)
- [ ] Adding comprehensive logging

### After Implementation
- [ ] Tests passing in Cursor terminal
- [ ] Logs verified at appropriate levels
- [ ] Error handling tested
- [ ] Configuration template validated

## Common Cursor Commands

```bash
# Test your connector
fivetran debug --configuration configuration.json

# Check output
ls -la warehouse.db

# View logs
# Logs appear in terminal during debug run
```

## Resources

- [Fivetran Connector SDK Documentation](https://fivetran.com/docs/connector-sdk)
- [Technical Reference](https://fivetran.com/docs/connector-sdk/technical-reference)
- [Best Practices Guide](https://fivetran.com/docs/connector-sdk/best-practices)
- [SDK Examples Repository](https://github.com/fivetran/fivetran_connector_sdk/tree/main/examples)
- [Cursor Documentation](https://cursor.sh/docs)

## Support

If you encounter issues:
1. Check ../../examples/ for similar connector patterns
2. Use Cursor's AI chat with the AGENTS.md context
3. Refer to official Fivetran documentation