# AI Agents for Fivetran Connector SDK

This directory provides specialized AI agent setups for different development tools to help you build custom Fivetran connectors efficiently. Each subfolder is optimized for a specific AI-powered development environment.

## Available AI Agents

### 🤖 [Claude Code](./claude_code/)
**Best for: Advanced AI-powered development with specialized sub-agents**
- Automatic pickup of CLAUDE.md instructions
- Specialized Fivetran SDK sub-agents
- Advanced code generation and debugging
- Enterprise-grade connector development

**Get Started:**
```bash
cd AI_agents/claude_code
claude-code
```

### 🖱️ [Cursor](./cursor/)
**Best for: AI-enhanced IDE with real-time code completion**
- AI-powered code editor with smart completion
- Real-time collaboration features
- Integrated terminal and debugging
- Comprehensive development workflow

**Get Started:** Download from [cursor.sh](https://cursor.sh) and open the `cursor/` folder

### 📝 [VS Code with GitHub Copilot](./vscode_with_github_copilot/)
**Best for: GitHub Copilot integration and traditional IDE experience**
- GitHub Copilot integration
- Rich extension ecosystem
- Debugging and testing capabilities
- Traditional IDE with AI assistance

**Get Started:** Install VS Code and recommended extensions, then open the `vscode_with_github_copilot/` folder

### ☁️ [Windsurf](./windsurf/)
**Best for: Cloud-based collaborative development**
- Browser-based development environment
- Real-time team collaboration
- Built-in AI assistance
- No local setup required

**Get Started:** Go to [windsurf.com](https://windsurf.com) and import this project

## How Each Agent Works

Each subfolder contains:

```
agent_name/
├── AGENTS.md          # AI instructions (or CLAUDE.md for Claude Code)
├── README.md          # Human setup instructions
└── .claude/           # Claude Code specific sub-agents (claude_code only)
    └── agents/

# Examples are located at: ../examples/
```

### Recommendations:
- **New to Fivetran SDK**: Start with **Claude Code** for maximum AI assistance
- **Experienced developers**: Use for Cursor or Windsurf familiar IDE environment with Agents

## Key Features Across All Agents

### Connector SDK Patterns
- **No Yield Required**: All agents use the latest SDK patterns without yield statements
- **Direct Operations**: Simple `op.upsert()`, `op.checkpoint()` calls
- **Enterprise Quality**: Production-ready code generation

### 📚 Complete Solutions
Each agent generates:
- `connector.py` - Complete connector implementation
- `requirements.txt` - Dependency specifications
- `configuration.json` - Configuration template
- Documentation and setup instructions

### 🎯 Fivetran Best Practices
- Proper error handling and logging
- Authentication and security patterns
- State management and checkpointing
- Rate limiting and pagination handling

## Quick Start Guide

1. **Choose your preferred AI agent** from the options above
2. **Navigate to the agent folder** (e.g., `cd AI_agents/claude_code`)
3. **Follow the README.md** in that folder for setup instructions
4. **Start building!** Reference ../examples/ for connector patterns

## Common Workflow

Regardless of which agent you choose, the workflow is similar:

1. **Setup**: Follow agent-specific setup instructions
2. **Context**: Start the agents in the right folder so that they automatically load the AGENTS.md (or .claude/agents/*) content for AI assistance
3. **Examples**: Browse the ../examples/ folder for connector patterns
4. **Build**: Create your connector with AI assistance
5. **Test**: Use `fivetran debug --configuration config.json`
6. **Deploy**: Follow Fivetran's deployment guidelines

## Support and Resources

### Documentation
- [Fivetran Connector SDK Documentation](https://fivetran.com/docs/connector-sdk)
- [Technical Reference](https://fivetran.com/docs/connector-sdk/technical-reference)
- [Best Practices Guide](https://fivetran.com/docs/connector-sdk/best-practices)

### Examples and Resources
Each agent folder contains:
- **Agent-specific guides**: Tailored instructions for each development environment
- **Examples**: Located at ../examples/ with comprehensive connector patterns
- **Documentation**: Links to official Fivetran SDK resources

### Getting Help
1. Check the agent-specific README for setup
2. Browse ../examples/ for similar connector patterns
3. Use the AI assistant with provided context for debugging
4. Refer to official Fivetran documentation for SDK details

---

**Note**: All AI agents support the same core Fivetran SDK functionality. Choose based on your preferred development environment and collaboration needs. Each agent folder is designed to be a complete starting point for your connector development journey.
