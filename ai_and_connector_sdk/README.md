# Introduction
This section of our example repository is designed to help you use emerging AI tools with the Connector SDK to build custom connectors. The [Fivetran Connector SDK System Instructions](https://github.com/fivetran/fivetran_connector_sdk/ai_and_connector_sdk/agents.md) file can be used in any IDE or AI assistant process. Simply add it as context to the prompt and let the model or agents do the rest! We have sample outputs and video demonstrations using common IDEs linked below. Follow along or enhance an existing example with AI today!


## ChatGPT
ChatGPT can be effectively used to help you leverage our Connector SDK to build custom connectors. This was achieved by passing examples from this repository along with the full [technical reference](https://fivetran.com/docs/connector-sdk/technical-reference) documentation to ChatGPT. When tested, it does require some manual adjustments of the created code.


## IDE & AI Assistant Support

Learn about using various IDEs and AI assistants with the Fivetran Connector SDK. Each section includes a summary and links to relevant sub-guides, sample content, and a short video.

- [Cursor](https://github.com/fivetran/fivetran_connector_sdk/ai_and_connector_sdk/cursor)
- [Windsurf](https://github.com/fivetran/fivetran_connector_sdk/ai_and_connector_sdk/windsurf)
- [Visual Studio Code](https://github.com/fivetran/fivetran_connector_sdk/ai_and_connector_sdk/vscode)
- [Claude](https://github.com/fivetran/fivetran_connector_sdk/ai_and_connector_sdk/claude)

---

## Cursor

**Summary:**  
Cursor is an AI-powered code editor designed for productivity, smart completion, and real-time collaboration, with robust Python support.

**Contents:**
- [Fivetran Connector SDK System Instructions {Notepad}](https://github.com/fivetran/fivetran_connector_sdk/ai_and_connector_sdk/Fivetran_Connector_SDK.md)
- [Installation Guide](https://www.cursor.com/)
- [Using Cursor with Fivetran Connector SDK](https://github.com/fivetran/fivetran_connector_sdk/ai_and_connector_sdk/cursor/cursor_instructions.md)
- [Video Demo](Insert Link)

---

## Windsurf

**Summary:**  
Windsurf is a lightweight, cloud-based IDE for rapid prototyping and collaboration, supporting live sharing and direct browser development.

**Contents:**
- [Getting Started](https://docs.windsurf.com/windsurf/getting-started)
- [Context Awareness](https://docs.windsurf.com/context-awareness/windsurf-overview)
- [Fivetran Connector SDK System Instructions](https://github.com/fivetran/fivetran_connector_sdk/ai_and_connector_sdk/Fivetran_Connector_SDK.md)
- [Video Demo](Insert Link)

---

## Visual Studio Code

**Summary:**  
Visual Studio Code (VS Code) is a widely-used, open-source code editor with Python support, advanced debugging, and a rich extension library.

**Contents:**
- [Getting Started](https://code.visualstudio.com/docs/getstarted/getting-started)
- [Copilot in VS Code](https://code.visualstudio.com/docs/copilot/getting-started)
- [Fivetran Connector SDK System Instructions](https://github.com/fivetran/fivetran_connector_sdk/ai_and_connector_sdk/Fivetran_Connector_SDK.md)
- [Video Demo](Insert Link)

---

## Claude

**Summary:**  
Claude is an AI assistant by Anthropic, capable of generating and editing Python code for Fivetran connectors. Claude can automate and accelerate connector development and has been used to generate connectors with multi-table schemas.

## Claude Code
Claude Code has proved to be capable of successfully generating a connector capable of populating a multi-table schema during its initial run. The CLAUDE.md file in this folder contains instructions for Claude to be able to successfully build and debug connectors using Fivetran's Connector SDK. Start Claude Code in this folder so that it can pick up the CLAUDE.md content when generating responses. 

We also have [published a blog post](https://www.fivetran.com/blog/building-a-fivetran-connector-in-1-hour-with-anthropics-claude-ai) about our experiences and included the code as Claude generated it in the folder claude_pokeapi_example with the output of our exploration with Claude. The warehouse.db and state.json files are included here so you can easily review the output of the first sync even though these files are only temporary files created during testing and are not required for Connector SDK.

**Contents:**
- [Getting Started with Claude](https://docs.anthropic.com/en/docs/get-started)
- [Blog Post: Building a Fivetran Connector in 1 Hour with Claude AI](https://www.fivetran.com/blog/building-a-fivetran-connector-in-1-hour-with-anthropics-claude-ai)
- [Fivetran Connector SDK System Instructions](https://github.com/fivetran/fivetran_connector_sdk/ai_and_connector_sdk/Fivetran_Connector_SDK.md)
- [Claude md File](https://github.com/fivetran/fivetran_connector_sdk/ai_and_connector_sdk/claude/CLAUDE.md)
- [Video Demo](Insert Link)
  
---
