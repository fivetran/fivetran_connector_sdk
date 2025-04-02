# Introduction
This section of our example repository is designed to help you use emerging AI tools with the Connector SDK to build custom connectors.


# ChatGPT
ChatGPT can be effectively used to help you leverage our Connector SDK to build custom connectors. This was achieved by passing examples from this repository along with the full [technical reference](https://fivetran.com/docs/connector-sdk/technical-reference) documentation to ChatGPT. When tested, it does require some manual adjustments of the created code.

# Claude Code
Claude Code has proved to be capable of successfully generating a connector capable of populating a multi-table schema during its initial run. The CLAUDE.md file in this folder contains instructions for Claude to be able to successfully build and debug connectors using Fivetran's Connector SDK. Start Claude Code in this folder so that it can pick up the CLAUDE.md content when generating responses. 

We also have [published a blog post](https://www.fivetran.com/blog/building-a-fivetran-connector-in-1-hour-with-anthropics-claude-ai) about our experiences and included the code as Claude generated it in the folder claude_pokeapi_example with the output of our exploration with Claude. The warehouse.db and state.json files are included here so you can easily review the output of the first sync even though these files are only temporary files created during testing and are not required for Connector SDK.
