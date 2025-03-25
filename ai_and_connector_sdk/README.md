# Introduction
This section of our example repository is designed to help you use emerging AI tools with the Connector SDK to build custom connectors.


# ChatGPT
ChatGPT can be effectively used to help you leverage our Connector SDK to build custom connectors. This was achieved by passing examples from this repository along with the full [technical reference](https://fivetran.com/docs/connector-sdk/technical-reference) documentation to ChatGPT. ChatGPT, when tested, required some manual adjustments of the created code.

# Claude
Claude has proved to be capable of creating a connector where the connection populated a multi-table schema at the first run. There were extra unnecessary requirements included in the `requirements.txt` file, and the `__main__` top-level code environment didn't include the code needed to run the connection using my IDE run button. We [created a blog article](www.fivetran.com/blog/building-a-fivetran-connector-in-1-hour-with-anthropics-claude-ai) about our experiences and included the code as Claude generated it. We have also included the `warehouse.db` and `state.json` files so you can easily review the output of the first sync even though these files are only temporary files created during testing and are not required for Connector SDK.