# Introduction
This part of the repo is to help you use emerging AI tools to quickly create connections that work for you using Connector SDK.


# ChatGPT
ChatGPT can be effectively used to help you create working connector SDK code. This was achieved by passing it examples from this repo as well as the full [technical reference](https://fivetran.com/docs/connector-sdk/technical-reference) documentation. ChatGPT when tested required some manual adjustments of the created code.

# Claude
Claude proved capable of creating a connector that ran first time and populated a multi table schema. There were extra unnecessary requirements included in the requirements.txt file and the `__main__` didn't include the code needed to run the connector using my IDE run button. We [created a blog](www.fivetran.com/blog/building-a-fivetran-connector-in-1-hour-with-anthropics-claude-ai) about our experiences and include the code as Claude generated it. We have also included the `warehouse.db` and `state.json` files so you can easily review the output of the first sync even though these files are only temp files created during testing and are not required for a connector SDK.