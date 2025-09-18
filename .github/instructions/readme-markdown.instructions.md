---
applyTo: "fivetran_connector_sdk/examples/source_examples/**/*README*.md,fivetran_connector_sdk/connectors/**/*README*.md"
---

# Copilot Review Instructions for Fivetran Documentation

These rules override generic Copilot comments. Review all Markdown files that have "README" in their name against the following:

## Never use:

- Nested bulleted lists where a simple list would be sufficient
- Bold text to stress important information
Example of wrong format and style:
"- **API example**
   - This example shows how to sync data using an API key and secret."
The correct format and style would be as follows:
""- API example - This example shows how to sync data using an API key and secret."

## Headings:

- There should be only one H1 heading in the file, and it should use Title Case, for example: # Connector SDK Somecompany API Connector Example
- All other heading should use Sentence case, for example: ## Requirements file

## Lists:

Use bulleted lists when just listing items in no particular order or when listing the alternatives, for example:
"The management actions are as follows:
- Create connection
- Retrieve connection
- Delete connection"

Use numbered list for actions that need to be taken in a particular order, for example:
"To obtain your API key, do the following:
1. Log in to your DreemBleem account.
2. Go to **Settings > API Key**.
3. Make a note the API key and secret. You will need to add it to `configuration.json` file later."

## Comment and suggestions

The comments and suggestion you make should deduplicated, for example - if you made a suggestion for one issue for lines 15 - 18, and then you want to post a suggestion for another issue for line 17, you should combine them in one suggestion.

Always analyse the connector example README.md files for compliance in terms of the structure, format, and style with the following template README:

"# Connector SDK Individual Example README.md Structure Template

*Instructions: Every example added to the repository should have a comprehensive README.md to make it easy for users landing directly on the README to understand what the example does, how to configure it for their own use, and how to find more information about working with Connector SDK.*

*To use the template, replace the italicized text with your own description. Non-italicized text is common to all READMEs and can be left as is.*

*If a particular section is not relevant to your example (e.g. Error Handling \- your example doesn’t have any special error handling, Additional Files \- your example doesn’t have any additional files), delete that heading from the README.*

*Notate what section relates to what function in the connector.py using the function name or specific line block. (e.g. Error Handling \- Refer to def handle_critical_error(error_message, error_details=None),  Pagination \- Refer to lines 150-175)*

*Do not use Title Case for any heading level other than H1 (a single top-level heading with a single `#`).*


## Connector overview
*Provide a detailed overview of the connector, including its functionality, the data source it connects to, and the use cases it addresses.*


## Requirements
- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)   
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.


## Features
- *List key features of the connector, such as supported endpoints, data replication methods, and any special capabilities.*


## Configuration file
*Detail the configuration keys defined for your connector, which are uploaded to Fivetran from the configuration.json file.* 

```
{
  "api_key": "YOUR_API_KEY",
  "base_url": "https://api.example.com",
  "start_date": "2023-01-01"
}
```

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.


## Requirements file
*Explain the role of the `requirements.txt` file in specifying the Python libraries required by the connector.*

*Example content of `requirements.txt`:*

```
pandas
```

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.


## Authentication
*Explain the authentication mechanism used to access the source (e.g., API Key, OAuth2) and provide steps to obtain necessary credentials.*


## Pagination
*Describe how the connector handles pagination when retrieving data from the source.*


## Data handling
*Outline how data is processed, transformed, and delivered to Fivetran, including information on schema mapping and data types.*


## Error handling
*Explain the error-handling strategies implemented in the connector.*



## Tables created

*(Optional) If the ERD is available, add the ERD file to the same folder as the example's README.md and add the ERD link at beginning of this section using the below example as a template.* 
*Example: "The entity-relationship diagram (ERD) below shows how tables are linked in the <source-name> schema."*
*![<source-name>-ERD](<source-name-ERD>.png)*

*Summary of tables replicated.*

*(Optional) Screenshot of the schema objects generated*


## Additional files
Some connectors include additional files to modularize functionality. Provide a description of each additional file and its purpose.*

- **mock\_api.py** – *A simulated API for testing data retrieval.*  
- **users\_sync.py** – *Handles user data synchronization logic.*  
- **api\_threading\_utils.py** – *Manages API request threading for performance optimization.*  
- **constants.py** – *Stores constant values used throughout the connector.*


## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team."

