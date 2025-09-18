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
"- API example - This example shows how to sync data using an API key and secret."

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

The comments and suggestion you make should be deduplicated, for example - if you made a suggestion for one issue for lines 15 - 18, and then you want to post a suggestion for another issue for line 17, you should combine them in one suggestion.


---

# Copilot Review Instructions: Connector SDK Example README.md

Apply these rules to every `README.md` file inside Connector SDK **example folders**.

---

## Structure

* Every README must begin with **one H1 (`#`) heading** in Title Case.
* All other headings must be **sentence case**.
* Required top-level sections (in order):

  1. Connector overview
  2. Requirements
  4. Getting started
  5. Features
  6. Configuration file
  7. Requirements file
  8. Authentication
  9. Pagination
  10. Data handling
  11. Error handling
  12. Tables created (optional)
  13. Additional files (optional)
  14. Additional considerations

If a section is irrelevant (e.g. no error handling), **delete the heading entirely**. Do not leave empty stubs.

---

## Section Compliance

* **Connector overview** → Must explain purpose, data source, and use cases.
* **Requirements** → Must list OS and Python versions. Use provided bullet format. It is a fixed content, verbatim (no edits or omissions):
  ## Requirements
  - [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)   
  - Operating system:
    - Windows: 10 or later (64-bit only)
    - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
    - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

* **Getting started** → Must include link to Connector SDK Setup Guide. It is a fixed content, verbatim (no edits or omissions):
  ## Getting started
  Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.
* **Features** → Must list concrete features (not left blank or italicized template text).
* **Configuration file** → Must include a JSON code block showing keys. Must mention that `configuration.json` should not be versioned, for example:
  ## Configuration file

  The connector requires the following configuration parameters:

  ```
  {
    "host": "<YOUR_REDSHIFT_HOST>",
    "database": "<YOUR_REDSHIFT_DATABASE>",
    "port": "<YOUR_REDSHIFT_PORT>",
    "user": "<YOUR_REDSHIFT_USER>",
    "password": "<YOUR_REDSHIFT_PASSWORD>"
  }
  ```

  Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.
  
* **Requirements file** → Must explain role of `requirements.txt` and remind that `fivetran_connector_sdk` and `requests` are excluded, for example:

  ## Requirements file
  The connector requires the `redshift_connector` package to connect to Redshift databases.

  ```
  redshift_connector
  ```

  Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

* **Authentication** → Must specify auth method (API key, OAuth2, etc). For example:
  ## Authentication

  This connector uses username and password authentication to connect to Redshift. The credentials are specified in the configuration file and passed to the `redshift_connector.connect()` function (refer to the `connect_to_redshift()` function).

  To set up authentication:

  1. Create a Redshift user with appropriate permissions to access the required tables.
  2. Provide the username and password in the `configuration.json` file.
  3. Ensure the user has `SELECT` permissions on the tables that need to be synced.

* **Pagination / Data handling / Error handling** → Must either provide explanation + code reference (function name or line range in `connector.py`) or remove section entirely.
* **Tables created** → If ERD is present, must link to it with correct Markdown syntax and add summary of replicated tables. The table schemas should be presented as JSON or as a Markdown table, for example:
  ## Tables Created

  The connector creates a single table named `customers` with the following schema (refer to the `schema()` function):

  ```json
   {
    "table": "customers",
    "primary_key": ["customer_id"],
    "columns": {
      "customer_id": "INT",
      "first_name": "STRING",
      "last_name": "STRING",
      "email": "STRING",
      "updated_at": "UTC_DATETIME"
    }
  }
  ``` 
* **Additional files** → Must list additional Python files and explain purpose.
* **Additional considerations** → Must include Fivetran disclaimer about example code. It is a fixed content, verbatim (no edits or omissions):
  ## Additional considerations
  The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.


---

## Formatting Rules

* Do not use Title Case for anything below H1.
* Use fenced code blocks with language hints (`json`, `sql`, `bash`, etc.) where relevant.
* Paths, filenames, and config keys must be inline code.
* Functions in `connector.py` must be referenced explicitly (e.g. *Refer to `def handle_error`*).
* Use bold for filenames in lists under *Additional files*.

---

## Prohibited

* Placeholder italic text like *Provide a detailed overview\...* must not remain.
* Do not copy the template verbatim without replacement.
* Do not leave empty sections with no explanation.

---

✅ **Reviewer Action**: Flag any README.md that:

* Misses a required section.
* Keeps italicized template text.
* Uses incorrect heading case.
* Omits required references to config, requirements, or connector functions.


---

This way, Copilot Review is told: *“Don’t just look for README presence; enforce the structure, headings, and filled content.”*

---

Would you like me to also create a **deliberately bad test README.md** (like I did for the style guide) that violates these new rules, so you can validate Copilot flags them correctly?


Always analyse the connector example README.md files for compliance in terms of structure, format, and style with the template structure  README outlined below:

"# Connector SDK Individual Example README.md Structure Template

*Instructions: Every example added to the repository should have a comprehensive README.md to make it easy for users landing directly on the README to understand what the example does, how to configure it for their own use, and how to find more information about working with Connector SDK.*

*To use the template, replace the italicized text with your own description. Non-italicized text is common to all READMEs and should be left as is.*

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

