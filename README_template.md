# Connector SDK Individual Example README.md Structure Template

*Instructions: Every example added to the repository should have a comprehensive README.md to make it easy for users landing directly on the README to understand what the example does, how to configure it for their own use, and how to find more information about working with Connector SDK.*

*To use the template, replace the italicized text with your own description. Non-italicized text is common to all READMEs and can be left as is.*

*If a particular section is not relevant to your example (e.g. Error Handling \- your example doesn’t have any special error handling, Additional Files \- your example doesn’t have any additional files), delete that heading from the README.*

*Notate what section relates to what function in the connector.py using the function name or specific line block. (e.g. Error Handling \- Refer to def handle_critical_error(error_message, error_details=None),  Pagination \- Refer to lines 150-175)*

## Connector overview

*Provide a detailed overview of the connector, including its functionality, the data source it connects to, and the use cases it addresses.*

## Requirements

* [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)   
* Operating System:  
  * Windows 10 or later  
  * macOS 13 (Ventura) or later

## Getting started

Refer to the [Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

* *List key features of the connector, such as supported endpoints, data replication methods, and any special capabilities.*

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

## Tables Created

*Summary of Tables replicated.*

*Screenshot of the schema objects generated*

## Additional files

*Some connectors include additional files to modularize functionality. Provide a description of each additional file and its purpose.*

* **mock\_api.py** – *A simulated API for testing data retrieval.*  
* **users\_sync.py** – *Handles user data synchronization logic.*  
* **api\_threading\_utils.py** – *Manages API request threading for performance optimization.*  
* **constants.py** – *Stores constant values used throughout the connector.*

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
