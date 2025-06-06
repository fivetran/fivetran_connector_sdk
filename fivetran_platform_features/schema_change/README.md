# Schema Change Example Connector

## Connector Overview

This connector demonstrates how Fivetran handles schema changes in Fivetran connectors by returning data with different types across multiple syncs. It shows how field types can evolve over time, from integer to float to string and back to float, while [Fivetran automatically promotes the column type to the most specific data type that losslessly accepts both the old and new data](https://fivetran.com/docs/core-concepts#changingdatatype)

This connector will not run more than once with ```fivetran_debug```, since the Fivetran tester does not handle schema changes. In order to observe the data type changing for column int_to_string in your Fivetran destination, we recommend this sequence: 
1. Deploy this connector to Fivtran using `fivetran deploy`.
2. In the Fivetran UI, trigger an initial sync for the connector. A single record will be written to the table change_int_to_string. 
3. In your destination schema, observe the table change_int_to_string. The column int_to_string should be a type that represents integers. The exact type will vary across platforms.
4. In the Fivetran UI, trigger another sync. This sync will insert a float-point value into change_int_to_string.int_to_string and change the target data type to a type that can represent floating point numbers.
5. In your destination schema, again observe the table change_int_to_string. The column int_to_string should now be a type that represents floating point numbers. The exact type will vary across platforms.
6. Repeat steps 5 and 6 twice more. The third sync will insert a string into change_int_to_string.int_to_string and change the data type once more. The fourth sync will insert another floating point number into change_int_to_string.int_to_string but the data type will not change.
7. In your destination, if access allows, observe the query history for Fivetran's user. The queries that Fivetran used to make changes to change_int_to_string table should be visible there.


## Requirements

* [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)   
* Operating System:  
  * Windows 10 or later  
  * macOS 13 (Ventura) or later

## Getting Started

Refer to the [Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

* **Schema Evolution**: Demonstrates how field types can evolve across syncs
* **Type Changes**: Shows handling of integer → float → string → float transitions
* **State Management**: Uses checkpointing to track sync count
* **Logging**: Includes informative logging of schema changes

## Configuration File

This connector does not require any configuration as it demonstrates schema changes using local data. The `configuration.json` file is not needed.

## Requirements File

The connector uses minimal external dependencies. The `requirements.txt` file should be empty as all required packages are pre-installed in the Fivetran environment.

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication

This connector does not require authentication as it demonstrates schema changes using local data.

## Pagination

This connector does not implement pagination as it works with a single record that changes type across syncs.

## Data Handling

The connector processes data in the following way:
1. Tracks sync count in state
2. Changes the type of `int_to_string` field across syncs:
   - First sync: integer (42)
   - Second sync: float (42.42)
   - Third sync: string ("forty-two point forty-two")
   - Fourth+ syncs: float (42.43)
3. Includes descriptive text for each sync's data type

## Error Handling

This connector does not implement error handling as there are minimal opportunities for errors.

## Additional Files

This connector consists of a single file:
* **connector.py** – Contains the main connector logic and schema change demonstration

## Additional Considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team. 
