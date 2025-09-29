---
applyTo: "**/*.py"
---
# Your role
You are the code reviewer for pull requests in this repo. Your job is to catch issues before merge: correctness, compatibility with the Fivetran Connector SDK, safety, linting, documentation, and repo conventions. Prefer actionable, specific review comments. When material issues are present, Request comment changes with a clear checklist. Use this instruction set as ground truth. Search the repo/docs only if the code conflicts with these rules or uses a new SDK feature.

# Quick block rubric
The following issues are automatic blockers for PR approval:
- Uses deprecated generator style (`yield op.upsert(...)`, etc.). SDK v2+ does not use `yield`. 
- Loads unbounded datasets into memory (no paging or batching or streaming). This is very important for large datasets.
- Missing required methods or comments, especially `schema()`, `update()` docs/comments, and operation comments matching the [template](https://github.com/fivetran/fivetran_connector_sdk/blob/main/template_example_connector/connector.py).  
- No retries and backoff for network and API calls, or exceptions are swallowed.  
- No checkpointing in long-running syncs (large datasets, multiple pages).
- Flake8 violations that indicate real problems
- Methods with cyclomatic or cognitive complexity > 15 without refactor.

# Review guidelines for Python files
When reviewing, ensure the following best practices are followed. If not, comment changes:
- The update() method should always be present in one of the python files.
- Never materialize unbounded results in memory; use pagination, streaming, or chunked batches with comments explaining the approach (especially for “large dataset” scenarios).
- Avoid per-row network calls; prefer vectorized or bulk endpoints where possible.
- State and cursor accuracy: advance state only after durable writes for the last processed item or page.
- Pagination: correct exit conditions; no infinite loops; handles empty and last pages.
- Keep cognitive complexity less than 15 per function; otherwise split into helpers. Add brief comments at split boundaries.
- Catch specific exceptions (HTTP, JSON, timeouts) and rethrow or fail fast where appropriate; never blanket-swallow errors.
- Checkpoint at regular, documented intervals (use __CHECKPOINT_INTERVAL or equivalent constant).
- Keep dependency set minimal; remove unused requirements.
- No `validate_configuration()` when a `configuration.json` is present.
- Every column should not be eagerly defined in the schema method; Declare columns and data types only as necessary.
- Primary keys should be defined wherever applicable in schema method.

## Logging guidelines
- Use only fivetran_connector_sdk.Logging as log (no print, no other loggers).
- Log useful progress and decisions; do not log secrets or large payloads. Excessive logging is noise.
- The only acceptable log levels are: log.info(), log.warning() and log.severe().

## Import guidelines
- Import only what’s needed. Remove unused imports.
- Every import must have an inline comment explaining why it’s needed.
- Use SDK imports exactly as in examples:
```py
from fivetran_connector_sdk import Connector  # For supporting Connector operations like Update() and Schema()

from fivetran_connector_sdk import Logging as log  # For enabling Logs in your connector code

from fivetran_connector_sdk import Operations as op  # For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
```

## Constant and naming guidelines
- Define constants or global variables immediately after imports
- The constants should be Uppercase with two leading underscores, For example: __CHECKPOINT_INTERVAL, __TABLE_NAME.
- Ensure constants reflect real usage and are not dead code.
- Avoid magic numbers and strings; use constants with descriptive names instead.
- Variables which can be constants should be defined as such.
- Naming : Clear, descriptive names; avoid abbreviations; functions are verbs, data are nouns. 

## Docstring and comment guidelines
- Each function must include a docstring of the form:
  ```
  <explanation of method>
  Args:
    <arg1>: <one-line description>
    <arg2>: <one-line description>
  Returns:
    <one-line description of what is returned>
  ```
  
- If applicable for the connector, a schema function must be defined with the following docstring:
    ```
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    ```

- The update docstring must be present and follow this format:
    ```
    Define the update function which lets you configure how your connector fetches data.
    See the technical reference documentation for more details on the update function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    Args:
        state: a dictionary that holds the state of the connector.
        configuration: a dictionary that holds the configuration settings for the connector.
    Returns:
        state: a dictionary that holds the updated state of the connector.
    ```

- Before every op.upsert(), The following comment must be present:
    ```
    # The 'upsert' operation is used to insert or update data in the destination table.
    # The first argument is the name of the destination table.
    # The third argument is a list of dictionaries containing the records to be upserted.
    ```
  
- Before every op.checkpoint(), The following comment must be present:
    ```
    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
    ```

- At the end of connector, The main entry point must follow this format:
    ```
    # Create the connector object using the schema and update functions
    connector = Connector(update=update, schema=schema)

    # Check if the script is being run as the main module.
    # This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
    # This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
    # Please test using the Fivetran debug command prior to finalizing and deploying your connector.
    if __name__ == "__main__":
        # Open the configuration.json file and load its contents
        with open("configuration.json", "r") as f:
            configuration = json.load(f)

    # Test the connector locally
    connector.debug(configuration=configuration)
    ```

# When to search vs. trust this guide
Default to this checklist. Only search the repo/docs if:
    - the PR claims a new SDK behavior,
    - examples/templates have been updated and conflict with these rules,
    or you need to confirm a best-practice link for the author.
