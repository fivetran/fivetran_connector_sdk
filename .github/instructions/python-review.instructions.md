---
applyTo: "**/*.py"
---
# Your role
You are the code reviewer for pull requests in this repo. Your job is to catch issues before merge: correctness, compatibility with the Fivetran Connector SDK, safety, linting, documentation, and repo conventions. Prefer actionable, specific review comments. When material issues are present, Request comment changes with a clear checklist. Use this instruction set as ground truth. Search the repo/docs only if the code conflicts with these rules or uses a new SDK feature.

# Quick block rubric (BLOCKER - PR cannot be approved)
The following issues are **automatic blockers** for PR approval:

## Critical SDK v2+ Violations
- **Uses deprecated `yield` pattern**: SDK v2+ does NOT use `yield` with operations
  - BLOCKER: `yield op.upsert(...)`, `yield op.checkpoint(...)`
  - CORRECT: `op.upsert(...)`, `op.checkpoint(...)`
- **Has `validate_configuration()` function** when `configuration.json` exists (SDK auto-validates)
- **Wrong import statements**: Not using SDK imports correctly
  - BLOCKER: `from fivetran_connector_sdk import operations as op`
  - CORRECT: `from fivetran_connector_sdk import Operations as op`

## Error Handling Issues
- **Generic exception catching** without re-raising: `except Exception: pass`
- **No retry logic** for network calls, API requests, or transient failures
- **Swallowed exceptions**: Catching errors without logging or re-raising

## Code Quality Issues
- **Missing required docstrings**: Especially `update()`, `schema()`, and operation comments
- **Cognitive complexity > 15** per function without refactoring into helpers
- **Flake8 violations** that indicate real problems (unused imports, syntax issues)
- **Missing required comments** before `op.upsert()`, `op.checkpoint()`, and main block

# Review guidelines for Python files

## Required Methods and Structure (BLOCKER if missing)
- **`update()` function is mandatory**: Must be present and properly implemented
  - Must accept `configuration` and `state` parameters
  - Must return nothing (state is updated via `op.checkpoint()`)
- **`schema()` function strongly recommended**: Should be present unless schema is fully dynamic
  - Must accept `configuration` parameter
  - Must return list of table schema dictionaries
- **Connector initialization**: Must be present at module level
  ```python
  connector = Connector(update=update, schema=schema)
  ```
- **Main block for debugging**: Must be present at end of file
  ```python
  if __name__ == "__main__":
      with open("configuration.json", "r") as f:
          configuration = json.load(f)
      connector.debug(configuration=configuration)
  ```

## Memory Management and Performance (BLOCKER if violated)
- **Never materialize unbounded data**: Use pagination, streaming, or chunked batches
  - BLOCKER: `all_records = list(api.get_all())` (loads everything into memory)
  - BLOCKER: `data = []; for page in pages: data.extend(page)` then process
  - CORRECT: Process each page immediately within the loop
  - **Add comments** explaining pagination/streaming approach for clarity
- **Avoid per-row network calls**: Prefer bulk/batch endpoints
  - BLOCKER: `for user_id in user_ids: fetch_user(user_id)`
  - CORRECT: `fetch_users_batch(user_ids)`
- **Stream large responses**: For APIs returning large payloads, use streaming
  - CORRECT: `response.iter_lines()` or `response.iter_content()`

## State Management and Checkpointing (BLOCKER if violated)
- **Accurate state progression**: Only advance state **after** successful data writes
  - CORRECT: `op.upsert(...)` then update state, then `op.checkpoint(state)`
  - BLOCKER: Update state before confirming data was written
- **Checkpoint frequency**: Must checkpoint at appropriate intervals
  - **In pagination loops**: Checkpoint after each page
  - **Large datasets**: Define `__CHECKPOINT_INTERVAL` constant (e.g., 1000 records)
  - **Multi-table syncs**: Checkpoint after completing each table
- **Checkpoint placement**: Always checkpoint **after** operations, not before
  ```python
  # CORRECT order
  for page in paginate():
      for record in page:
          op.upsert(table_name, record)
      op.checkpoint(state)  # After processing page
  ```
- **State structure**: Use clear, descriptive keys
  - GOOD: `{"last_sync_timestamp": "2024-01-15T10:30:00Z", "last_user_id": 12345}`
  - BAD: `{"t": "2024-01-15", "id": 12345}` (cryptic keys)

## Pagination Patterns (BLOCKER if incorrect)
- **Correct exit conditions**: Must handle end of pagination properly
  - Check for: empty pages, missing `next` token, `has_more=false`, etc.
  - BLOCKER: Infinite loops when API returns empty results
- **Handle edge cases**:
  - Empty first page (no data available)
  - Last page (no next token)
  - Single page result

## Error Handling and Resilience (BLOCKER if violated)
- **Catch specific exceptions**: Never use bare `except Exception:`
  - CORRECT: `except (requests.HTTPError, requests.Timeout, ConnectionError) as e:`
  - BLOCKER: `except Exception: pass` (swallows everything)
  - BLOCKER: `except: pass` (even worse)
- **Implement retry logic**: For transient failures (network, rate limits, 5xx errors)
  - Use exponential backoff: `sleep_time = min(60, 2 ** retry_count)`
  - Limit retry attempts (e.g., 3-5 retries)
  - Log each retry attempt
  ```python
  for attempt in range(__MAX_RETRIES):
      try:
          response = make_api_call()
          break
      except (requests.Timeout, requests.ConnectionError) as e:
          if attempt == __MAX_RETRIES - 1:
              raise
          sleep_time = min(60, 2 ** attempt)
          log.warning(f"Retry {attempt + 1}/{__MAX_RETRIES} after {sleep_time}s")
          time.sleep(sleep_time)
  ```
- **Fail fast for permanent errors**: Don't retry authentication failures, 4xx client errors
  - Re-raise immediately for: 401, 403, 404, 400 with invalid params
- **Never swallow exceptions silently**: Always log before re-raising or handle explicitly

## Code Complexity Management (REQUEST_CHANGES if violated)
- **Cognitive complexity < 15**: Split complex functions into helpers
  - Use helper functions with clear names
  - Add docstrings to helpers
  - Add brief comments at split boundaries
- **Single Responsibility**: Each function should do one thing well
  - Separate data fetching from processing
  - Separate pagination logic from business logic
  - Separate error handling from core logic
- **Avoid deep nesting**: Use early returns, extract conditions to variables
  - BAD: 4+ levels of indentation
  - GOOD: Guard clauses, extracted helper functions

## Dependencies and Imports (REQUEST_CHANGES if violated)
- **Minimal dependencies**: Only include what's actually needed
  - Remove unused packages from `requirements.txt`
  - Avoid heavyweight libraries for simple tasks
  - Don't use pandas just for JSON parsing
- **NO forbidden imports**: Never include these in `requirements.txt`:
  - `fivetran_connector_sdk` (provided by runtime)
  - `requests` (provided by runtime)
- **Every import needs a comment**: Explain why each import is needed
  ```python
  import json  # For reading configuration from JSON file
  from fivetran_connector_sdk import Connector  # For connector initialization
  import pandas as pd  # For CSV parsing and data transformation
  ```

## Logging Guidelines (BLOCKER if wrong logger used)
- **Only use SDK logging**: `from fivetran_connector_sdk import Logging as log`
  - BLOCKER: `print()` statements
  - BLOCKER: `import logging` or Python's logging module
  - BLOCKER: Other logging frameworks (loguru, structlog, etc.)
- **Four log levels only**:
  - `log.info()` - Progress updates, cursors, pagination status (moderate frequency)
    - Example: `log.info(f"Processing page {page_num} of {total_pages}")`
    - Example: `log.info(f"Current cursor: {cursor_value}")`
  - `log.warning()` - Potential issues, rate limit warnings, retry attempts
    - Example: `log.warning(f"Rate limit approaching: {remaining_calls} calls left")`
    - Example: `log.warning(f"Retrying request, attempt {retry_count}")`
  - `log.severe()` - Errors, failures, critical issues requiring attention
    - Example: `log.severe(f"API request failed: {error_message}")`
    - Example: `log.severe(f"Authentication error: {auth_error}")`
  - `log.fine()` - Detailed debug info, very high frequency, only for deep debugging
    - Example: `log.fine(f"Full API response: {response_json}")`
- **NO excessive logging**: Do not log every record or inside tight loops
  - BAD: Logging each record in a loop of 10,000 records
  - GOOD: Log every N records or at page boundaries
- **NO secrets in logs**: Never log:
  - API keys, tokens, passwords
  - Full configuration objects
  - Personal data (emails, phone numbers, addresses)
  - Large payloads (> 100 lines of data)
- **Useful context**: Include relevant identifiers
  - GOOD: `log.info(f"Syncing table {table_name}, cursor: {cursor}")`
  - BAD: `log.info("Processing data")` (too vague)

## Schema Definition Best Practices (REQUEST_CHANGES if violated)
- **Minimal schema approach**: Only define what's necessary
  - GOOD: Define only table name and primary_key
  - BAD: Eagerly defining all 50+ columns with data types
- **When to define columns**:
  - When specific data type is required (e.g., INTEGER vs STRING)
  - When SDK cannot infer type correctly (rare)
  - When column names need explicit documentation
- **Always define primary keys**: Specify primary_key wherever a unique identifier exists
  - GOOD: `"primary_key": ["id"]` or `"primary_key": ["user_id", "timestamp"]`
  - BAD: Omitting primary_key when table has unique identifiers
- **Example minimal schema**:
  ```python
  def schema(configuration: dict):
      """
      Define the schema function which lets you configure the schema your connector delivers.
      See the technical reference documentation for more details on the schema function:
      https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
      Args:
          configuration: a dictionary that holds the configuration settings for the connector.
      """
      return [
          {
              "table": "users",
              "primary_key": ["user_id"]
          },
          {
              "table": "transactions",
              "primary_key": ["transaction_id"],
              "columns": {
                  "transaction_id": "STRING",
                  "amount": "DECIMAL",  # Explicitly define when precision matters
                  "created_at": "UTC_DATETIME"
              }
          }
      ]
  ```

## Import Guidelines (REQUEST_CHANGES if violated)
- **Import only what's needed**: Remove unused imports (flake8 will catch)
- **Every import needs inline comment**: Explain why each import is needed
- **Standard SDK imports** (use exactly as shown):
  ```python
  # For reading configuration from a JSON file
  import json

  # Import required classes from fivetran_connector_sdk
  from fivetran_connector_sdk import Connector

  # For enabling Logs in your connector code
  from fivetran_connector_sdk import Logging as log

  # For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
  from fivetran_connector_sdk import Operations as op
  ```
- **Third-party imports**: Always comment the specific purpose
  ```python
  import requests  # For making HTTP API requests (NOTE: provided by SDK runtime)
  import pandas as pd  # For CSV data parsing and transformation
  import clickhouse_connect  # For connecting to ClickHouse database
  ```
- **Import order**: Follow PEP 8
  1. Standard library imports (json, datetime, time)
  2. SDK imports (fivetran_connector_sdk)
  3. Third-party imports (requests, pandas, etc.)

## Constants and Naming Guidelines (REQUEST_CHANGES if violated)
- **Define constants after imports**: Place all constants/globals immediately after import statements
- **Naming convention for constants**: Uppercase with double leading underscores
  - GOOD: `__CHECKPOINT_INTERVAL = 1000`, `__TABLE_NAME = "users"`, `__MAX_RETRIES = 5`
  - BAD: `CHECKPOINT_INTERVAL = 1000` (missing double underscore)
  - BAD: `checkpoint_interval = 1000` (not uppercase for constant)
- **Private by default**: Use double underscore prefix to keep constants module-private
  - Constants are private unless specifically needed by external modules
- **No magic numbers**: Define constants for all literal numbers and strings
  - BAD: `if len(batch) >= 1000:` (magic number)
  - GOOD: `__BATCH_SIZE = 1000` then `if len(batch) >= __BATCH_SIZE:`
  - BAD: `sleep(30)` (magic number)
  - GOOD: `__RETRY_DELAY_SECONDS = 30` then `sleep(__RETRY_DELAY_SECONDS)`
- **Ensure constants are used**: Remove unused constants (dead code)
- **Promote variables to constants**: If a variable value never changes, make it a constant
- **Descriptive naming conventions**:
  - **Functions**: Use verbs describing actions
    - GOOD: `fetch_users()`, `process_records()`, `handle_pagination()`
    - BAD: `users()`, `data()`, `pagination()` (nouns, not verbs)
  - **Variables**: Use nouns describing data
    - GOOD: `user_records`, `api_response`, `last_sync_time`
    - BAD: `fetch`, `get`, `process` (verbs, not nouns)
  - **NO abbreviations**: Use full words for clarity
    - BAD: `cfg`, `resp`, `rec`, `usr`, `db`, `conn`
    - GOOD: `configuration`, `response`, `record`, `user`, `database`, `connection`
  - **Boolean variables**: Use is/has/should prefix
    - GOOD: `is_authenticated`, `has_more_pages`, `should_retry`
    - BAD: `authenticated`, `more_pages`, `retry` (unclear boolean) 

## Docstring and Comment Guidelines (BLOCKER if missing required docstrings)

### Required Function Docstrings (BLOCKER if missing or incorrect)

**1. Every function must have a docstring** following this format:
```python
def function_name(arg1, arg2):
    """
    Brief description of what the function does.
    Args:
        arg1: One-line description of arg1.
        arg2: One-line description of arg2.
    Returns:
        Description of what is returned (or omit if function returns None).
    """
```

**2. Schema function docstring** (BLOCKER - must be exact):
```python
def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
```

**3. Update function docstring** (BLOCKER - must be exact):
```python
def update(configuration: dict, state: dict):
    """
    Define the update function which lets you configure how your connector fetches data.
    See the technical reference documentation for more details on the update function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
        state: a dictionary that holds the state of the connector.
    """
```
Note: Update function should NOT have a Returns section (state is managed via checkpoint)

### Required Operation Comments (BLOCKER if missing)

**Before EVERY `op.upsert()`** call:
```python
# The 'upsert' operation is used to insert or update data in the destination table.
# The first argument is the name of the destination table.
# The second argument is a dictionary containing the record to be upserted.
op.upsert(table="table_name", data=record)
```

**Before EVERY `op.checkpoint()`** call:
```python
# Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
# from the correct position in case of next sync or interruptions.
# Learn more about how and where to checkpoint by reading our best practices documentation
# (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
op.checkpoint(state)
```

### Required End-of-File Comments (BLOCKER if missing or incorrect)

**At the end of connector.py**:
```python
# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Test the connector locally
    connector.debug()
```

### Helper Function Comments (REQUEST_CHANGES if missing)
- **Complex logic**: Add comments explaining non-obvious logic
- **Business rules**: Document why, not just what
- **API-specific behavior**: Note any API quirks or special handling
- **Performance considerations**: Explain streaming, batching, or optimization choices
- **Pagination**: Explain how pagination works and exit conditions

### When NOT to Comment
- Don't comment obvious code: `x = 1  # Set x to 1`
- Don't repeat function name: `def get_users(): # Get users`
- Don't use outdated comments: Update comments when code changes

# Code Review Checklist Summary

When reviewing Python connector code, systematically check:

1. **SDK v2+ compliance**: No `yield`,correct imports
2. **Memory safety**: Pagination/streaming for large datasets, no unbounded memory loading
3. **State & checkpoints**: Proper state management, checkpoints after operations, frequent checkpointing
4. **Error handling**: Specific exceptions, retry logic with backoff, fail fast on permanent errors
5. **Required docstrings**: Update, schema, and all helper functions properly documented
6. **Required comments**: Before upsert, checkpoint, and main block
7. **Logging**: Only SDK logging, appropriate log levels, no secrets
8. **Schema**: Minimal approach with primary keys
9. **Code quality**: Complexity < 15, descriptive names, no magic numbers
10. **Dependencies**: Minimal, all commented, no forbidden imports
