"""Paylocity connector for syncing employee data, payroll, benefits, and HR information.
This connector demonstrates how to fetch data from Paylocity API and upsert it into destination using memory-efficient streaming patterns.
See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

# For reading configuration from a JSON file
import json

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# For making HTTP requests to Paylocity API
import requests

# For handling dates and timestamps
from datetime import datetime, timedelta, timezone

# For implementing delays in retry logic and rate limiting
import time

""" ADD YOUR SOURCE-SPECIFIC IMPORTS HERE
Example: import pandas, boto3, etc.
Add comment for each import to explain its purpose for users to follow.
"""

# Private constants (use __ prefix)
__INVALID_LITERAL_ERROR = "invalid literal"
__TOKEN_URL_SANDBOX = "https://dc1demogwext.paylocity.com/public/security/v1/token"
__TOKEN_URL_PRODUCTION = "https://dc1prodgwext.paylocity.com/public/security/v1/token"
__API_BASE_URL_SANDBOX = "https://dc1demogwext.paylocity.com/public"
__API_BASE_URL_PRODUCTION = "https://dc1prodgwext.paylocity.com/public"


def __get_config_int(
    configuration: dict, key: str, default: int, min_val: int = None, max_val: int = None
) -> int:
    """
    Parse and validate an integer configuration parameter with optional min/max constraints.
    Args:
        configuration: A dictionary that holds the configuration settings for the connector.
        key: The configuration key to retrieve.
        default: The default value to use if the key is missing or invalid.
        min_val: Optional minimum value constraint.
        max_val: Optional maximum value constraint.
    Returns:
        The parsed integer value, constrained to min_val and max_val if specified.
    Raises:
        ValueError: If the value cannot be parsed and is not using the default.
    """
    try:
        value = int(configuration.get(key, default))
        if min_val is not None and value < min_val:
            log.warning(f"Config {key}={value} below minimum {min_val}, using {min_val}")
            return min_val
        if max_val is not None and value > max_val:
            log.warning(f"Config {key}={value} above maximum {max_val}, using {max_val}")
            return max_val
        return value
    except (ValueError, TypeError) as e:
        if __INVALID_LITERAL_ERROR in str(e).lower():
            log.warning(f"Invalid {key} format, using default: {default}")
            return default
        raise ValueError(f"Failed to parse {key}: {str(e)}")


def __get_config_bool(configuration: dict, key: str, default: bool) -> bool:
    """
    Parse a boolean configuration parameter from various input formats.
    Args:
        configuration: A dictionary that holds the configuration settings for the connector.
        key: The configuration key to retrieve.
        default: The default value to use if the key is missing.
    Returns:
        The parsed boolean value.
    """
    value = configuration.get(key, default)
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ("true", "1", "yes", "on")
    return bool(value)


def __calculate_wait_time(
    attempt: int, response_headers: dict, base_delay: int = 1, max_delay: int = 60
) -> int:
    """
    Calculate wait time with exponential backoff and jitter for rate limiting and retries.
    Args:
        attempt: The current retry attempt number (0-indexed).
        response_headers: HTTP response headers that may contain Retry-After information.
        base_delay: The base delay in seconds for exponential backoff.
        max_delay: The maximum delay in seconds.
    Returns:
        The calculated wait time in seconds.
    """
    # Check for Retry-After header
    retry_after = response_headers.get("Retry-After")
    if retry_after:
        try:
            return min(int(retry_after), max_delay)
        except (ValueError, TypeError):
            pass

    # Exponential backoff with jitter
    delay = min(base_delay * (2**attempt), max_delay)
    return delay


def __handle_rate_limit(attempt: int, response: requests.Response):
    """
    Handle HTTP 429 rate limiting by waiting for the appropriate duration.
    Args:
        attempt: The current retry attempt number.
        response: The HTTP response object containing rate limit information.
    """
    wait_time = __calculate_wait_time(attempt, response.headers)
    log.warning(f"Rate limited, waiting {wait_time} seconds (attempt {attempt + 1})")
    time.sleep(wait_time)


def __handle_request_error(attempt: int, retry_attempts: int, error: Exception, endpoint: str):
    """
    Handle request errors with retry logic and exponential backoff.
    Args:
        attempt: The current retry attempt number.
        retry_attempts: The total number of retry attempts allowed.
        error: The exception that occurred during the request.
        endpoint: The API endpoint that failed.
    """
    if attempt < retry_attempts - 1:
        wait_time = __calculate_wait_time(attempt, {})
        log.warning(
            f"Request to {endpoint} failed (attempt {attempt + 1}): {str(error)}. Retrying in {wait_time}s"
        )
        time.sleep(wait_time)
    else:
        log.severe(f"Final attempt failed for {endpoint}: {str(error)}")


def get_access_token(
    client_id: str, client_secret: str, is_sandbox: bool = True, configuration: dict = None
) -> str:
    """
    Get OAuth2 access token from Paylocity authentication endpoint.
    Args:
        client_id: The Paylocity OAuth2 client ID.
        client_secret: The Paylocity OAuth2 client secret.
        is_sandbox: Whether to use the sandbox or production environment.
        configuration: A dictionary that holds additional configuration settings for the connector.
    Returns:
        The OAuth2 access token string.
    Raises:
        RuntimeError: If access token cannot be obtained after retries.
        ValueError: If the token response does not contain an access_token.
    """
    token_url = __TOKEN_URL_SANDBOX if is_sandbox else __TOKEN_URL_PRODUCTION

    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
    }

    timeout = __get_config_int(configuration, "request_timeout_seconds", 30)
    retry_attempts = __get_config_int(configuration, "retry_attempts", 3)

    for attempt in range(retry_attempts):
        try:
            response = requests.post(token_url, headers=headers, data=data, timeout=timeout)

            if response.status_code == 429:
                __handle_rate_limit(attempt, response)
                continue

            response.raise_for_status()
            token_data = response.json()

            access_token = token_data.get("access_token")
            if not access_token:
                raise ValueError("No access_token in response")

            log.info("Successfully obtained access token")
            return access_token

        except requests.exceptions.RequestException as e:
            __handle_request_error(attempt, retry_attempts, e, "token endpoint")
            continue

    raise RuntimeError("Failed to obtain access token after retries")


def execute_api_request(
    endpoint: str, access_token: str, params: dict = None, configuration: dict = None
) -> dict:
    """
    Execute an API request to Paylocity with retry logic and rate limit handling.
    Args:
        endpoint: The API endpoint path to call.
        access_token: The OAuth2 access token for authentication.
        params: Optional query parameters to include in the request.
        configuration: A dictionary that holds additional configuration settings for the connector.
    Returns:
        The JSON response from the API as a dictionary.
    Raises:
        RuntimeError: If the API request fails after all retry attempts.
    """
    is_sandbox = __get_config_bool(configuration, "use_sandbox", True)
    base_url = __API_BASE_URL_SANDBOX if is_sandbox else __API_BASE_URL_PRODUCTION
    url = f"{base_url}{endpoint}"

    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}

    timeout = __get_config_int(configuration, "request_timeout_seconds", 30)
    retry_attempts = __get_config_int(configuration, "retry_attempts", 3)

    for attempt in range(retry_attempts):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=timeout)

            if response.status_code == 429:
                __handle_rate_limit(attempt, response)
                continue

            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            __handle_request_error(attempt, retry_attempts, e, endpoint)
            continue

    raise RuntimeError(f"Unexpected error in API request execution for {endpoint}")


def get_time_range(last_sync_time: str = None, configuration: dict = None) -> dict:
    """
    Generate a time range for incremental or initial data synchronization.
    Args:
        last_sync_time: The timestamp of the last successful sync in ISO format. If None, uses initial_sync_days.
        configuration: A dictionary that holds the configuration settings for the connector.
    Returns:
        A dictionary containing "start" and "end" time strings in ISO format.
    """
    end_time = datetime.now(timezone.utc).isoformat()

    if last_sync_time:
        start_time = last_sync_time
    else:
        initial_sync_days = __get_config_int(configuration, "initial_sync_days", 90)
        start_time = (datetime.now(timezone.utc) - timedelta(days=initial_sync_days)).isoformat()

    return {"start": start_time, "end": end_time}


def __map_employee_data(employee: dict, company_id: str) -> dict:
    """
    Map employee data from Paylocity API response to destination table format.
    Args:
        employee: A dictionary containing employee data from the Paylocity API.
        company_id: The Paylocity company identifier.
    Returns:
        A dictionary containing mapped employee data ready for upsert.
    """
    return {
        "company_id": company_id,
        "employee_id": employee.get("employeeId", ""),
        "first_name": employee.get("firstName", ""),
        "last_name": employee.get("lastName", ""),
        "middle_name": employee.get("middleName", ""),
        "email": employee.get("email", ""),
        "employee_status": employee.get("employeeStatus", ""),
        "hire_date": employee.get("hireDate", ""),
        "birth_date": employee.get("birthDate", ""),
        "department": employee.get("department", ""),
        "position": employee.get("position", ""),
        "supervisor_id": employee.get("supervisorId", ""),
        "pay_rate": employee.get("payRate", ""),
        "pay_frequency": employee.get("payFrequency", ""),
        "home_phone": employee.get("homePhone", ""),
        "mobile_phone": employee.get("mobilePhone", ""),
        "address_line1": employee.get("addressLine1", ""),
        "address_line2": employee.get("addressLine2", ""),
        "city": employee.get("city", ""),
        "state": employee.get("state", ""),
        "zip_code": employee.get("zipCode", ""),
        "ssn": employee.get("ssn", ""),
        "gender": employee.get("gender", ""),
        "marital_status": employee.get("maritalStatus", ""),
        "is_active": employee.get("isActive", True),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def get_employees(
    access_token: str, company_id: str, last_sync_time: str = None, configuration: dict = None
):
    """
    Fetch employee data using a streaming generator approach to prevent memory issues.
    This function yields employee records one at a time rather than accumulating all records in memory.
    Args:
        access_token: The OAuth2 access token for authentication.
        company_id: The Paylocity company identifier.
        last_sync_time: The timestamp of the last successful sync in ISO format.
        configuration: A dictionary that holds the configuration settings for the connector.
    Yields:
        A dictionary containing employee data ready for upsert into the destination table.
    """
    endpoint = f"/coreHr/v1/companies/{company_id}/employees"
    max_records = __get_config_int(configuration, "max_records_per_page", 20)

    # Get only active employees for incremental sync optimization
    active_only = __get_config_bool(configuration, "active_employees_only", False)

    params = {
        "limit": max_records,
        "include": "info,position,status,payRate",  # Get comprehensive employee data
    }

    if active_only:
        params["activeOnly"] = "true"

    next_token = None

    while True:
        if next_token:
            params["nextToken"] = next_token

        response = execute_api_request(endpoint, access_token, params, configuration)

        employees = response.get("employees", [])
        if not employees:
            break

        # Yield individual records instead of accumulating
        for employee in employees:
            yield __map_employee_data(employee, company_id)

        # Check for pagination
        next_token = response.get("nextToken")
        if not next_token:
            break


def __map_payroll_data(payroll_record: dict, company_id: str) -> dict:
    """
    Map payroll data from Paylocity API response to destination table format.
    Args:
        payroll_record: A dictionary containing payroll data from the Paylocity API.
        company_id: The Paylocity company identifier.
    Returns:
        A dictionary containing mapped payroll data ready for upsert.
    """
    return {
        "company_id": company_id,
        "employee_id": payroll_record.get("employeeId", ""),
        "pay_period_start": payroll_record.get("payPeriodStart", ""),
        "pay_period_end": payroll_record.get("payPeriodEnd", ""),
        "pay_date": payroll_record.get("payDate", ""),
        "gross_pay": payroll_record.get("grossPay", 0.0),
        "net_pay": payroll_record.get("netPay", 0.0),
        "total_deductions": payroll_record.get("totalDeductions", 0.0),
        "total_taxes": payroll_record.get("totalTaxes", 0.0),
        "regular_hours": payroll_record.get("regularHours", 0.0),
        "overtime_hours": payroll_record.get("overtimeHours", 0.0),
        "payroll_run_id": payroll_record.get("payrollRunId", ""),
        "check_number": payroll_record.get("checkNumber", ""),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def get_payroll_data(
    access_token: str, company_id: str, last_sync_time: str = None, configuration: dict = None
):
    """
    Fetch payroll data using a streaming generator approach to prevent memory issues.
    This function yields payroll records one at a time rather than accumulating all records in memory.
    Args:
        access_token: The OAuth2 access token for authentication.
        company_id: The Paylocity company identifier.
        last_sync_time: The timestamp of the last successful sync in ISO format.
        configuration: A dictionary that holds the configuration settings for the connector.
    Yields:
        A dictionary containing payroll data ready for upsert into the destination table.
    """

    endpoint = f"/payroll/v1/companies/{company_id}/payrollRuns"
    max_records = __get_config_int(configuration, "max_records_per_page", 20)

    time_range = get_time_range(last_sync_time, configuration)

    params = {
        "limit": max_records,
        "startDate": time_range["start"][:10],  # Extract date part
        "endDate": time_range["end"][:10],
    }

    next_token = None

    while True:
        if next_token:
            params["nextToken"] = next_token

        try:
            response = execute_api_request(endpoint, access_token, params, configuration)

            payroll_records = response.get("payrollRuns", [])
            if not payroll_records:
                break

            # Yield individual records instead of accumulating
            for record in payroll_records:
                yield __map_payroll_data(record, company_id)

            # Check for pagination
            next_token = response.get("nextToken")
            if not next_token:
                break

        except Exception as e:
            # Payroll endpoint might not be available for all clients
            log.warning(f"Payroll data not available: {str(e)}")
            break


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    Args:
        configuration: A dictionary that holds the configuration settings for the connector.
    """
    return [
        {"table": "employees", "primary_key": ["company_id", "employee_id"]},
        {"table": "payroll", "primary_key": ["company_id", "employee_id", "pay_period_start"]},
    ]


def update(configuration: dict, state: dict):
    """
    Define the update function, which is a required function, and is called by Fivetran during each sync.
    See the technical reference documentation for more details on the update function
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    Args:
        configuration: A dictionary containing connection details.
        state: A dictionary containing state information from previous runs.
        The state dictionary is empty for the first sync or for any full re-sync
    """
    log.info("Starting Paylocity connector sync")

    # Extract configuration parameters
    client_id = str(configuration.get("client_id", ""))
    client_secret = str(configuration.get("client_secret", ""))
    company_id = str(configuration.get("company_id", ""))
    is_sandbox = __get_config_bool(configuration, "use_sandbox", True)
    enable_payroll = __get_config_bool(configuration, "enable_payroll", False)

    # Get state for incremental sync
    last_sync_time = state.get("last_sync_time")

    try:
        # Get access token
        log.info("Obtaining access token...")
        access_token = get_access_token(client_id, client_secret, is_sandbox, configuration)

        # Fetch employee data using generator (NO MEMORY ACCUMULATION)
        log.info("Fetching employee data...")
        employee_count = 0
        records_processed_in_batch = 0
        batch_size = __get_config_int(configuration, "max_records_per_page", 20)

        for employee_record in get_employees(
            access_token, company_id, last_sync_time, configuration
        ):
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into.
            # - The second argument is a dictionary containing the data to be upserted,
            op.upsert(table="employees", data=employee_record)
            employee_count += 1
            records_processed_in_batch += 1

            # Checkpoint after each batch/page to save progress incrementally
            if records_processed_in_batch >= batch_size:
                current_state = {
                    "last_sync_time": datetime.now(timezone.utc).isoformat(),
                    "employees_processed": employee_count,
                }
                op.checkpoint(current_state)
                log.info(f"Checkpointed after processing {employee_count} employee records")
                records_processed_in_batch = 0

        log.info(f"Processed {employee_count} employee records")

        # Fetch payroll data if enabled
        if enable_payroll:
            log.info("Fetching payroll data...")
            payroll_count = 0
            payroll_records_processed_in_batch = 0

            for payroll_record in get_payroll_data(
                access_token, company_id, last_sync_time, configuration
            ):
                # The 'upsert' operation is used to insert or update data in the destination table.
                # The op.upsert method is called with two arguments:
                # - The first argument is the name of the table to upsert the data into.
                # - The second argument is a dictionary containing the data to be upserted,
                op.upsert(table="payroll", data=payroll_record)
                payroll_count += 1
                payroll_records_processed_in_batch += 1

                # Checkpoint after each batch/page to save progress incrementally
                if payroll_records_processed_in_batch >= batch_size:
                    current_state = {
                        "last_sync_time": datetime.now(timezone.utc).isoformat(),
                        "employees_processed": employee_count,
                        "payroll_processed": payroll_count,
                    }
                    op.checkpoint(current_state)
                    log.info(f"Checkpointed after processing {payroll_count} payroll records")
                    payroll_records_processed_in_batch = 0

            log.info(f"Processed {payroll_count} payroll records")

        # Update state and checkpoint
        new_state = {"last_sync_time": datetime.now(timezone.utc).isoformat()}

        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        op.checkpoint(new_state)

        log.info("Paylocity sync completed successfully")

    except Exception as e:
        log.severe(f"Paylocity sync failed: {str(e)}")
        raise RuntimeError(f"Failed to sync Paylocity data: {str(e)}")


# Main Connector instance
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
