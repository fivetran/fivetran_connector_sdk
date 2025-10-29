"""Gusto API Connector
This connector demonstrates how to fetch data from Gusto API and upsert it into destination using the Fivetran Connector SDK.
Supports querying data from Companies, Employees, Payrolls, Benefits, Time Off, and Banking APIs.
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

# For reading configuration from a JSON file
import json

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# Import required libraries for API interactions
import requests
import time
import random
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional

__GUSTO_API_ENDPOINT = "https://api.gusto-demo.com/v1"


def __get_config_int(
    configuration: Optional[Dict[str, Any]],
    key: str,
    default: int,
    min_val: int = None,
    max_val: int = None,
) -> int:
    """
    Extract and validate integer configuration value.
    Args:
        configuration: Configuration dictionary
        key: Configuration key
        default: Default value
        min_val: Minimum allowed value
        max_val: Maximum allowed value
    Returns:
        Validated integer value
    """
    if not configuration:
        return default

    try:
        value = int(str(configuration.get(key, str(default))))
        if min_val is not None and value < min_val:
            return default
        if max_val is not None and value > max_val:
            return default
        return value
    except (ValueError, TypeError):
        return default


def __get_config_bool(
    configuration: Optional[Dict[str, Any]], key: str, default: bool = True
) -> bool:
    """
    Extract and validate boolean configuration value.
    Args:
        configuration: Configuration dictionary
        key: Configuration key
        default: Default boolean value
    Returns:
        Validated boolean value
    """
    if not configuration:
        return default

    value = str(configuration.get(key, str(default))).lower()
    return value in ("true", "1", "yes", "on")


def __calculate_wait_time(
    attempt: int,
    response_headers: Dict[str, Any],
    base_delay: int = 1,
    max_delay: int = 60,
) -> float:
    """
    Calculate wait time for rate limiting and retries with exponential backoff and jitter.
    Args:
        attempt: Current attempt number
        response_headers: Response headers dictionary
        base_delay: Base delay in seconds
        max_delay: Maximum delay in seconds
    Returns:
        Wait time in seconds with jitter
    """
    # Check for Retry-After header
    retry_after = response_headers.get("Retry-After")
    if retry_after:
        try:
            return int(retry_after)
        except ValueError:
            pass

    # Calculate exponential backoff with jitter
    wait_time = min(base_delay * (2**attempt), max_delay)
    jitter = random.uniform(0, min(wait_time * 0.1, 5))
    return wait_time + jitter


def __handle_rate_limit(attempt: int, response: requests.Response) -> None:
    """
    Handle HTTP 429 rate limiting with intelligent retry.
    Args:
        attempt: Current attempt number
        response: HTTP response object
    """
    wait_time = __calculate_wait_time(attempt, response.headers)
    log.info(f"Rate limited (HTTP 429) on attempt {attempt + 1}, waiting {wait_time:.2f} seconds")
    time.sleep(wait_time)


def __handle_request_error(
    attempt: int, retry_attempts: int, error: Exception, endpoint: str
) -> None:
    """
    Handle request errors with exponential backoff retry logic.
    Args:
        attempt: Current attempt number
        retry_attempts: Total retry attempts
        error: Exception that occurred
        endpoint: API endpoint being called
    Raises:
        RuntimeError: If this is the final attempt
    """
    if attempt == retry_attempts - 1:
        log.severe(
            f"Failed to execute API request to {endpoint} after {retry_attempts} attempts: {str(error)}"
        )
        raise RuntimeError(f"API request failed after {retry_attempts} attempts: {str(error)}")

    wait_time = __calculate_wait_time(attempt, {})
    log.info(f"Attempt {attempt + 1} failed, retrying in {wait_time:.2f} seconds: {str(error)}")
    time.sleep(wait_time)


def execute_api_request(
    endpoint: str,
    api_token: str,
    params: Optional[Dict[str, Any]] = None,
    configuration: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Execute an API request against Gusto API with robust error handling.
    Args:
        endpoint: The API endpoint to call
        api_token: Gusto API token
        params: Optional query parameters
        configuration: Optional configuration dictionary for timeout and retry settings
    Returns:
        The response data from the API
    """
    url = f"{__GUSTO_API_ENDPOINT}{endpoint}"
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

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

    raise RuntimeError("Unexpected error in API request execution")


def get_time_range(
    last_sync_time: Optional[str] = None, configuration: Optional[Dict[str, Any]] = None
) -> Dict[str, str]:
    """
    Generate dynamic time range for API queries.
    Args:
        last_sync_time: Last sync timestamp for incremental sync
        configuration: Optional configuration dictionary for initial sync days
    Returns:
        Dictionary with start and end time parameters
    """
    end_time = datetime.now(timezone.utc).isoformat()

    if last_sync_time:
        start_time = last_sync_time
    else:
        initial_sync_days = __get_config_int(configuration, "initial_sync_days", 90)
        start_time = (datetime.now(timezone.utc) - timedelta(days=initial_sync_days)).isoformat()

    return {"start": start_time, "end": end_time}


def __map_company_data(company: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map API response fields to database schema for companies.
    Args:
        company: Company data from API
    Returns:
        Mapped company data
    """
    return {
        "company_id": company.get("id", ""),
        "name": company.get("name", ""),
        "trade_name": company.get("trade_name", ""),
        "ein": company.get("ein", ""),
        "entity_type": company.get("entity_type", ""),
        "company_status": company.get("company_status", ""),
        "locations_count": len(company.get("locations", [])),
        "primary_signatory_first_name": company.get("primary_signatory", {}).get("first_name", ""),
        "primary_signatory_last_name": company.get("primary_signatory", {}).get("last_name", ""),
        "primary_signatory_email": company.get("primary_signatory", {}).get("email", ""),
        "primary_payroll_admin_first_name": company.get("primary_payroll_admin", {}).get(
            "first_name", ""
        ),
        "primary_payroll_admin_last_name": company.get("primary_payroll_admin", {}).get(
            "last_name", ""
        ),
        "primary_payroll_admin_email": company.get("primary_payroll_admin", {}).get("email", ""),
        "created_at": company.get("created_at", ""),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def get_company_data(
    api_token: str, company_id: str, configuration: Optional[Dict[str, Any]] = None
):
    """
    Fetch company data from Gusto API.
    Args:
        api_token: Gusto API token
        company_id: Company ID
        configuration: Optional configuration dictionary
    Yields:
        Company record
    """
    endpoint = f"/companies/{company_id}"
    response = execute_api_request(endpoint, api_token, configuration=configuration)

    if response:
        yield __map_company_data(response)


def __map_employee_data(employee: Dict[str, Any], company_id: str) -> Dict[str, Any]:
    """
    Map API response fields to database schema for employees.
    Args:
        employee: Employee data from API
        company_id: Company ID
    Returns:
        Mapped employee data
    """
    return {
        "employee_id": employee.get("id", ""),
        "company_id": company_id,
        "first_name": employee.get("first_name", ""),
        "middle_initial": employee.get("middle_initial", ""),
        "last_name": employee.get("last_name", ""),
        "email": employee.get("email", ""),
        "ssn": (
            employee.get("ssn", "")[:4] if employee.get("ssn") else ""
        ),  # Only last 4 for privacy
        "date_of_birth": employee.get("date_of_birth", ""),
        "phone": employee.get("phone", ""),
        "employee_number": employee.get("employee_number", ""),
        "department": employee.get("department", ""),
        "manager_id": employee.get("manager", {}).get("id", "") if employee.get("manager") else "",
        "location_id": employee.get("home_address", {}).get("id", ""),
        "work_location_id": employee.get("work_location", {}).get("id", ""),
        "job_title": (
            employee.get("jobs", [{}])[0].get("title", "") if employee.get("jobs") else ""
        ),
        "hire_date": (
            employee.get("jobs", [{}])[0].get("hire_date", "") if employee.get("jobs") else ""
        ),
        "termination_date": employee.get("termination_date", ""),
        "employment_status": employee.get("employment_status", ""),
        "pay_rate": (
            str(employee.get("jobs", [{}])[0].get("rate", "")) if employee.get("jobs") else ""
        ),
        "pay_period": (
            employee.get("jobs", [{}])[0].get("payment_unit", "") if employee.get("jobs") else ""
        ),
        "two_percent_shareholder": employee.get("two_percent_shareholder", False),
        "has_ssn_on_file": bool(employee.get("ssn")),
        "created_at": employee.get("created_at", ""),
        "updated_at": employee.get("updated_at", ""),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def get_employees_data(
    api_token: str,
    company_id: str,
    last_sync_time: Optional[str] = None,
    configuration: Optional[Dict[str, Any]] = None,
):
    """
    Fetch employees data from Gusto API using streaming approach.
    Args:
        api_token: Gusto API token
        company_id: Company ID
        last_sync_time: Last sync timestamp for incremental sync
        configuration: Optional configuration dictionary
    Yields:
        Individual employee records
    """
    endpoint = f"/companies/{company_id}/employees"
    max_records = __get_config_int(configuration, "max_records_per_page", 50)

    params = {"page": 1, "per": max_records}

    # Add time filtering if available
    time_range = get_time_range(last_sync_time, configuration)
    if last_sync_time:
        params["updated_since"] = time_range["start"]

    page = 1
    while True:
        params["page"] = page
        response = execute_api_request(endpoint, api_token, params, configuration)

        employees = response.get("employees", []) if isinstance(response, dict) else response or []
        if not employees:
            break

        for employee in employees:
            yield __map_employee_data(employee, company_id)

        if len(employees) < max_records:
            break
        page += 1


def __map_payroll_data(payroll: Dict[str, Any], company_id: str) -> Dict[str, Any]:
    """
    Map API response fields to database schema for payrolls.
    Args:
        payroll: Payroll data from API
        company_id: Company ID
    Returns:
        Mapped payroll data
    """
    return {
        "payroll_id": payroll.get("id", ""),
        "company_id": company_id,
        "pay_period_start_date": payroll.get("pay_period", {}).get("start_date", ""),
        "pay_period_end_date": payroll.get("pay_period", {}).get("end_date", ""),
        "check_date": payroll.get("check_date", ""),
        "processed": payroll.get("processed", False),
        "processed_date": payroll.get("processed_date", ""),
        "payroll_deadline": payroll.get("payroll_deadline", ""),
        "employees_count": len(payroll.get("employee_compensations", [])),
        "gross_pay": str(payroll.get("totals", {}).get("gross_pay", "")),
        "net_pay": str(payroll.get("totals", {}).get("net_pay", "")),
        "employer_taxes": str(payroll.get("totals", {}).get("employer_taxes", "")),
        "employee_taxes": str(payroll.get("totals", {}).get("employee_taxes", "")),
        "employee_deductions": str(payroll.get("totals", {}).get("employee_deductions", "")),
        "employer_contributions": str(payroll.get("totals", {}).get("employer_contributions", "")),
        "created_at": payroll.get("created_at", ""),
        "updated_at": payroll.get("updated_at", ""),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def get_payrolls_data(
    api_token: str,
    company_id: str,
    last_sync_time: Optional[str] = None,
    configuration: Optional[Dict[str, Any]] = None,
):
    """
    Fetch payrolls data from Gusto API.
    Args:
        api_token: Gusto API token
        company_id: Company ID
        last_sync_time: Last sync timestamp for incremental sync
        configuration: Optional configuration dictionary
    Yields:
        Individual payroll records
    """
    endpoint = f"/companies/{company_id}/payrolls"
    max_records = __get_config_int(configuration, "max_records_per_page", 50)

    params = {"page": 1, "per": max_records}

    # Add time filtering
    time_range = get_time_range(last_sync_time, configuration)
    params["start_date"] = time_range["start"][:10]  # YYYY-MM-DD format
    params["end_date"] = time_range["end"][:10]

    page = 1
    while True:
        params["page"] = page
        response = execute_api_request(endpoint, api_token, params, configuration)

        payrolls = response.get("payrolls", []) if isinstance(response, dict) else response or []
        if not payrolls:
            break

        for payroll in payrolls:
            yield __map_payroll_data(payroll, company_id)

        if len(payrolls) < max_records:
            break
        page += 1


def __map_pay_schedule_data(pay_schedule: Dict[str, Any], company_id: str) -> Dict[str, Any]:
    """
    Map API response fields to database schema for pay schedules.
    Args:
        pay_schedule: Pay schedule data from API
        company_id: Company ID
    Returns:
        Mapped pay schedule data
    """
    return {
        "pay_schedule_id": pay_schedule.get("uuid", ""),
        "company_id": company_id,
        "frequency": pay_schedule.get("frequency", ""),
        "name": pay_schedule.get("name", ""),
        "custom_name": pay_schedule.get("custom_name", ""),
        "anchor_pay_date": pay_schedule.get("anchor_pay_date", ""),
        "anchor_end_of_pay_period": pay_schedule.get("anchor_end_of_pay_period", ""),
        "day_1": pay_schedule.get("day_1", ""),
        "day_2": pay_schedule.get("day_2", ""),
        "auto_pilot": pay_schedule.get("auto_pilot", False),
        "active": pay_schedule.get("active", False),
        "version": pay_schedule.get("version", ""),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def get_pay_schedules_data(
    api_token: str,
    company_id: str,
    configuration: Optional[Dict[str, Any]] = None,
):
    """
    Fetch pay schedules data from Gusto API.
    Args:
        api_token: Gusto API token
        company_id: Company ID
        configuration: Optional configuration dictionary
    Yields:
        Individual pay schedule records
    """
    endpoint = f"/companies/{company_id}/pay_schedules"
    response = execute_api_request(endpoint, api_token, configuration=configuration)

    pay_schedules = (
        response.get("pay_schedules", []) if isinstance(response, dict) else response or []
    )
    for pay_schedule in pay_schedules:
        yield __map_pay_schedule_data(pay_schedule, company_id)


def __map_company_benefit_data(company_benefit: Dict[str, Any], company_id: str) -> Dict[str, Any]:
    """
    Map API response fields to database schema for company benefits.
    Args:
        company_benefit: Company benefit data from API
        company_id: Company ID
    Returns:
        Mapped company benefit data
    """
    return {
        "company_benefit_id": company_benefit.get("uuid", ""),
        "company_id": company_id,
        "version": company_benefit.get("version", ""),
        "benefit_type": company_benefit.get("benefit_type", ""),
        "description": company_benefit.get("description", ""),
        "active": company_benefit.get("active", False),
        "source": company_benefit.get("source", ""),
        "partner_name": company_benefit.get("partner_name", ""),
        "deletable": company_benefit.get("deletable", False),
        "supports_percentage_amounts": company_benefit.get("supports_percentage_amounts", False),
        "responsible_for_employer_taxes": company_benefit.get(
            "responsible_for_employer_taxes", False
        ),
        "responsible_for_employee_w2": company_benefit.get("responsible_for_employee_w2", False),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def get_company_benefits_data(
    api_token: str,
    company_id: str,
    configuration: Optional[Dict[str, Any]] = None,
):
    """
    Fetch company benefits data from Gusto API.
    Args:
        api_token: Gusto API token
        company_id: Company ID
        configuration: Optional configuration dictionary
    Yields:
        Individual company benefit records
    """
    endpoint = f"/companies/{company_id}/company_benefits"
    max_records = __get_config_int(configuration, "max_records_per_page", 50)

    params = {"page": 1, "per": max_records}

    page = 1
    while True:
        params["page"] = page
        response = execute_api_request(endpoint, api_token, params, configuration)

        company_benefits = (
            response.get("company_benefits", []) if isinstance(response, dict) else response or []
        )
        if not company_benefits:
            break

        for company_benefit in company_benefits:
            yield __map_company_benefit_data(company_benefit, company_id)

        if len(company_benefits) < max_records:
            break
        page += 1


def __map_location_data(location: Dict[str, Any], company_id: str) -> Dict[str, Any]:
    """
    Map API response fields to database schema for company locations.
    Args:
        location: Location data from API
        company_id: Company ID
    Returns:
        Mapped location data
    """
    return {
        "location_id": location.get("id", ""),
        "company_id": company_id,
        "street_1": location.get("street_1", ""),
        "street_2": location.get("street_2", ""),
        "city": location.get("city", ""),
        "state": location.get("state", ""),
        "zip": location.get("zip", ""),
        "country": location.get("country", ""),
        "phone_number": location.get("phone_number", ""),
        "active": location.get("active", True),
        "created_at": location.get("created_at", ""),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def get_locations_data(
    api_token: str, company_id: str, configuration: Optional[Dict[str, Any]] = None
):
    """
    Fetch company locations data from Gusto API.
    Args:
        api_token: Gusto API token
        company_id: Company ID
        configuration: Optional configuration dictionary
    Yields:
        Individual location records
    """
    endpoint = f"/companies/{company_id}/locations"
    response = execute_api_request(endpoint, api_token, configuration=configuration)

    locations = response.get("locations", []) if isinstance(response, dict) else response or []
    for location in locations:
        yield __map_location_data(location, company_id)


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    return [
        {"table": "company", "primary_key": ["company_id"]},
        {"table": "employee", "primary_key": ["employee_id"]},
        {"table": "payroll", "primary_key": ["payroll_id"]},
        {"table": "pay_schedule", "primary_key": ["pay_schedule_id"]},
        {"table": "company_benefit", "primary_key": ["company_benefit_id"]},
        {"table": "location", "primary_key": ["location_id"]},
    ]


def update(configuration: dict, state: dict):
    """
    Define the update function, which is called by Fivetran during each sync.
    Args:
        configuration: A dictionary containing connection details
        state: A dictionary containing state information from previous runs
    """
    log.warning("Starting Gusto API connector sync")

    # Extract configuration parameters
    api_token = str(configuration.get("api_token", ""))
    company_id = str(configuration.get("company_id", ""))

    # Get feature flags from configuration
    enable_employees = __get_config_bool(configuration, "enable_employees", True)
    enable_payrolls = __get_config_bool(configuration, "enable_payrolls", True)
    enable_pay_schedules = __get_config_bool(configuration, "enable_pay_schedules", True)
    enable_company_benefits = __get_config_bool(configuration, "enable_company_benefits", True)
    enable_locations = __get_config_bool(configuration, "enable_locations", True)
    enable_debug_logging = __get_config_bool(configuration, "enable_debug_logging", False)

    # Get the state variable for the sync
    last_sync_time = state.get("last_sync_time")

    # Log sync type
    if last_sync_time:
        log.info(f"Incremental sync: fetching data since {last_sync_time}")
    else:
        initial_days = str(configuration.get("initial_sync_days", "90"))
        log.info(f"Initial sync: fetching all available data (last {initial_days} days)")

    if enable_debug_logging:
        log.info(
            f"Configuration: employees={enable_employees}, payrolls={enable_payrolls}, "
            f"pay_schedules={enable_pay_schedules}, company_benefits={enable_company_benefits}, "
            f"locations={enable_locations}"
        )

    try:
        # Fetch company data
        log.info("Fetching company data...")
        for record in get_company_data(api_token, company_id, configuration):
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into.
            # - The second argument is a dictionary containing the data to be upserted,
            op.upsert(table="company", data=record)

        # Fetch locations data (if enabled)
        if enable_locations:
            log.info("Fetching locations data...")
            for record in get_locations_data(api_token, company_id, configuration):
                # The 'upsert' operation is used to insert or update data in the destination table.
                # The op.upsert method is called with two arguments:
                # - The first argument is the name of the table to upsert the data into.
                # - The second argument is a dictionary containing the data to be upserted,
                op.upsert(table="location", data=record)
        else:
            log.info("Locations data fetching disabled")

        # Fetch employees data (if enabled)
        if enable_employees:
            log.info("Fetching employees data...")
            for record in get_employees_data(api_token, company_id, last_sync_time, configuration):
                # The 'upsert' operation is used to insert or update data in the destination table.
                # The op.upsert method is called with two arguments:
                # - The first argument is the name of the table to upsert the data into.
                # - The second argument is a dictionary containing the data to be upserted,
                op.upsert(table="employee", data=record)
        else:
            log.info("Employees data fetching disabled")

        # Fetch payrolls data (if enabled)
        if enable_payrolls:
            log.info("Fetching payrolls data...")
            for record in get_payrolls_data(api_token, company_id, last_sync_time, configuration):
                # The 'upsert' operation is used to insert or update data in the destination table.
                # The op.upsert method is called with two arguments:
                # - The first argument is the name of the table to upsert the data into.
                # - The second argument is a dictionary containing the data to be upserted,
                op.upsert(table="payroll", data=record)
        else:
            log.info("Payrolls data fetching disabled")

        # Fetch pay schedules data (if enabled)
        if enable_pay_schedules:
            log.info("Fetching pay schedules data...")
            for record in get_pay_schedules_data(api_token, company_id, configuration):
                # The 'upsert' operation is used to insert or update data in the destination table.
                # The op.upsert method is called with two arguments:
                # - The first argument is the name of the table to upsert the data into.
                # - The second argument is a dictionary containing the data to be upserted,
                op.upsert(table="pay_schedule", data=record)
        else:
            log.info("Pay schedules data fetching disabled")

        # Fetch company benefits data (if enabled)
        if enable_company_benefits:
            log.info("Fetching company benefits data...")
            for record in get_company_benefits_data(api_token, company_id, configuration):
                # The 'upsert' operation is used to insert or update data in the destination table.
                # The op.upsert method is called with two arguments:
                # - The first argument is the name of the table to upsert the data into.
                # - The second argument is a dictionary containing the data to be upserted,
                op.upsert(table="company_benefit", data=record)
        else:
            log.info("Company benefits data fetching disabled")

        # Update state with the current sync time
        new_state = {"last_sync_time": datetime.now(timezone.utc).isoformat()}

        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        op.checkpoint(new_state)

        log.info("Gusto API connector sync completed successfully")

    except Exception as e:
        log.severe(f"Failed to sync Gusto data: {str(e)}")
        raise RuntimeError(f"Failed to sync data: {str(e)}")


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
