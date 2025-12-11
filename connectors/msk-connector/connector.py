"""Workday HCM API Connector for Fivetran.

This connector demonstrates how to fetch position data from Workday HCM API using SOAP
and upsert it into destination using the Fivetran Connector SDK with proper error handling,
pagination, and state management.
See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional
from xml.etree import ElementTree as ET
from xml.etree.ElementTree import Element
from calendar import monthrange

import requests
from requests.auth import HTTPBasicAuth

# For reading configuration from a JSON file
import json

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# Constants
_CHECKPOINT_INTERVAL = 100  # Checkpoint every 100 records
_MAX_RETRIES = 3
_RETRY_DELAY = 1  # Initial retry delay in seconds
_PAGE_SIZE = 100  # Number of records per page
_WORKDAY_NAMESPACE = "urn:com.workday/bsvc"
_SOAP_ENVELOPE_NS = "http://schemas.xmlsoap.org/soap/envelope/"
_WORKDAY_API_VERSION = "v44.1"


def validate_configuration(configuration: dict) -> None:
    """Validate the configuration dictionary to ensure it contains all required parameters.

    This function is called at the start of the update method to ensure that the connector
    has all necessary configuration values.

    Args:
        configuration: a dictionary that holds the configuration settings for the connector.

    Raises:
        ValueError: if any required configuration parameter is missing.
    """
    required_configs = ["workday_url", "username", "password", "tenant"]
    for key in required_configs:
        if key not in configuration:
            raise ValueError(f"Missing required configuration value: {key}")

    # Validate workday_url format (allow http:// for local testing)
    workday_url = configuration["workday_url"]
    if not (workday_url.startswith("https://") or workday_url.startswith("http://")):
        raise ValueError("workday_url must start with https:// or http://")

    # Validate effective_start_date if provided
    if "effective_start_date" in configuration:
        try:
            datetime.strptime(configuration["effective_start_date"], "%Y-%m-%d")
        except ValueError:
            raise ValueError("effective_start_date must be in YYYY-MM-DD format")


def _get_last_day_of_month(year: int, month: int) -> datetime:
    """Get the last day of a given month.

    Args:
        year: Year (e.g., 2024)
        month: Month (1-12)

    Returns:
        datetime object representing the last day of the month
    """
    last_day = monthrange(year, month)[1]
    return datetime(year, month, last_day, tzinfo=timezone.utc)


def _generate_monthly_dates(start_date: datetime, end_date: datetime) -> List[str]:
    """Generate a list of dates representing the last day of each month between start and end dates.

    Args:
        start_date: Starting date (inclusive)
        end_date: Ending date (inclusive)

    Returns:
        List of date strings in YYYY-MM-DD format (last day of each month)
    """
    dates = []
    current = start_date.replace(day=1)  # Start from first day of start month

    while current <= end_date:
        # Get last day of current month
        last_day = _get_last_day_of_month(current.year, current.month)

        # Only include if it's within our date range
        if last_day >= start_date and last_day <= end_date:
            dates.append(last_day.strftime("%Y-%m-%d"))

        # Move to first day of next month
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1, day=1)
        else:
            current = current.replace(month=current.month + 1, day=1)

    return dates


def _generate_daily_dates(start_date: datetime, end_date: datetime) -> List[str]:
    """Generate a list of dates representing each day between start and end dates.

    Args:
        start_date: Starting date (inclusive)
        end_date: Ending date (inclusive)

    Returns:
        List of date strings in YYYY-MM-DD format (one per day)
    """
    dates = []
    current = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
    end = end_date.replace(hour=0, minute=0, second=0, microsecond=0)

    while current <= end:
        dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)

    return dates


def make_soap_request(
    url: str,
    soap_body: str,
    username: str,
    password: str,
) -> str:
    """Make a SOAP POST request to Workday API with retry logic and exponential backoff.

    Args:
        url: The Workday API endpoint URL
        soap_body: The SOAP envelope XML body
        username: Workday username for authentication
        password: Workday password for authentication
        tenant: Workday tenant name

    Returns:
        str: XML response from the SOAP service

    Raises:
        RuntimeError: If all retry attempts fail
    """
    headers = {
        "Content-Type": "text/xml; charset=utf-8",
        "SOAPAction": "",
        "User-Agent": "Fivetran-Workday-Connector/1.0",
    }

    # Workday uses HTTP Basic Authentication
    auth = HTTPBasicAuth(username, password)

    for attempt in range(_MAX_RETRIES):
        try:
            response = requests.post(url, headers=headers, data=soap_body, auth=auth, timeout=60)
            response.raise_for_status()
            return response.text

        except requests.exceptions.RequestException as e:
            if attempt == _MAX_RETRIES - 1:
                raise RuntimeError(
                    f"Workday SOAP request failed after {_MAX_RETRIES} attempts: {str(e)}"
                )

            # Exponential backoff
            delay = _RETRY_DELAY * (2**attempt)
            log.warning(
                f"Workday SOAP request attempt {attempt + 1} failed, retrying in {delay}s: {str(e)}"
            )
            time.sleep(delay)


def _get_workday_api_url(workday_url: str, tenant: str) -> str:
    """Get the Workday API URL for Human Capital Management.

    Args:
        workday_url: Base Workday URL (e.g., https://wd2-impl.workday.com)
        tenant: Workday tenant name

    Returns:
        The full API URL for HCM operations
    """
    # Workday API endpoint format: https://{workday_url}/ccx/service/{tenant}/Human_Resources/v{version}
    return f"{workday_url}/ccx/service/{tenant}/Human_Resources/{_WORKDAY_API_VERSION}"


def _create_get_positions_soap_envelope(
    page: int = 1,
    count: int = _PAGE_SIZE,
    effective_date: Optional[str] = None,
    position_ids: Optional[List[str]] = None,
    include_reference: bool = True,
    include_position_definition: bool = True,
    include_compensation: bool = True,
    include_organization_assignment: bool = True,
    include_worker_data: bool = True,
) -> str:
    """Create a SOAP envelope for Get_Positions_Request.

    Args:
        page: Page number for pagination
        count: Number of records per page
        effective_date: Effective date for the query (YYYY-MM-DD format)
        position_ids: Optional list of specific position IDs to query
        include_reference: Include reference data
        include_position_definition: Include position definition data
        include_compensation: Include compensation data
        include_organization_assignment: Include organization assignment data
        include_worker_data: Include worker data for filled positions

    Returns:
        str: SOAP envelope XML string
    """
    # Build request references if position IDs are provided
    request_references = ""
    if position_ids:
        refs = "".join(
            [
                f'<wd:Positions_Reference><wd:ID wd:type="Position_ID">{pid}</wd:ID></wd:Positions_Reference>'
                for pid in position_ids
            ]
        )
        request_references = f"""<wd:Request_References
               wd:Skip_Non_Existing_Instances="true"
               wd:Ignore_Invalid_References="true">
               {refs}
           </wd:Request_References>"""

    # Build response filter
    effective_date_str = (
        f"<wd:As_Of_Effective_Date>{effective_date}</wd:As_Of_Effective_Date>"
        if effective_date
        else ""
    )
    response_filter = f"""<wd:Response_Filter>
               {effective_date_str}
               <wd:Page>{page}</wd:Page>
               <wd:Count>{count}</wd:Count>
           </wd:Response_Filter>"""

    # Build response group
    response_group = f"""<wd:Response_Group>
               <wd:Include_Reference>{str(include_reference).lower()}</wd:Include_Reference>
               <wd:Include_Position_Definition_Data>{str(include_position_definition).lower()}</wd:Include_Position_Definition_Data>
               <wd:Include_Position_Restrictions_Data>true</wd:Include_Position_Restrictions_Data>
               <wd:Include_Position_Restriction_Working_Hours_Details_Data>true</wd:Include_Position_Restriction_Working_Hours_Details_Data>
               <wd:Include_Default_Compensation_Data>{str(include_compensation).lower()}</wd:Include_Default_Compensation_Data>
               <wd:Include_Default_Position_Organization_Assignment_Data>{str(include_organization_assignment).lower()}</wd:Include_Default_Position_Organization_Assignment_Data>
               <wd:Include_Worker_For_Filled_Positions_Data>{str(include_worker_data).lower()}</wd:Include_Worker_For_Filled_Positions_Data>
               <wd:Include_Qualifications>true</wd:Include_Qualifications>
               <wd:Include_Job_Requisition_Data>true</wd:Include_Job_Requisition_Data>
               <wd:Include_Job_Requisition_Attachments>true</wd:Include_Job_Requisition_Attachments>
           </wd:Response_Group>"""

    envelope = f"""<?xml version="1.0" encoding="UTF-8"?>
<env:Envelope
   xmlns:env="{_SOAP_ENVELOPE_NS}"
   xmlns:xsd="http://www.w3.org/2001/XMLSchema">
   <env:Body>
       <wd:Get_Positions_Request xmlns:wd="{_WORKDAY_NAMESPACE}" wd:version="{_WORKDAY_API_VERSION}">
           {request_references}
           {response_filter}
           {response_group}
       </wd:Get_Positions_Request>
   </env:Body>
</env:Envelope>"""
    return envelope


def _parse_soap_response(
    xml_response: str, effective_date: Optional[str] = None
) -> tuple[List[Dict], bool, int]:
    """Parse Workday SOAP XML response and extract position data.

    Args:
        xml_response: The XML response string from Workday SOAP service
        effective_date: The effective date used in the query (YYYY-MM-DD format)

    Returns:
        Tuple of (list of position records, has_more_pages, total_pages)
    """
    try:
        # Parse XML
        root = ET.fromstring(xml_response)

        # Register namespaces
        namespaces = {
            "env": _SOAP_ENVELOPE_NS,
            "wd": _WORKDAY_NAMESPACE,
        }

        # Check for SOAP faults
        fault = root.find(".//env:Fault", namespaces)
        if fault is not None:
            fault_string = root.find(".//env:faultstring", namespaces)
            error_msg = fault_string.text if fault_string is not None else "Unknown SOAP fault"
            raise RuntimeError(f"Workday SOAP fault: {error_msg}")

        # Find Position elements (which contain Position_Reference and Position_Data)
        positions = root.findall(".//wd:Position", namespaces)

        records = []
        for position in positions:
            record = _extract_position_data(position, namespaces, effective_date)
            if record:
                records.append(record)

        # Check pagination
        response_results = root.find(".//wd:Response_Results", namespaces)
        has_more_pages = False
        total_pages = 1

        if response_results is not None:
            total_pages_elem = response_results.find("wd:Total_Pages", namespaces)
            page_elem = response_results.find("wd:Page", namespaces)

            if total_pages_elem is not None and page_elem is not None:
                try:
                    total_pages = int(total_pages_elem.text)
                    current_page = int(page_elem.text)
                    has_more_pages = current_page < total_pages
                except (ValueError, AttributeError):
                    pass

        return records, has_more_pages, total_pages

    except ET.ParseError as e:
        log.warning(f"Failed to parse Workday SOAP response: {str(e)}")
        raise RuntimeError(f"Workday SOAP response parsing failed: {str(e)}")


def _extract_position_data(
    position_element: Element, namespaces: dict, effective_date: Optional[str] = None
) -> Optional[Dict]:
    """Extract position data from a Position XML element.

    Args:
        position_element: XML element containing Position (with Position_Reference and Position_Data)
        namespaces: XML namespace mapping
        effective_date: The effective date used in the query (YYYY-MM-DD format). This will be set in the record.

    Returns:
        Dictionary containing position record data
    """
    try:
        record = {
            "sync_timestamp": datetime.now(timezone.utc).isoformat(),
        }

        # Always set the effective_date from the query parameter
        # This ensures each month's data is uniquely identified
        if effective_date:
            record["effective_date"] = effective_date

        # Extract Position Reference (from Position element, not Position_Data)
        position_ref = position_element.find("wd:Position_Reference", namespaces)
        if position_ref is not None:
            # Find all ID elements and extract both WID and Position_ID
            id_elems = position_ref.findall("wd:ID", namespaces)
            for id_elem in id_elems:
                id_type = id_elem.get("{urn:com.workday/bsvc}type") or id_elem.get("type")
                if id_type == "Position_ID":
                    record["position_id"] = id_elem.text
                elif id_type == "WID":
                    record["position_wid"] = id_elem.text

        # Extract Position_Data element
        position_data = position_element.find("wd:Position_Data", namespaces)
        if position_data is None:
            # Ensure we have at least a position_id
            if "position_id" not in record:
                return None
            return record

        # Extract Supervisory Organization Reference
        supervisory_org = position_data.find("wd:Supervisory_Organization_Reference", namespaces)
        if supervisory_org is not None:
            org_id_elems = supervisory_org.findall("wd:ID", namespaces)
            for org_id_elem in org_id_elems:
                id_type = org_id_elem.get("{urn:com.workday/bsvc}type") or org_id_elem.get("type")
                if id_type == "Organization_Reference_ID":
                    record["supervisory_organization_id"] = org_id_elem.text
                elif id_type == "WID":
                    record["supervisory_organization_wid"] = org_id_elem.text

        # Note: We use the effective_date from the query parameter (already set above)
        # The Effective_Date in the XML response may differ, so we prioritize the query parameter
        # to ensure consistent monthly iteration tracking

        # Extract Position Definition Data
        position_def = position_data.find("wd:Position_Definition_Data", namespaces)
        if position_def is not None:
            # Position_ID from Position_Definition_Data
            pos_id = position_def.find("wd:Position_ID", namespaces)
            if pos_id is not None and pos_id.text:
                record["position_id"] = pos_id.text

            # Job Posting Title (not Position_Title)
            job_posting_title = position_def.find("wd:Job_Posting_Title", namespaces)
            if job_posting_title is not None and job_posting_title.text:
                record["job_posting_title"] = job_posting_title.text

            # Academic Tenure Eligible
            academic_tenure = position_def.find("wd:Academic_Tenure_Eligible", namespaces)
            if academic_tenure is not None and academic_tenure.text:
                record["academic_tenure_eligible"] = academic_tenure.text

            # Job Description Summary
            job_desc_summary = position_def.find("wd:Job_Description_Summary", namespaces)
            if job_desc_summary is not None and job_desc_summary.text:
                record["job_description_summary"] = job_desc_summary.text

            # Job Description
            job_desc = position_def.find("wd:Job_Description", namespaces)
            if job_desc is not None and job_desc.text:
                record["job_description"] = job_desc.text

            # Position Status References (multiple)
            status_refs = position_def.findall("wd:Position_Status_Reference", namespaces)
            if status_refs:
                status_wids = []
                for status_ref in status_refs:
                    status_id = status_ref.find("wd:ID", namespaces)
                    if status_id is not None:
                        id_type = status_id.get("{urn:com.workday/bsvc}type") or status_id.get(
                            "type"
                        )
                        if id_type == "WID":
                            status_wids.append(status_id.text)
                if status_wids:
                    record["position_status_wids"] = ",".join(status_wids)

            # Various flags
            flags = [
                "Available_For_Hire",
                "Available_for_Recruiting",
                "Hiring_Freeze",
                "Work_Shift_Required",
                "Available_for_Overlap",
                "Critical_Job",
            ]
            for flag in flags:
                flag_elem = position_def.find(f"wd:{flag}", namespaces)
                if flag_elem is not None and flag_elem.text:
                    record[flag.lower()] = flag_elem.text

        # Extract Position Restrictions Data
        restrictions = position_data.find("wd:Position_Restrictions_Data", namespaces)
        if restrictions is not None:
            # Availability Date
            avail_date = restrictions.find("wd:Availability_Date", namespaces)
            if avail_date is not None and avail_date.text:
                record["availability_date"] = avail_date.text

            # Earliest Hire Date
            earliest_hire = restrictions.find("wd:Earliest_Hire_Date", namespaces)
            if earliest_hire is not None and earliest_hire.text:
                record["earliest_hire_date"] = earliest_hire.text

            # Job Profile Restriction Summary Data
            job_profile_summary = restrictions.find(
                "wd:Job_Profile_Restriction_Summary_Data", namespaces
            )
            if job_profile_summary is not None:
                job_profile_ref = job_profile_summary.find("wd:Job_Profile_Reference", namespaces)
                if job_profile_ref is not None:
                    job_profile_ids = job_profile_ref.findall("wd:ID", namespaces)
                    for job_profile_id in job_profile_ids:
                        id_type = job_profile_id.get(
                            "{urn:com.workday/bsvc}type"
                        ) or job_profile_id.get("type")
                        if id_type == "Job_Profile_ID":
                            record["job_profile_id"] = job_profile_id.text
                        elif id_type == "WID":
                            record["job_profile_wid"] = job_profile_id.text

                job_profile_name = job_profile_summary.find("wd:Job_Profile_Name", namespaces)
                if job_profile_name is not None and job_profile_name.text:
                    record["job_profile_name"] = job_profile_name.text

                # Management Level Reference
                management_level_ref = job_profile_summary.find(
                    "wd:Management_Level_Reference", namespaces
                )
                if management_level_ref is not None:
                    management_level_ids = management_level_ref.findall("wd:ID", namespaces)
                    for mgmt_level_id in management_level_ids:
                        id_type = mgmt_level_id.get(
                            "{urn:com.workday/bsvc}type"
                        ) or mgmt_level_id.get("type")
                        if id_type == "Management_Level_ID":
                            record["management_level_id"] = mgmt_level_id.text
                        elif id_type == "WID":
                            record["management_level_wid"] = mgmt_level_id.text

                # Job Family Reference
                job_family_ref = job_profile_summary.find("wd:Job_Family_Reference", namespaces)
                if job_family_ref is not None:
                    job_family_ids = job_family_ref.findall("wd:ID", namespaces)
                    for job_family_id in job_family_ids:
                        id_type = job_family_id.get(
                            "{urn:com.workday/bsvc}type"
                        ) or job_family_id.get("type")
                        if id_type == "Job_Family_ID":
                            record["job_family_id"] = job_family_id.text
                        elif id_type == "WID":
                            record["job_family_wid"] = job_family_id.text

                # Work Shift Required (from Job Profile Restriction Summary)
                work_shift_required = job_profile_summary.find(
                    "wd:Work_Shift_Required", namespaces
                )
                if work_shift_required is not None and work_shift_required.text:
                    record["job_profile_work_shift_required"] = work_shift_required.text

                # Job Profile Exempt Data
                job_profile_exempt = job_profile_summary.find(
                    "wd:Job_Profile_Exempt_Data", namespaces
                )
                if job_profile_exempt is not None:
                    # Location Context Reference
                    location_context_ref = job_profile_exempt.find(
                        "wd:Location_Context_Reference", namespaces
                    )
                    if location_context_ref is not None:
                        location_context_ids = location_context_ref.findall("wd:ID", namespaces)
                        for loc_ctx_id in location_context_ids:
                            id_type = loc_ctx_id.get(
                                "{urn:com.workday/bsvc}type"
                            ) or loc_ctx_id.get("type")
                            if id_type == "ISO_3166-1_Alpha-2_Code":
                                record["location_context_country_code"] = loc_ctx_id.text
                            elif id_type == "ISO_3166-1_Alpha-3_Code":
                                record["location_context_country_code_alpha3"] = loc_ctx_id.text
                            elif id_type == "OPM_Country_Code":
                                record["location_context_opm_country_code"] = loc_ctx_id.text
                            elif id_type == "ISO_3166-1_Numeric-3_Code":
                                record["location_context_country_code_numeric"] = loc_ctx_id.text
                            elif id_type == "WID":
                                record["location_context_wid"] = loc_ctx_id.text

                    # Job Exempt flag
                    job_exempt = job_profile_exempt.find("wd:Job_Exempt", namespaces)
                    if job_exempt is not None and job_exempt.text:
                        record["job_exempt"] = job_exempt.text

                # Critical Job flag (from Job Profile Restriction Summary)
                critical_job = job_profile_summary.find("wd:Critical_Job", namespaces)
                if critical_job is not None and critical_job.text:
                    record["job_profile_critical_job"] = critical_job.text

            # Location Reference
            location_ref = restrictions.find("wd:Location_Reference", namespaces)
            if location_ref is not None:
                location_ids = location_ref.findall("wd:ID", namespaces)
                for location_id in location_ids:
                    id_type = location_id.get("{urn:com.workday/bsvc}type") or location_id.get(
                        "type"
                    )
                    if id_type == "Location_ID":
                        record["location_id"] = location_id.text
                    elif id_type == "WID":
                        record["location_wid"] = location_id.text

            # Worker Type Reference
            worker_type_ref = restrictions.find("wd:Worker_Type_Reference", namespaces)
            if worker_type_ref is not None:
                worker_type_ids = worker_type_ref.findall("wd:ID", namespaces)
                for worker_type_id in worker_type_ids:
                    id_type = worker_type_id.get(
                        "{urn:com.workday/bsvc}type"
                    ) or worker_type_id.get("type")
                    if id_type == "Worker_Type_ID":
                        record["worker_type_id"] = worker_type_id.text
                    elif id_type == "WID":
                        record["worker_type_wid"] = worker_type_id.text

            # Time Type Reference
            time_type_ref = restrictions.find("wd:Time_Type_Reference", namespaces)
            if time_type_ref is not None:
                time_type_ids = time_type_ref.findall("wd:ID", namespaces)
                for time_type_id in time_type_ids:
                    id_type = time_type_id.get("{urn:com.workday/bsvc}type") or time_type_id.get(
                        "type"
                    )
                    if id_type == "Position_Time_Type_ID":
                        record["time_type_id"] = time_type_id.text
                    elif id_type == "WID":
                        record["time_type_wid"] = time_type_id.text

            # Position Worker Type Reference
            pos_worker_type_ref = restrictions.find(
                "wd:Position_Worker_Type_Reference", namespaces
            )
            if pos_worker_type_ref is not None:
                pos_worker_type_ids = pos_worker_type_ref.findall("wd:ID", namespaces)
                for pos_worker_type_id in pos_worker_type_ids:
                    id_type = pos_worker_type_id.get(
                        "{urn:com.workday/bsvc}type"
                    ) or pos_worker_type_id.get("type")
                    if id_type == "Employee_Type_ID":
                        record["employee_type_id"] = pos_worker_type_id.text
                    elif id_type == "WID":
                        record["employee_type_wid"] = pos_worker_type_id.text

        # Extract Position Restriction Working Hours Details Data
        working_hours = position_data.find(
            "wd:Position_Restriction_Working_Hours_Details_Data", namespaces
        )
        if working_hours is not None:
            default_hours = working_hours.find("wd:Default_Hours", namespaces)
            if default_hours is not None and default_hours.text:
                record["default_hours"] = default_hours.text

            scheduled_hours = working_hours.find("wd:Scheduled_Hours", namespaces)
            if scheduled_hours is not None and scheduled_hours.text:
                record["scheduled_hours"] = scheduled_hours.text

        # Extract Default Compensation Data
        compensation = position_data.find("wd:Default_Compensation_Data", namespaces)
        if compensation is not None:
            primary_comp_basis = compensation.find("wd:Primary_Compensation_Basis", namespaces)
            if primary_comp_basis is not None and primary_comp_basis.text:
                record["primary_compensation_basis"] = primary_comp_basis.text

        # Extract Default Position Organization Assignments Data (plural)
        org_assignments = position_data.find(
            "wd:Default_Position_Organization_Assignments_Data", namespaces
        )
        if org_assignments is not None:
            # Company Assignments Reference
            company_ref = org_assignments.find("wd:Company_Assignments_Reference", namespaces)
            if company_ref is not None:
                company_ids = company_ref.findall("wd:ID", namespaces)
                for company_id in company_ids:
                    id_type = company_id.get("{urn:com.workday/bsvc}type") or company_id.get(
                        "type"
                    )
                    if id_type == "Organization_Reference_ID":
                        record["company_organization_id"] = company_id.text
                    elif id_type == "Company_Reference_ID":
                        record["company_id"] = company_id.text
                    elif id_type == "WID":
                        record["company_wid"] = company_id.text

            # Cost Center Assignments Reference
            cost_center_ref = org_assignments.find(
                "wd:Cost_Center_Assignments_Reference", namespaces
            )
            if cost_center_ref is not None:
                cost_center_ids = cost_center_ref.findall("wd:ID", namespaces)
                for cost_center_id in cost_center_ids:
                    id_type = cost_center_id.get(
                        "{urn:com.workday/bsvc}type"
                    ) or cost_center_id.get("type")
                    if id_type == "Organization_Reference_ID":
                        record["cost_center_organization_id"] = cost_center_id.text
                    elif id_type == "Cost_Center_Reference_ID":
                        record["cost_center_id"] = cost_center_id.text
                    elif id_type == "WID":
                        record["cost_center_wid"] = cost_center_id.text

        # Extract Closed flag
        closed = position_data.find("wd:Closed", namespaces)
        if closed is not None and closed.text:
            record["closed"] = closed.text

        # Extract Worker Data (for filled positions) - this might be at Position level
        worker_data = position_element.find("wd:Worker_For_Filled_Positions_Data", namespaces)
        if worker_data is None:
            worker_data = position_data.find("wd:Worker_For_Filled_Positions_Data", namespaces)
        if worker_data is not None:
            worker_ref = worker_data.find("wd:Worker_Reference", namespaces)
            if worker_ref is not None:
                worker_ids = worker_ref.findall("wd:ID", namespaces)
                for worker_id in worker_ids:
                    id_type = worker_id.get("{urn:com.workday/bsvc}type") or worker_id.get("type")
                    if id_type == "WID":
                        record["worker_id"] = worker_id.text

        # Ensure we have at least a position_id
        if "position_id" not in record:
            return None

        return record

    except Exception as e:
        log.warning(f"Failed to extract position data: {str(e)}")
        return None


def get_positions_data(
    configuration: dict,
    last_sync_time: Optional[str] = None,
    position_ids: Optional[List[str]] = None,
) -> List[Dict]:
    """Fetch position data from Workday API with monthly iteration for initial sync and daily iteration for incremental sync.

    Args:
        configuration: Configuration dictionary containing API credentials and settings
        last_sync_time: ISO timestamp of the last successful sync for incremental updates
        position_ids: Optional list of specific position IDs to query

    Returns:
        List of position data records

    Raises:
        RuntimeError: If API requests fail
    """
    workday_url = configuration["workday_url"]
    username = configuration["username"]
    password = configuration["password"]
    tenant = configuration["tenant"]

    api_url = _get_workday_api_url(workday_url, tenant)

    # Determine start date and iteration mode
    is_incremental_sync = last_sync_time is not None

    if is_incremental_sync:
        # Incremental sync: start from last sync time, use daily iteration
        try:
            start_date = datetime.fromisoformat(last_sync_time.replace("Z", "+00:00"))
        except Exception:
            log.warning(
                f"Invalid last_sync_time format: {last_sync_time}, using effective_start_date"
            )
            start_date = None
            is_incremental_sync = False
    else:
        # Initial sync: use effective_start_date from configuration, use monthly iteration
        start_date = None

    # Get effective_start_date from configuration if start_date is not set
    if start_date is None:
        effective_start_date_str = configuration.get("effective_start_date")
        if not effective_start_date_str:
            raise ValueError("effective_start_date is required in configuration for initial sync")
        try:
            start_date = datetime.strptime(effective_start_date_str, "%Y-%m-%d").replace(
                tzinfo=timezone.utc
            )
        except ValueError:
            raise ValueError(
                f"Invalid effective_start_date format: {effective_start_date_str}. Expected YYYY-MM-DD"
            )

    # End date is current date
    end_date = datetime.now(timezone.utc)

    # Generate list of effective dates based on sync type
    if is_incremental_sync:
        # Incremental sync: use daily iteration
        effective_dates = _generate_daily_dates(start_date, end_date)
        iteration_type = "daily"
        date_description = "day"
    else:
        # Initial sync: use monthly iteration (last day of each month)
        effective_dates = _generate_monthly_dates(start_date, end_date)
        iteration_type = "monthly"
        date_description = "month"

    if not effective_dates:
        log.warning(
            f"No {iteration_type} dates to process between {start_date.date()} and {end_date.date()}"
        )
        return []

    log.fine(
        f"Processing {len(effective_dates)} {date_description}s from {effective_dates[0]} to {effective_dates[-1]} ({iteration_type} iteration)"
    )

    all_records = []

    # Iterate through each effective date
    for effective_date_str in effective_dates:
        if is_incremental_sync:
            log.fine(f"Fetching positions for effective date: {effective_date_str} (daily sync)")
        else:
            log.fine(
                f"Fetching positions for effective date: {effective_date_str} (last day of month)"
            )

        # Fetch all pages for this effective date
        page = 1
        has_more_data = True

        while has_more_data:
            log.fine(f"Fetching Workday positions data for {effective_date_str}, page {page}")

            try:
                soap_body = _create_get_positions_soap_envelope(
                    page=page,
                    count=_PAGE_SIZE,
                    effective_date=effective_date_str,
                    position_ids=(
                        position_ids if page == 1 else None
                    ),  # Only use position_ids on first page
                )

                xml_response = make_soap_request(api_url, soap_body, username, password)
                records, has_more_pages, total_pages = _parse_soap_response(
                    xml_response, effective_date_str
                )

                if not records:
                    has_more_data = False
                    continue

                all_records.extend(records)

                # Check if more pages are available
                has_more_data = has_more_pages
                page += 1

                log.fine(
                    f"Fetched {len(records)} positions from page {page - 1} of {total_pages} for {effective_date_str}"
                )

            except Exception as e:
                log.warning(
                    f"Failed to fetch data for {effective_date_str}, page {page}: {str(e)}"
                )
                raise

        log.fine(f"Completed fetching positions for effective date {effective_date_str}")

    log.fine(
        f"Fetched {len(all_records)} Workday position records total across {len(effective_dates)} {date_description}s"
    )
    return all_records


def schema(configuration: dict) -> List[Dict]:
    """Define the schema function which configures the schema your connector delivers.

    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema

    Args:
        configuration: a dictionary that holds the configuration settings for the connector.

    Returns:
        List of table schema definitions
    """
    return [
        {
            "table": "positions",
            "primary_key": ["position_id", "effective_date"],
        },
    ]


def _process_position_records(position_records: List[Dict]) -> int:
    """Process position records and return the count of processed records.

    Args:
        position_records: List of position records to process

    Returns:
        Number of records processed
    """
    records_processed = 0

    for record in position_records:
        # The 'upsert' operation is used to insert or update data in the destination table.
        # The op.upsert method is called with two arguments:
        # - The first argument is the name of the table to upsert the data into.
        # - The second argument is a dictionary containing the data to be upserted,
        op.upsert(table="positions", data=record)
        records_processed += 1

        # Checkpoint progress at regular intervals
        if records_processed % _CHECKPOINT_INTERVAL == 0:
            checkpoint_state = {
                "records_processed": records_processed,
            }
            # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
            # from the correct position in case of next sync or interruptions.
            # Learn more about how and where to checkpoint by reading our best practices documentation
            # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
            op.checkpoint(checkpoint_state)
            log.fine(f"Checkpointed after processing {records_processed} position records")

    return records_processed


def update(configuration: dict, state: dict) -> None:
    """Define the update function, which is called by Fivetran during each sync.

    See the technical reference documentation for more details on the update function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update

    Args:
        configuration: A dictionary containing connection details
        state: A dictionary containing state information from previous runs
               The state dictionary is empty for the first sync or for any full re-sync
    """
    log.info("Workday HCM Connector: Starting sync")

    # Validate the configuration to ensure it contains all required values
    validate_configuration(configuration=configuration)

    # Get the state variable for incremental sync
    last_sync_time = state.get("last_sync_time")
    records_processed = 0

    try:
        # Fetch and process position data
        position_records = get_positions_data(configuration, last_sync_time)
        records_processed = _process_position_records(position_records)

        # Final checkpoint with the latest sync time
        final_state = {
            "last_sync_time": datetime.now(timezone.utc).isoformat(),
            "records_processed": records_processed,
            "last_successful_sync": datetime.now(timezone.utc).isoformat(),
        }
        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        op.checkpoint(final_state)

        log.info(
            f"Workday HCM Connector: Successfully processed {records_processed} position records"
        )

    except Exception as e:
        error_msg = f"Failed to sync Workday data: {str(e)}"
        log.warning(error_msg)
        raise RuntimeError(error_msg)


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This allows your script to be run directly from the command line or IDE for debugging.
# This method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents
    with open("configuration.json", "r") as f:
        configuration = json.load(f)

    # Test the connector locally
    connector.debug(configuration=configuration)
