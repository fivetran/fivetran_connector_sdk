"""
Livestock Weather Intelligence - Fivetran Custom Connector with Snowflake Cortex AI

Chains weather forecast APIs with Snowflake Cortex Agent to provide actionable
livestock health intelligence based on predicted weather conditions.

APIs Used:
1. Zippopotam.us - ZIP code to coordinates
2. National Weather Service - Weather forecasts
3. Snowflake Cortex Agent - Livestock health risk analysis

Features:
- Weather forecast sync for farm ZIP codes
- Real-time AI enrichment during ingestion
- Livestock health risk assessment based on weather
- Farm-specific recommendations
- Historical weather-health correlations
- Cost control with max_enrichments limit
"""

import json
import time
import requests
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op


# ============================================================================
# MODULE-LEVEL CONSTANTS
# ============================================================================

# API Configuration Constants
__BASE_URL_ZIPPOPOTAM = "http://api.zippopotam.us/us"
__BASE_URL_NWS = "https://api.weather.gov"
__API_TIMEOUT_SECONDS = 10

# Retry and Rate Limiting Constants
__MAX_RETRIES = 3
__BASE_DELAY_SECONDS = 1
__RETRYABLE_STATUS_CODES = [429, 500, 502, 503, 504]
__NWS_RATE_LIMIT_DELAY = 1.0
__ZIP_API_RATE_LIMIT_DELAY = 0.5

# Default Configuration Values
__DEFAULT_MAX_ENRICHMENTS = 10
__DEFAULT_CORTEX_TIMEOUT = 60
__DEFAULT_USER_AGENT = "LivestockWeatherIntelligence/1.0"

# Cortex Agent Configuration
__CORTEX_AGENT_ENDPOINT = "/api/v2/cortex/agent:run"


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================


def flatten_dict(d, parent_key="", sep="_"):
    """
    Flatten nested dictionaries and serialize lists to JSON strings.

    This is REQUIRED for Fivetran compatibility as nested structures
    cannot be directly inserted into warehouse tables.

    Args:
        d: Dictionary to flatten
        parent_key: Prefix for nested keys (used in recursion)
        sep: Separator between nested key levels

    Returns:
        Flattened dictionary with all nested structures resolved

    Example:
        >>> flatten_dict({'user': {'name': 'John', 'age': 30}, 'tags': ['a', 'b']})
        {'user_name': 'John', 'user_age': 30, 'tags': '["a", "b"]'}
    """
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        elif isinstance(v, (list, tuple)):
            items.append((new_key, json.dumps(v) if v else None))
        else:
            items.append((new_key, v))
    return dict(items)


def fetch_data_with_retry(session, url, params=None, headers=None):
    """
    Fetch data from API with exponential backoff retry logic.

    Implements industry-standard retry pattern for transient failures
    including rate limits (429) and server errors (5xx).

    Args:
        session: requests.Session object for connection pooling
        url: Full URL to fetch
        params: Optional query parameters
        headers: Optional request headers

    Returns:
        JSON response as dictionary

    Raises:
        RuntimeError: If all retry attempts fail
        requests.exceptions.RequestException: For non-retryable errors
    """
    for attempt in range(__MAX_RETRIES):
        try:
            response = session.get(
                url, params=params, headers=headers, timeout=__API_TIMEOUT_SECONDS
            )
            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            should_retry = (
                hasattr(e, "response")  # noqa: W503
                and e.response is not None  # noqa: W503
                and hasattr(e.response, "status_code")  # noqa: W503
                and e.response.status_code in __RETRYABLE_STATUS_CODES  # noqa: W503
            )

            if should_retry and attempt < __MAX_RETRIES - 1:
                delay_seconds = __BASE_DELAY_SECONDS * (2**attempt)
                log.warning(
                    f"Request failed with status {e.response.status_code}, "
                    f"retrying in {delay_seconds}s (attempt {attempt + 1}/{__MAX_RETRIES})"
                )
                time.sleep(delay_seconds)
            else:
                # Log final failure
                status_code = (
                    e.response.status_code
                    if hasattr(e, "response") and e.response is not None
                    else "N/A"
                )
                log.severe(
                    f"Failed after {__MAX_RETRIES} attempts. "
                    f"URL: {url}, Status: {status_code}, Error: {str(e)}"
                )
                raise RuntimeError(f"API request failed after {__MAX_RETRIES} attempts: {e}")


def parse_farm_zip_mapping(mapping_str):
    """
    Parse farm-to-ZIP mapping string into dictionary.

    Mapping format allows multiple farms per ZIP code, supporting
    complex farm network configurations.

    Args:
        mapping_str: String in format "ZIP:FARM1,FARM2|ZIP:FARM3"

    Returns:
        Dictionary mapping ZIP codes to lists of farm IDs

    Example:
        >>> parse_farm_zip_mapping("77429:FARM_000000,FARM_000001|77856:FARM_000002")
        {'77429': ['FARM_000000', 'FARM_000001'], '77856': ['FARM_000002']}
    """
    if not mapping_str:
        return {}

    mapping = {}
    for zip_farms in mapping_str.split("|"):
        if ":" not in zip_farms:
            continue
        zip_code, farms_str = zip_farms.split(":", 1)
        mapping[zip_code.strip()] = [f.strip() for f in farms_str.split(",")]

    return mapping


# ============================================================================
# API INTEGRATION FUNCTIONS
# ============================================================================


def get_coordinates_from_zip(session, zip_code):
    """
    Get geographic coordinates for a ZIP code using Zippopotam.us API.

    Zippopotam.us provides free geocoding for US ZIP codes without
    requiring API keys or authentication.

    Args:
        zip_code: 5-digit US ZIP code as string

    Returns:
        Dictionary containing latitude, longitude, place_name, state,
        and state_abbr, or None if ZIP code not found or API error occurs

    Raises:
        requests.exceptions.RequestException: For non-404 HTTP errors

    Example:
        >>> get_coordinates_from_zip("77429")
        {
            'zip_code': '77429',
            'latitude': 29.8088,
            'longitude': -95.6391,
            'place_name': 'Cypress',
            'state': 'Texas',
            'state_abbr': 'TX'
        }
    """
    url = f"{__BASE_URL_ZIPPOPOTAM}/{zip_code}"

    try:
        data = fetch_data_with_retry(session, url)

        if "places" in data and len(data["places"]) > 0:
            place = data["places"][0]
            return {
                "zip_code": zip_code,
                "latitude": float(place["latitude"]),
                "longitude": float(place["longitude"]),
                "place_name": place["place name"],
                "state": place["state"],
                "state_abbr": place["state abbreviation"],
            }
        else:
            log.warning(f"No location data found for ZIP code {zip_code}")
            return None

    except RuntimeError as e:
        # Handle 404 as expected case (invalid ZIP)
        if "404" in str(e):
            log.warning(f"ZIP code {zip_code} not found (404)")
            return None
        raise
    except Exception as e:
        log.warning(f"Error fetching coordinates for ZIP {zip_code}: {str(e)}")
        return None


def get_weather_forecast(session, latitude, longitude, user_agent):
    """
    Get weather forecast from National Weather Service API.

    NWS API requires a two-step process:
    1. Convert lat/long to grid coordinates
    2. Fetch forecast for grid location

    Args:
        session: requests.Session object for connection pooling
        latitude: Latitude as float
        longitude: Longitude as float
        user_agent: User-Agent header (required by NWS, should include contact)

    Returns:
        Dictionary containing office, grid coordinates, and forecast data,
        or None if API request fails

    Raises:
        requests.exceptions.RequestException: For API errors

    Note:
        NWS API requires User-Agent with contact info per their terms of service
    """
    headers = {"User-Agent": user_agent}

    try:
        # Step 1: Get grid coordinates
        points_url = f"{__BASE_URL_NWS}/points/{latitude},{longitude}"
        log.info(f"Fetching grid coordinates for {latitude},{longitude}")

        points_data = fetch_data_with_retry(session, points_url, headers=headers)

        properties = points_data.get("properties", {})
        office = properties.get("gridId")
        grid_x = properties.get("gridX")
        grid_y = properties.get("gridY")

        if not all([office, grid_x, grid_y]):
            log.warning(f"Missing grid data for {latitude},{longitude}")
            return None

        # Rate limiting - NWS requests 1 second between calls
        time.sleep(__NWS_RATE_LIMIT_DELAY)

        # Step 2: Get forecast
        forecast_url = f"{__BASE_URL_NWS}/gridpoints/{office}/{grid_x},{grid_y}/forecast"
        log.info(f"Fetching forecast from {office} office, grid {grid_x},{grid_y}")

        forecast_data = fetch_data_with_retry(session, forecast_url, headers=headers)

        return {"office": office, "grid_x": grid_x, "grid_y": grid_y, "forecast": forecast_data}

    except Exception as e:
        log.warning(f"Error fetching weather forecast: {str(e)}")
        return None


def parse_agent_response(response_text, weather_record):
    """
    Parse the Cortex Agent's structured response into enriched fields.

    Extracts structured sections from agent response with robust parsing
    and fallback default values.

    Args:
        response_text: Full text response from Cortex Agent
        weather_record: Weather record being enriched (for logging)

    Returns:
        Dictionary with enriched fields ready for record update
    """
    enriched = {}

    # Default values
    enriched["agent_livestock_risk_assessment"] = "No assessment available"
    enriched["agent_affected_farms"] = json.dumps([])
    enriched["agent_species_risk_matrix"] = json.dumps({})
    enriched["agent_recommended_actions"] = "Monitor livestock regularly"
    enriched["agent_historical_correlation"] = "No historical data available"

    response_upper = response_text.upper()

    # Extract RISK ASSESSMENT section
    if "RISK ASSESSMENT:" in response_upper:
        start = response_upper.find("RISK ASSESSMENT:") + len("RISK ASSESSMENT:")
        end = response_upper.find("AFFECTED FARMS:", start)
        if end == -1:
            end = start + 500
        section = response_text[start:end].strip()
        if section and len(section) > 10:
            enriched["agent_livestock_risk_assessment"] = section[:1000]

    # Extract AFFECTED FARMS section
    if "AFFECTED FARMS:" in response_upper:
        start = response_upper.find("AFFECTED FARMS:") + len("AFFECTED FARMS:")
        end = response_upper.find("SPECIES RISK MATRIX:", start)
        if end == -1:
            end = start + 200
        section = response_text[start:end].strip()

        # Parse farm IDs
        farm_ids = []
        if section and "no high-risk" not in section.lower():
            # Try to extract FARM_XXXXXX patterns
            import re

            farm_pattern = r"FARM_\d{6}"
            farm_ids = re.findall(farm_pattern, section)

            if not farm_ids:
                # Fallback: split by commas
                farm_ids = [f.strip() for f in section.split(",") if "FARM" in f.upper()]

        enriched["agent_affected_farms"] = json.dumps(list(set(farm_ids)))  # Remove duplicates

    # Extract SPECIES RISK MATRIX section
    if "SPECIES RISK MATRIX:" in response_upper:
        start = response_upper.find("SPECIES RISK MATRIX:") + len("SPECIES RISK MATRIX:")
        end = response_upper.find("RECOMMENDED ACTIONS:", start)
        if end == -1:
            end = start + 300
        section = response_text[start:end].strip()

        # Parse species risks
        species_risks = {}
        for line in section.split("\n"):
            if ":" in line:
                parts = line.split(":", 1)
                if len(parts) == 2:
                    species = parts[0].strip().lstrip("-*• ")
                    risk = parts[1].strip().split()[0]  # Get first word (risk level)
                    if species and risk:
                        species_risks[species] = risk

        enriched["agent_species_risk_matrix"] = json.dumps(species_risks)

    # Extract RECOMMENDED ACTIONS section
    if "RECOMMENDED ACTIONS:" in response_upper:
        start = response_upper.find("RECOMMENDED ACTIONS:") + len("RECOMMENDED ACTIONS:")
        end = response_upper.find("HISTORICAL CORRELATION:", start)
        if end == -1:
            end = start + 800
        section = response_text[start:end].strip()
        if section and len(section) > 10:
            enriched["agent_recommended_actions"] = section[:1500]

    # Extract HISTORICAL CORRELATION section
    if "HISTORICAL CORRELATION:" in response_upper:
        start = response_upper.find("HISTORICAL CORRELATION:") + len("HISTORICAL CORRELATION:")
        section = response_text[start : start + 800].strip()
        if section and len(section) > 10:
            enriched["agent_historical_correlation"] = section[:1000]

    return enriched


def call_cortex_livestock_agent(session, config, weather_record):
    """
    Call Snowflake Cortex Agent via REST API for livestock health analysis.

    Invokes Cortex Agent with Cortex Analyst tool to query AGR_RECORDS
    and generate weather-based livestock health risk assessments.

    Args:
        session: requests.Session object with proper authentication
        config: Configuration dictionary containing Snowflake credentials
        weather_record: Weather forecast record to analyze

    Returns:
        Dictionary with enriched fields (risk_assessment, affected_farms, etc.),
        or None if agent call fails

    Raises:
        requests.exceptions.RequestException: For API communication errors

    Note:
        Uses SSE streaming response format from Cortex Agent API
        Endpoint: POST /api/v2/cortex/agent:run
    """
    snowflake_account = config.get("snowflake_account")
    pat_token = config.get("snowflake_pat_token")
    timeout = int(config.get("cortex_timeout", str(__DEFAULT_CORTEX_TIMEOUT)))

    # Get farm IDs for this ZIP code
    farm_ids = weather_record.get("farm_ids", [])
    if not farm_ids:
        return None

    farm_ids_str = ", ".join(farm_ids)

    # Construct Cortex Agent REST API endpoint
    url = f"https://{snowflake_account}{__CORTEX_AGENT_ENDPOINT}"

    headers = {
        "Authorization": f"Bearer {pat_token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    # Build weather analysis prompt
    prompt = f"""Analyze weather impact on livestock health for farms: {farm_ids_str}

**WEATHER FORECAST:**
- Location: {weather_record.get('place_name')}, {weather_record.get('state')} (ZIP: {weather_record.get('zip_code')})
- Period: {weather_record.get('period_name')}
- Time: {weather_record.get('start_time')} to {weather_record.get('end_time')}
- Temperature: {weather_record.get('temperature')}°{weather_record.get('temperature_unit')}
- Temperature Trend: {weather_record.get('temperature_trend') or 'Stable'}
- Conditions: {weather_record.get('short_forecast')}
- Detailed: {weather_record.get('detailed_forecast')}
- Wind: {weather_record.get('wind_speed')}, {weather_record.get('wind_direction')}

**YOUR TASK:**
Query the AGR_RECORDS table using your Cortex Analyst tool to analyze current livestock health for these farms and assess weather-related risks.

Provide your response in these exact sections:

RISK ASSESSMENT:
[Overall risk level and 2-3 sentence summary]

AFFECTED FARMS:
[Comma-separated farm IDs with concerns, or "No high-risk farms identified"]

SPECIES RISK MATRIX:
[Format: Species: RiskLevel (one per line)]

RECOMMENDED ACTIONS:
1. [Specific action with timeframe]
2. [Specific action with timeframe]
3. [Specific action with timeframe]

HISTORICAL CORRELATION:
[Past weather-health patterns or "No similar historical events found"]

Use real data from AGR_RECORDS to make specific, actionable recommendations."""

    # Cortex Agent API payload - matches official docs
    payload = {
        "model": "llama3.3-70b",
        "response_instruction": "You are a livestock health expert. Use your Cortex Analyst tool to query AGR_RECORDS data and provide data-driven livestock health risk assessments based on weather forecasts.",
        "tools": [{"tool_spec": {"type": "cortex_analyst_text_to_sql", "name": "Analyst1"}}],
        "tool_resources": {
            "Analyst1": {
                "semantic_view": "HOL_DATABASE.AGR_0729_CLAUDE.AGR_LIVESTOCK_OVERALL_PERFORMANCE_SV"
            }
        },
        "tool_choice": {"type": "auto"},
        "messages": [{"role": "user", "content": [{"type": "text", "text": prompt}]}],
    }

    try:
        response = session.post(url, headers=headers, json=payload, timeout=timeout, stream=True)
        response.raise_for_status()

        # Parse Server-Sent Events (SSE) streaming response
        agent_response = ""
        for line in response.iter_lines():
            if line:
                line_text = line.decode("utf-8")
                if line_text.startswith("data: "):
                    try:
                        data = json.loads(line_text[6:])

                        # Skip if data is a list (telemetry/tracing data)
                        if not isinstance(data, dict):
                            continue

                        # Extract text from message.delta events
                        if data.get("object") == "message.delta":
                            delta = data.get("delta", {})
                            content = delta.get("content", [])

                            # Validate content is a list before iterating
                            if not isinstance(content, list):
                                continue

                            # Iterate through content array
                            for item in content:
                                # Validate each item is a dict before calling .get()
                                if not isinstance(item, dict):
                                    continue

                                # Safe to use .get() now
                                if item.get("type") == "text":
                                    agent_response += item.get("text", "")

                                elif item.get("type") == "tool_results":
                                    # Extract text from tool results
                                    tool_results = item.get("tool_results")

                                    # tool_results can be dict or list
                                    if isinstance(tool_results, dict):
                                        tool_content = tool_results.get("content", [])

                                        if isinstance(tool_content, list):
                                            for result_item in tool_content:
                                                if not isinstance(result_item, dict):
                                                    continue

                                                if result_item.get("type") == "json":
                                                    json_data = result_item.get("json", {})
                                                    if (
                                                        isinstance(json_data, dict)
                                                        and "text" in json_data  # noqa: W503
                                                    ):  # noqa: W503
                                                        agent_response += json_data["text"] + "\n"

                                                elif result_item.get("type") == "text":
                                                    agent_response += result_item.get("text", "")

                                    elif isinstance(tool_results, list):
                                        for result_item in tool_results:
                                            if not isinstance(result_item, dict):
                                                continue

                                            if result_item.get("type") == "json":
                                                json_data = result_item.get("json", {})
                                                if (
                                                    isinstance(json_data, dict)
                                                    and "text" in json_data  # noqa: W503
                                                ):  # noqa: W503
                                                    agent_response += json_data["text"] + "\n"

                                            elif result_item.get("type") == "text":
                                                agent_response += result_item.get("text", "")

                    except json.JSONDecodeError:
                        continue
                    except Exception as e:
                        log.warning(f"Error parsing SSE line: {e}")
                        continue

                elif line_text.startswith("event: done"):
                    break

        if not agent_response:
            log.warning(f"Empty response from agent for ZIP {weather_record.get('zip_code')}")
            return None

        # Parse the structured response
        enriched = parse_agent_response(agent_response, weather_record)

        log.info(
            f"✓ Enriched forecast for {weather_record.get('zip_code')} {weather_record.get('period_name')}"
        )
        return enriched

    except requests.exceptions.Timeout:
        log.warning(f"Agent timeout after {timeout}s for ZIP {weather_record.get('zip_code')}")
        return None
    except Exception as e:
        log.warning(f"Agent error for ZIP {weather_record.get('zip_code')}: {e}")
        return None


# ============================================================================
# FIVETRAN SDK FUNCTIONS
# ============================================================================


def schema(configuration: dict):
    """
    Define the schema for the connector.

    Fivetran will automatically infer column types from the data.
    Only the table name and primary key need to be specified.

    Args:
        configuration: Configuration dictionary (not used for schema)

    Returns:
        List of table definitions with primary keys
    """
    return [
        {"table": "livestock_weather_intelligence", "primary_key": ["zip_code", "period_number"]}
    ]


def update(configuration: dict, state: dict):
    """
    Extract weather forecasts and enrich with livestock health intelligence.

    Main sync function that orchestrates the complete data pipeline:
    1. Parse configuration and validate required fields
    2. Set up API sessions and authentication
    3. Process each ZIP code with inline logic (ORIGINAL WORKING PATTERN)
    4. Checkpoint after processing all ZIP codes

    Args:
        configuration: Configuration dictionary from configuration.json
        state: State dictionary from previous sync (empty on first run)

    Yields:
        Operation: Upsert operations for records and checkpoint operations

    Raises:
        ValueError: If required configuration is missing
        RuntimeError: If API requests fail after max retries
    """
    # Get configuration values with type conversion and defaults
    zip_codes_str = configuration.get("zip_codes", "")
    user_agent = configuration.get("user_agent", __DEFAULT_USER_AGENT)
    farm_zip_mapping_str = configuration.get("farm_zip_mapping", "")

    # Validate required configuration
    if not zip_codes_str:
        raise ValueError("zip_codes configuration is required")

    # Parse ZIP codes
    zip_codes = [z.strip() for z in zip_codes_str.split(",") if z.strip()]

    if not zip_codes:
        raise ValueError("No valid ZIP codes provided")

    # Parse farm-to-ZIP mapping
    farm_mapping = parse_farm_zip_mapping(farm_zip_mapping_str)

    # Get Cortex enrichment configuration with type conversion
    enable_cortex = configuration.get("enable_cortex_enrichment", "false").lower() == "true"
    snowflake_account = configuration.get("snowflake_account")
    snowflake_pat_token = configuration.get("snowflake_pat_token")
    max_enrichments = int(configuration.get("max_enrichments", str(__DEFAULT_MAX_ENRICHMENTS)))

    # Validate Cortex configuration
    if enable_cortex:
        if not all([snowflake_account, snowflake_pat_token]):
            log.warning("Cortex enrichment disabled: Missing Snowflake credentials")
            enable_cortex = False
        else:
            log.info(f"Cortex enrichment ENABLED: max_enrichments={max_enrichments}")
    else:
        log.info("Cortex enrichment DISABLED")

    # Log sync parameters
    log.info(f"Starting weather forecast sync for {len(zip_codes)} ZIP codes")
    if farm_mapping:
        log.info(f"Farm mapping configured for {len(farm_mapping)} ZIP codes")

    # Set up API sessions
    weather_session = requests.Session()
    cortex_session = requests.Session() if enable_cortex else None

    total_forecasts = 0
    enriched_count = 0

    try:
        # Process each ZIP code (ORIGINAL WORKING INLINE PATTERN)
        for zip_code in zip_codes:
            log.info(f"Processing ZIP code: {zip_code}")

            # Get farm IDs for this ZIP
            farm_ids = farm_mapping.get(zip_code, [])

            # Step 1: Get coordinates
            location_data = get_coordinates_from_zip(weather_session, zip_code)

            if not location_data:
                log.warning(f"Skipping ZIP code {zip_code} - no location data")
                continue

            time.sleep(__ZIP_API_RATE_LIMIT_DELAY)

            # Step 2: Get weather forecast
            try:
                weather_data = get_weather_forecast(
                    weather_session,
                    location_data["latitude"],
                    location_data["longitude"],
                    user_agent,
                )

                if not weather_data or "forecast" not in weather_data:
                    log.warning(f"No forecast data for ZIP code {zip_code}")
                    continue

                # Extract forecast periods
                periods = weather_data["forecast"].get("properties", {}).get("periods", [])

                if not periods:
                    log.warning(f"No forecast periods for ZIP code {zip_code}")
                    continue

                # Process each forecast period
                for period in periods:
                    period_number = period.get("number")
                    if period_number is None:
                        continue

                    # Build base record
                    record = {
                        # Location data
                        "zip_code": zip_code,
                        "place_name": location_data["place_name"],
                        "state": location_data["state"],
                        "state_abbr": location_data["state_abbr"],
                        "latitude": location_data["latitude"],
                        "longitude": location_data["longitude"],
                        # Farm data
                        "farm_ids": farm_ids,
                        "farm_count": len(farm_ids),
                        # NWS office/grid data
                        "nws_office": weather_data["office"],
                        "grid_x": weather_data["grid_x"],
                        "grid_y": weather_data["grid_y"],
                        # Forecast period data
                        "period_number": period_number,
                        "period_name": period.get("name"),
                        "start_time": period.get("startTime"),
                        "end_time": period.get("endTime"),
                        "is_daytime": period.get("isDaytime"),
                        "temperature": period.get("temperature"),
                        "temperature_unit": period.get("temperatureUnit"),
                        "temperature_trend": period.get("temperatureTrend"),
                        "wind_speed": period.get("windSpeed"),
                        "wind_direction": period.get("windDirection"),
                        "icon": period.get("icon"),
                        "short_forecast": period.get("shortForecast"),
                        "detailed_forecast": period.get("detailedForecast"),
                    }

                    # Add AI enrichment if enabled and within limits
                    if enable_cortex and enriched_count < max_enrichments and farm_ids:
                        enriched_fields = call_cortex_livestock_agent(
                            cortex_session, configuration, record
                        )

                        if enriched_fields:
                            record.update(enriched_fields)
                            enriched_count += 1
                        else:
                            # Add empty enrichment fields if agent call failed
                            record.update(
                                {
                                    "agent_livestock_risk_assessment": None,
                                    "agent_affected_farms": None,
                                    "agent_species_risk_matrix": None,
                                    "agent_recommended_actions": None,
                                    "agent_historical_correlation": None,
                                }
                            )
                    else:
                        # No enrichment
                        if not farm_ids:
                            record.update(
                                {
                                    "agent_livestock_risk_assessment": "No farms mapped to this ZIP code",
                                    "agent_affected_farms": json.dumps([]),
                                    "agent_species_risk_matrix": json.dumps({}),
                                    "agent_recommended_actions": "Configure farm_zip_mapping to enable analysis",
                                    "agent_historical_correlation": "Farm mapping required",
                                }
                            )

                    # Flatten nested structures
                    flattened_record = flatten_dict(record)

                    # Upsert record
                    yield op.upsert(table="livestock_weather_intelligence", data=flattened_record)
                    total_forecasts += 1

                log.info(f"Successfully processed {len(periods)} periods for ZIP {zip_code}")

            except requests.exceptions.RequestException as e:
                log.warning(f"Failed to fetch weather for ZIP {zip_code}: {str(e)}")
                continue

        # Checkpoint
        yield op.checkpoint(
            state={
                "last_sync": time.time(),
                "zip_codes_processed": len(zip_codes),
                "forecasts_synced": total_forecasts,
                "enrichments_completed": enriched_count,
            }
        )

        log.info(
            f"Sync complete: {total_forecasts} forecasts from {len(zip_codes)} ZIP codes, {enriched_count} enriched"
        )

    except Exception as e:
        log.warning(f"Unexpected error during sync: {str(e)}")
        raise

    finally:
        weather_session.close()
        if cortex_session:
            cortex_session.close()


# ============================================================================
# CONNECTOR INSTANCE
# ============================================================================

connector = Connector(update=update, schema=schema)
