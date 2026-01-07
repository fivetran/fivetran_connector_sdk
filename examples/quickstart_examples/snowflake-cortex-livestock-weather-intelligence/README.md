# Livestock Weather Intelligence - Fivetran Custom Connector

Custom Fivetran connector that syncs weather forecasts with **real-time AI-powered livestock health risk analysis** using a Snowflake Cortex Agent during ingestion.

## 🎯 What This Connector Does

- **Syncs weather data**: 7-day forecasts for farm ZIP codes (NOAA/NWS)
- **AI enrichment**: Adds livestock health intelligence via Snowflake Cortex Agent REST API and Cortex Analyst tool
- **Farm-specific analysis**: Correlates weather → current livestock health status
- **Actionable recommendations**: Specific preventive actions before weather arrives
- **Historical context**: References past weather-health correlations
- **Cost control**: Configurable limit on enrichments for testing/production
- **Ready-to-try**: Exponential backoff retry logic, comprehensive error handling, and extensive logging

## ✨ The Value Proposition

**Traditional Weather Alerts:**
> "Cold front approaching: Low 28°F, winds 15-20 mph"

**Livestock Weather Intelligence:**
> "ALERT: 144 Beef Cattle at FARM_000000 face dangerous conditions. 15 injured animals highly vulnerable. Historical data shows 35% risk increase. Actions: Move injured cattle indoors within 6 hours, increase feed 20%, prepare heated water, stock antibiotics. Similar conditions in Dec 2024 caused 8 respiratory incidents ($3,600 treatment cost). Early intervention reduced impact 62%."

## 📊 How It Works

```
ZIP Codes (77429, 77856, 81611)
    ↓
[Zippopotam.us API] + Exponential Backoff Retry
    ↓
Coordinates + Location Names
    ↓
[National Weather Service API] + Rate Limiting
    ↓
7-Day Weather Forecasts (14 periods)
    ↓
[Snowflake Cortex Agent REST API] + llama3.3-70b + Cortex Analyst
    ↓
Queries AGR_RECORDS via Semantic View
Analyzes current livestock health
Correlates weather → health risks
    ↓
Enriched Intelligence Added During Ingestion
    ↓
[Fivetran → Snowflake]
    ↓
Actionable Livestock Weather Intelligence
```

## ✨ AI Enrichment Fields Added

For each weather forecast period, the Snowflake Cortex Agent adds:

1. **`agent_livestock_risk_assessment`** - Overall health risk analysis
   - *Example: "The overall risk level for the specified farms is moderate, with potential concerns related to respiratory health due to cloudy conditions and moderate temperatures. Current livestock health data indicates good overall health, but vigilance is warranted."*

2. **`agent_affected_farms`** - JSON array of farm IDs at risk
   - *Example: `["FARM_000000", "FARM_000001"]`*

3. **`agent_species_risk_matrix`** - JSON object with species-specific risks
   - *Example: `{"Beef Cattle": "Moderate", "Chickens": "Low", "Pigs": "Low", "Goats": "Low"}`*

4. **`agent_recommended_actions`** - Numbered list of specific actions
   - *Example: "1. Monitor temperature and humidity levels closely in livestock housing areas overnight to prevent respiratory stress. 2. Ensure adequate ventilation in all livestock structures, especially for cattle and poultry. 3. Provide clean, dry bedding to maintain comfort and prevent health issues during the cloudy, mild conditions."*

5. **`agent_historical_correlation`** - Past weather-health patterns
   - *Example: "Historical data indicates that similar cloudy, mild conditions in the past have occasionally been associated with minor increases in respiratory issues among cattle, particularly if ventilation is inadequate. However, overall impacts have been minimal when proper management practices are followed."*

## 📋 Prerequisites

- Python 3.8 or higher (3.12 recommended)
- Fivetran CLI: `pip install fivetran-connector-sdk`
- Fivetran account with API access
- Snowflake account with Cortex enabled
- Snowflake PAT (Personal Access Token)
- Existing **AGR_RECORDS** table with livestock health data
- Existing **Semantic View** for Cortex Analyst access

## 🚀 Quick Start

### 1. Install Dependencies

```bash
pip install fivetran-connector-sdk
```

**Note:** No additional dependencies required! The connector uses only pre-installed packages (requests, json, time).

### 2. Get Snowflake PAT Token

**Via Snowflake UI (Recommended):**
1. Go to https://app.snowflake.com/
2. Click your profile icon (top right) → **Account Settings**
3. Go to **Security** → **Personal Access Tokens**
4. Click **+ Token** to create a new one
5. Give it a name like "Fivetran Connector"
6. **Copy the token immediately** (you can't see it again!)

Token format: `ver:1-hint:abc123def456-ETMsDg...`

**Grant necessary permissions:**

```sql
USE ROLE ACCOUNTADMIN;

-- Grant access to your livestock data
GRANT USAGE ON DATABASE YOUR_DATABASE_NAME TO USER YOUR_USERNAME;
GRANT USAGE ON SCHEMA YOUR_DATABASE_NAME.YOUR_SCHEMA_NAME TO USER YOUR_USERNAME;
GRANT SELECT ON ALL TABLES IN SCHEMA YOUR_DATABASE_NAME.YOUR_SCHEMA_NAME TO USER YOUR_USERNAME;
GRANT SELECT ON ALL VIEWS IN SCHEMA YOUR_DATABASE_NAME.YOUR_SCHEMA_NAME TO USER YOUR_USERNAME;
```

### 3. Map Farms to ZIP Codes

Determine which farms operate in which ZIP code areas. This mapping tells the agent which livestock to analyze for each weather forecast.

**Format:** `"ZIP:FARM1,FARM2|ZIP:FARM3"`

**Example:**
```
77429:FARM_000000,FARM_000001|77856:FARM_000002|81611:FARM_000003,FARM_000004
```

This means:
- ZIP 77429 has FARM_000000 and FARM_000001
- ZIP 77856 has FARM_000002
- ZIP 81611 has FARM_000003 and FARM_000004

### 4. Configure Data Source

Create `configuration.json`:

```json
{
  "zip_codes": "77429,77856,81611",
  "user_agent": "LivestockWeatherIntelligence/1.0 (your-email@example.com)",
  "farm_zip_mapping": "77429:FARM_000000,FARM_000001|77856:FARM_000002|81611:FARM_000003",
  "enable_cortex_enrichment": "true",
  "snowflake_account": "YOUR_ACCOUNT.REGION.snowflakecomputing.com",
  "snowflake_pat_token": "ver:1-hint:YOUR_TOKEN_HERE",
  "cortex_timeout": "60",
  "max_enrichments": "5"
}
```

**Configuration Parameters:**

| Parameter | Required | Description | Default | Example |
|-----------|----------|-------------|---------|---------|
| `zip_codes` | ✅ | Comma-separated US ZIP codes | - | `"77429,77856,81611"` |
| `user_agent` | ✅ | Contact info for NWS API | - | `"App/1.0 (email@domain.com)"` |
| `farm_zip_mapping` | ✅ | Map ZIP codes to farm IDs | - | `"77429:FARM_001,FARM_002"` |
| `enable_cortex_enrichment` | ❌ | Enable AI enrichment | `"false"` | `"true"` |
| `snowflake_account` | ✅* | Snowflake account URL | - | `"abc12345.us-east-1.snowflakecomputing.com"` |
| `snowflake_pat_token` | ✅* | Personal Access Token | - | `"ver:1-hint:..."` |
| `cortex_timeout` | ❌ | Timeout per agent call (seconds) | `"60"` | `"90"` |
| `max_enrichments` | ❌ | Max enrichments per sync | `"10"` | `"5"` |

*Required only if `enable_cortex_enrichment` is `"true"`

**Finding Your Snowflake Account URL:**
1. Log into Snowflake
2. Look at your browser URL: `https://app.snowflake.com/ORGID/ACCOUNTID/...`
3. Your account URL format: `ACCOUNTID.REGION.snowflakecomputing.com`
4. Example: `abc12345-my-org.us-west-2.snowflakecomputing.com`

### 5. Test Locally

```bash
# Test the connector
fivetran debug --configuration configuration.json

# Expected output:
# ✓ Enriched forecast for 77429 This Afternoon
# ✓ Enriched forecast for 77429 Tonight
# Sync complete: 42 forecasts from 3 ZIP codes, 5 enriched
```

### 6. Deploy to Fivetran

Once local testing passes:

1. Create a Fivetran account if you don't have one
2. Set up a destination (Snowflake, BigQuery, etc.)
3. Deploy the connector using the Fivetran SDK CLI
4. Configure the sync schedule

## 📊 Table Schema

### livestock_weather_intelligence

Weather forecasts enriched with livestock health risk analysis.

**Primary Key:** `zip_code`, `period_number`

**Base Weather Fields:**

| Column | Type | Description |
|--------|------|-------------|
| `zip_code` | STRING | US ZIP code |
| `place_name` | STRING | City/town name |
| `state` | STRING | Full state name |
| `state_abbr` | STRING | Two-letter state code |
| `latitude` | FLOAT | Location latitude |
| `longitude` | FLOAT | Location longitude |
| `farm_ids` | STRING | JSON array of farm IDs in this ZIP |
| `farm_count` | INTEGER | Number of farms mapped to ZIP |
| `nws_office` | STRING | NWS forecast office code |
| `grid_x` | INTEGER | NWS grid X coordinate |
| `grid_y` | INTEGER | NWS grid Y coordinate |
| `period_number` | INTEGER | Forecast period (1-14) |
| `period_name` | STRING | Period name (e.g., "Tonight", "Friday") |
| `start_time` | TIMESTAMP | Period start time (ISO 8601) |
| `end_time` | TIMESTAMP | Period end time (ISO 8601) |
| `is_daytime` | BOOLEAN | Daytime period indicator |
| `temperature` | INTEGER | Temperature value |
| `temperature_unit` | STRING | Temperature unit ("F" or "C") |
| `temperature_trend` | STRING | Trend (rising/falling/null) |
| `wind_speed` | STRING | Wind speed (e.g., "5 to 10 mph") |
| `wind_direction` | STRING | Wind direction (e.g., "N", "SW") |
| `icon` | STRING | URL to weather icon |
| `short_forecast` | STRING | Brief forecast summary |
| `detailed_forecast` | STRING | Detailed forecast description |

**AI-Enriched Livestock Intelligence Fields:**

| Column | Type | Description |
|--------|------|-------------|
| `agent_livestock_risk_assessment` | STRING | Overall livestock health risk analysis (or default message if not enriched) |
| `agent_affected_farms` | STRING | JSON array of farm IDs at risk (or empty array if no farms/not enriched) |
| `agent_species_risk_matrix` | STRING | JSON object with species-specific risks (or empty object if not enriched) |
| `agent_recommended_actions` | STRING | Numbered list of specific preventive actions (or default message if not enriched) |
| `agent_historical_correlation` | STRING | Past weather-health patterns (or default message if not enriched) |

## 💡 Example Queries

### Critical Weather Alerts for Livestock

```sql
-- Find high-risk weather periods requiring immediate action
SELECT 
    zip_code,
    place_name,
    period_name,
    start_time,
    temperature || temperature_unit as temp,
    short_forecast,
    agent_livestock_risk_assessment,
    agent_affected_farms,
    agent_recommended_actions
FROM livestock_weather_intelligence
WHERE (agent_livestock_risk_assessment LIKE '%High%'
   OR agent_livestock_risk_assessment LIKE '%Critical%'
   OR agent_livestock_risk_assessment LIKE '%elevated%')
  AND agent_livestock_risk_assessment NOT LIKE '%No assessment available%'
ORDER BY start_time;
```

### Farm-Specific Weather Intelligence

```sql
-- Get weather and health risk analysis for a specific farm
SELECT 
    zip_code,
    period_name,
    start_time,
    temperature,
    wind_speed,
    short_forecast,
    agent_livestock_risk_assessment,
    agent_recommended_actions
FROM livestock_weather_intelligence
WHERE agent_affected_farms LIKE '%FARM_000000%'
ORDER BY start_time;
```

### Species-Specific Weather Risks

```sql
-- Parse species risk matrix and identify high-risk species
SELECT 
    zip_code,
    place_name,
    period_name,
    temperature,
    agent_species_risk_matrix,
    agent_recommended_actions
FROM livestock_weather_intelligence
WHERE agent_species_risk_matrix LIKE '%High%'
  AND agent_species_risk_matrix != '{}'
ORDER BY start_time;
```

### Enriched vs Non-Enriched Forecasts

```sql
-- Compare enriched and non-enriched forecast periods
SELECT 
    CASE 
        WHEN agent_livestock_risk_assessment = 'No assessment available' 
             OR agent_livestock_risk_assessment LIKE '%No farms mapped%'
             OR agent_livestock_risk_assessment LIKE '%limit reached%'
        THEN 'Not Enriched'
        ELSE 'Enriched'
    END as enrichment_status,
    COUNT(*) as forecast_count,
    COUNT(DISTINCT zip_code) as zip_codes
FROM livestock_weather_intelligence
GROUP BY enrichment_status;
```

## 💰 Cost Control & Performance

### Understanding `max_enrichments`

Controls the **total number of Cortex Agent API calls per sync**:

```json
// POC - Test with 5 enrichments
{"max_enrichments": "5"}
Duration: ~105 seconds (~19s per enrichment)
Cost: ~$0.0064 (0.64¢)
Coverage: 5 of 42 forecast periods (12%)

// Small production - 18 enrichments (3 ZIPs × 6 periods)
{"max_enrichments": "18"}
Duration: ~6 minutes
Cost: ~$0.023 (2.3¢)
Coverage: Next 48 hours for all locations

// Medium production - 42 enrichments (complete 7-day forecast)
{"max_enrichments": "42"}
Duration: ~14 minutes
Cost: ~$0.054 (5.4¢)
Coverage: Complete 7-day outlook for all locations
```

### Performance Metrics

Example from test run (3 ZIP codes, 5 enrichments):

```
Sync Duration: 105 seconds (1.75 minutes)
Total Records: 42 (14 periods × 3 ZIP codes)
AI Enrichments: 5 (12% of total)
Avg Time Per Enrichment: ~19 seconds
Base Sync (no AI): 2.6 records/sec
With AI (5 enrichments): 0.4 records/sec
Estimated Cost: ~$0.0064 (0.64¢)
```

**Performance factors:**
- **Weather API calls**: ~2-3 seconds per ZIP code (with retry logic and rate limiting)
- **AI enrichment**: ~15-25 seconds per forecast period (includes Cortex Agent query and SSE streaming)
- **Retry logic**: Exponential backoff for 429/5xx errors (up to 3 attempts)
- **Rate limiting**: Built-in delays (NWS: 1s, Zippopotam.us: 0.5s)

### Cost Optimization Strategies

**Strategy 1: POC/Testing (Recommended for getting started)**
```json
{"max_enrichments": "5"}
```
- Enrich first 5 forecast periods (sample across ZIPs)
- Cost: ~$0.0064 per sync
- Sync frequency: On-demand for testing

**Strategy 2: Selective Production (Recommended)**
```json
{"max_enrichments": "18"}
```
- Enrich ~6 periods per ZIP (next 48-72 hours)
- Cost: ~$0.023 per sync
- Sync every 12 hours for current intelligence
- **Best balance of cost and coverage**

**Strategy 3: Full Enrichment**
```json
{"max_enrichments": "42"}
```
- Enrich all 14 periods for all 3 ZIPs
- Cost: ~$0.054 per sync
- Sync daily for complete 7-day outlook

## 🤖 Cortex Agent Configuration

This connector requires a Cortex Agent configured in Snowflake with access to livestock health data. The connector uses the **Cortex Agent REST API** to make real-time queries during data ingestion.

### Quick Overview

**Agent Name:** Your choice (e.g., `LIVESTOCK_HEALTH_GUARDIAN_AGENT`)

**Model:** `llama3.3-70b` (excellent reasoning for complex livestock health analysis)

**Primary Tool:** Cortex Analyst with semantic view access to your livestock data

**Semantic View:** Your semantic view name (e.g., `YOUR_DATABASE.YOUR_SCHEMA.LIVESTOCK_SEMANTIC_VIEW`)

### What the Agent Does

The agent analyzes weather forecasts against current livestock health data to provide:
- Risk assessment for specific farms and animals
- Species-specific weather impact analysis
- Actionable recommendations with timeframes
- Historical weather-health correlations

### Data Requirements

The agent queries a semantic view over livestock health data containing:

| Field | Type | Description |
|-------|------|-------------|
| `RECORD_ID` | STRING | Unique record identifier |
| `ANIMAL_ID` | STRING | Unique animal identifier |
| `FARM_ID` | STRING | Farm identifier (e.g., FARM_000000) |
| `SPECIES` | STRING | Animal species (Beef Cattle, Chickens, Pigs, etc.) |
| `BREED` | STRING | Breed information |
| `AGE` | INTEGER | Animal age |
| `WEIGHT` | DECIMAL | Animal weight |
| `HEALTH_STATUS` | STRING | Current health status (Healthy, Injured, Sick, etc.) |
| `VACCINATION_HISTORY` | STRING | Vaccination records |
| `MEDICATION_HISTORY` | STRING | Medication records |
| `TEMPERATURE` | DECIMAL | Environmental temperature |
| `HUMIDITY` | DECIMAL | Environmental humidity |

### REST API Configuration

**Important:** The Cortex Agent REST API doesn't support calling pre-created agents by name. Instead, the connector:

1. Dynamically constructs the agent configuration in each API request
2. Specifies the model (`llama3.3-70b`), tools, and semantic view programmatically
3. Ensures consistent behavior

**Update the connector.py to use your semantic view:**

```python
"tool_resources": {
    "Analyst1": {
        "semantic_view": "YOUR_DATABASE.YOUR_SCHEMA.YOUR_SEMANTIC_VIEW_NAME"
    }
}
```

Your UI-created agent is useful for:
- ✅ Testing queries and validating responses in Snowflake UI
- ✅ Verifying semantic view access and permissions
- ✅ Developing and refining prompt engineering
- ✅ Demonstrating capabilities to stakeholders

## 🏗️ Technical Architecture

### Code Quality Features (v2.0.0+)

**Industry-Standard Best Practices:**
- ✅ **Module-level constants** - No magic numbers, all values configurable
- ✅ **Exponential backoff retry** - Handles 429/5xx errors automatically
- ✅ **Complete PEP 257 docstrings** - All functions fully documented with Args/Returns/Raises
- ✅ **Cyclomatic complexity < 10** - Maintainable, testable code
- ✅ **Error handling** - Specific exception types with logging
- ✅ **Proper data flattening** - Fivetran-compatible nested structure handling
- ✅ **18 unit tests** - Comprehensive test coverage

**Key Technical Components:**

1. **Retry Logic with Exponential Backoff**
```python
# Automatically retries on 429 (rate limit) and 5xx (server errors)
# Delay: 1s → 2s → 4s across 3 attempts
fetch_data_with_retry(session, url, params, headers)
```

2. **Rate Limiting**
```python
# Built-in delays to respect API limits
__NWS_RATE_LIMIT_DELAY = 1.0        # NWS requires 1 second between calls
__ZIP_API_RATE_LIMIT_DELAY = 0.5    # Zippopotam.us: 0.5 second delay
```

3. **Configuration Type Conversion**
```python
# Safe conversion with defaults from module constants
enable_cortex = config.get('enable_cortex_enrichment', 'false').lower() == 'true'
max_enrichments = int(config.get('max_enrichments', str(__DEFAULT_MAX_ENRICHMENTS)))
timeout = int(config.get('cortex_timeout', str(__DEFAULT_CORTEX_TIMEOUT)))
```

### Cortex Agent Integration

This connector uses the **Snowflake Cortex Agent REST API** with llama3.3-70b model:

**Endpoint:** `POST https://{account}/api/v2/cortex/agent:run`

**API Payload Structure:**
```python
{
    "model": "llama3.3-70b",
    "response_instruction": "You are a livestock health expert...",
    "tools": [{
        "tool_spec": {
            "type": "cortex_analyst_text_to_sql",
            "name": "Analyst1"
        }
    }],
    "tool_resources": {
        "Analyst1": {
            "semantic_view": "YOUR_DATABASE.YOUR_SCHEMA.YOUR_SEMANTIC_VIEW_NAME"
        }
    },
    "tool_choice": {"type": "auto"},
    "messages": [...]
}
```

**Response Parsing:**
- Uses Server-Sent Events (SSE) streaming
- Parses `message.delta` events with nested content
- Extracts text from `tool_results` with robust type checking
- Handles both direct text and nested JSON responses

**Why llama3.3-70b?**
- Excellent reasoning capabilities for complex livestock health analysis
- Strong performance with structured output
- Cost-effective compared to larger models
- Fast inference times for real-time enrichment

## 🧪 Testing

### Run Unit Tests

```bash
# Run all tests
python test_connector.py

# Or with pytest for detailed output
pytest test_connector.py -v

# Expected output:
# ====== 18 passed in 0.02s ======
```

### Test Coverage

**18 tests covering:**
- ✅ `flatten_dict()` - 6 tests (nested objects, arrays, nulls, deep nesting)
- ✅ `parse_farm_zip_mapping()` - 6 tests (single/multiple ZIPs, whitespace, malformed)
- ✅ `parse_agent_response()` - 5 tests (complete/partial responses, farm extraction, defaults)
- ✅ `get_coordinates_from_zip()` - 1 integration test (mocked API)

### Test Categories

**Utility Functions:**
- Data transformation (`flatten_dict`)
- Configuration parsing (`parse_farm_zip_mapping`)

**AI Integration:**
- Agent response parsing with regex farm ID extraction
- Section extraction (risk assessment, affected farms, species matrix)
- Default value handling for missing data

**API Integration:**
- Mocked external API responses
- Error handling validation

## 🆘 Troubleshooting

### Setup Issues

**"Module not found" errors:**
- Install Fivetran SDK: `pip install fivetran-connector-sdk`
- Verify Python version: `python --version` (3.8+ required)

**"Configuration file not found":**
- Create `configuration.json` from the example above
- Ensure file is in the same directory as `connector.py`

### Authentication Errors

**400 Bad Request (Cortex Agent):**
- ✅ **Fixed in v2.0.0** - Now uses correct API payload structure with llama3.3-70b
- Verify PAT token is valid and not expired
- Check Snowflake account URL format (include region)
- Ensure semantic view exists and is accessible
- Verify semantic view path in connector.py matches your setup

**401 Unauthorized (Snowflake):**
- Verify PAT token format (starts with `ver:1-hint:`)
- Generate new token in Snowflake UI if expired
- Confirm user has SELECT permissions on your livestock data tables and semantic views

**403 Forbidden (NWS):**
- NWS requires User-Agent header with contact email
- Format: `"AppName/Version (email@domain.com)"`

**404 Not Found (ZIP code):**
- Verify ZIP code is valid US ZIP (5 digits)
- Some ZIPs may not be in Zippopotam.us database
- Connector logs warning and continues with other ZIPs

### Data Issues

**No AI enrichments appearing:**
- Verify `enable_cortex_enrichment` is `"true"` (string, not boolean)
- Check Snowflake account URL includes full domain
- Verify PAT token hasn't expired
- Confirm `farm_zip_mapping` is configured
- Check `max_enrichments` > 0
- Review logs for Cortex timeout errors
- **Most common**: Update semantic view path in connector.py to match your database/schema

**Empty or default agent responses:**
- Agent may not have access to your livestock data table
- Verify semantic view is correctly configured
- Check farm IDs exist in your livestock data table
- Consider increasing `cortex_timeout` (default: 60s)

**Farm mapping not working:**
- Format must be exact: `"ZIP:FARM1,FARM2|ZIP:FARM3"`
- No spaces around colons or pipes
- Farm IDs must match your data exactly (case-sensitive)

### Performance Issues

**Slow sync speed:**
- Normal with AI enrichment (~15-25s per period)
- Reduce `max_enrichments` for faster testing
- Check network latency to Snowflake
- Verify Snowflake warehouse is running and has capacity

**Agent timeouts:**
- Increase `cortex_timeout` from 60s to 90s or 120s
- Agent may be querying large data table
- Check Snowflake compute resources
- Consider optimizing semantic view or adding indexes

**Rate limiting errors:**
- Connector has built-in rate limiting (NWS: 1s, Zippopotam.us: 0.5s)
- Exponential backoff handles 429 errors automatically
- If persistent, reduce sync frequency

## 🔄 Sync Behavior

**Important:** This connector performs **full refresh** on each sync:
- Weather forecasts are time-sensitive and constantly updated
- NWS forecasts represent point-in-time predictions
- Full refresh ensures latest data without stale records
- No incremental state tracking needed

**Recommended Sync Frequency:**
- **With AI enrichment (max_enrichments=18)**: Every 12 hours
- **With AI enrichment (max_enrichments=42)**: Every 24 hours
- **Without AI enrichment**: Every 6 hours
- NWS updates forecasts approximately every 6 hours

## 📈 Upgrading from POC to Production

### Phase 1: POC with Limited Enrichment

```json
{
  "zip_codes": "77429,77856,81611",
  "max_enrichments": "5",
  "enable_cortex_enrichment": "true"
}
```

- 5 enrichments per sync (sample across ZIPs)
- Cost: ~$0.0064 (0.64¢) per sync
- Duration: ~105 seconds
- **Use case**: Testing, validation, demo

### Phase 2: Production - Selective Enrichment (Recommended)

```json
{
  "zip_codes": "77429,77856,81611,90210,10001",
  "max_enrichments": "30",
  "enable_cortex_enrichment": "true"
}
```

- Enrich ~6 periods per ZIP (next 72 hours)
- Cost: ~$0.038 per sync
- Duration: ~10 minutes per sync
- Run every 12 hours
- **Use case**: Operational farm management

### Phase 3: Production - Full Enrichment

```json
{
  "zip_codes": "77429,77856,81611,90210,10001,60601,33101,98101,02108,94102",
  "max_enrichments": "140",
  "enable_cortex_enrichment": "true"
}
```

- Full 7-day forecast for 10 farm locations
- Cost: ~$0.18 per sync
- Duration: ~45 minutes per sync
- Run daily
- **Use case**: Enterprise farm network

## 🗂️ File Structure

```
.
├── connector.py                 # Main connector (849 lines, production-ready)
├── test_connector.py           # Unit tests (18 tests, all passing)
├── configuration.json.example  # Configuration template
├── accounts.json.example       # Accounts template
├── requirements.txt            # Dependencies (empty - all pre-installed)
└── README.md                   # This file
```

## 🔒 Security Best Practices

- ✅ Never commit `configuration.json` or `accounts.json` with real credentials
- ✅ Use `.gitignore` to exclude sensitive files
- ✅ Rotate Snowflake PAT tokens regularly (every 90 days recommended)
- ✅ Use separate Fivetran accounts for prod/staging/dev environments
- ✅ Monitor API usage and access logs in Snowflake
- ✅ User-Agent must include contact email (NWS requirement)
- ✅ Use least-privilege permissions for PAT users (SELECT only on required tables)
- ✅ Store credentials in environment variables or secrets manager in production

## 📚 API Documentation

- **Zippopotam.us:** https://zippopotam.us/ (Free geocoding for US ZIP codes)
- **National Weather Service:** https://www.weather.gov/documentation/services-web-api (Public weather data)
- **Snowflake Cortex Agents REST API:** https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents-rest-api
- **Fivetran Connector SDK:** https://fivetran.com/docs/connector-sdk (Custom connector development)

## 🎯 Real-World Use Cases

### Use Case 1: Proactive Health Management
Farm manager receives 24-48 hour advance warning of dangerous weather conditions affecting specific animals, allowing preventive measures that reduce veterinary costs by 60%+.

**Example:** Cold front forecast triggers automated analysis showing 15 injured cattle are vulnerable. Manager moves them indoors 6 hours before temperature drops, preventing respiratory infections that historically cost $450/animal to treat.

### Use Case 2: Emergency Preparedness
Automated alerts for extreme weather (heat waves, cold snaps, storms) with species-specific action plans based on current herd health status.

**Example:** Heat wave alert triggers species-specific recommendations: increase shade for beef cattle (high risk), add cooling for pigs (medium risk), standard protocols for chickens (low risk).

### Use Case 3: Historical Analysis
Track correlations between weather patterns and livestock health outcomes to improve future decision-making and resource allocation.

**Example:** Analysis shows 35% increase in respiratory issues following similar weather patterns in December 2024, informing this year's preparation strategy.

### Use Case 4: Insurance Documentation
Detailed weather and health risk documentation for insurance claims related to weather-related livestock losses.

**Example:** Complete audit trail showing weather forecasts, AI-generated risk assessments, actions taken, and outcomes for insurance claim substantiation.

### Use Case 5: Regulatory Compliance
Demonstrate proactive animal welfare management with documented weather monitoring and preventive action plans.

**Example:** Documented evidence of weather monitoring, risk assessment, and preventive actions for USDA animal welfare compliance audits.

## 📝 Changelog

### Version 2.0.0 (2026-01-07)
**Major Code Quality Improvements:**
- ✅ **Refactored for Fivetran contribution standards**
- ✅ **12 module-level constants** - Eliminated all magic numbers
- ✅ **Exponential backoff retry logic** - Handles 429/5xx errors (3 retries with 1s→2s→4s delays)
- ✅ **Complete PEP 257 docstrings** - All functions fully documented with Args/Returns/Raises
- ✅ **Cyclomatic complexity < 10** - All functions maintainable and testable
- ✅ **Enhanced error handling** - Specific exception types with comprehensive logging
- ✅ **18 comprehensive unit tests** - All passing, covering core functionality
- ✅ **Improved SSE parsing** - Robust type checking for nested Cortex Agent responses
- ✅ **Better configuration handling** - Safe type conversion with defaults
- ✅ **Production-ready logging** - No logs inside loops, proper severity levels

**Functional Improvements:**
- ✅ **Fixed Cortex Agent API integration** - Correct payload structure with llama3.3-70b
- ✅ **Improved regex farm ID extraction** - Handles various response formats
- ✅ **Better default values** - Meaningful messages when enrichment not performed
- ✅ **Rate limiting built-in** - Respects API limits (NWS: 1s, Zippopotam.us: 0.5s)

**Testing:**
- ✅ **18 unit tests** - flatten_dict (6), parse_farm_zip_mapping (6), parse_agent_response (5), API integration (1)
- ✅ **Test coverage** - Core utilities, AI integration, API mocking
- ✅ **All tests passing** - Validated on Python 3.12.8

### Version 1.0.0 (2025-10-14)
- ✓ Initial release with weather forecast sync
- ✓ Real-time AI enrichment via Snowflake Cortex Agent REST API
- ✓ Farm-to-ZIP mapping configuration
- ✓ Livestock health risk assessment
- ✓ Configurable enrichment limits

## 🙏 Acknowledgments

- **National Weather Service** - Public weather forecast data (US Government)
- **Snowflake Cortex AI** - Enterprise-grade AI enrichment platform
- **Fivetran** - Modern data movement platform and connector SDK

## 📄 License

This connector is provided as-is for use with the Fivetran Connector SDK. Weather data from the National Weather Service is public domain (US Government).

## 🤝 Contributing

Contributions are welcome! This connector follows Fivetran's coding standards:

**Before contributing:**
1. Read Python coding standards in Fivetran documentation
2. Ensure all functions have complete PEP 257 docstrings
3. Keep cyclomatic complexity < 10 per function
4. Use module-level constants (no magic numbers)
5. Add unit tests for new functionality
6. Run tests: `pytest test_connector.py -v`

**To contribute:**
1. Fork the repository
2. Create a feature branch: `git checkout -b feature/your-feature`
3. Make your changes following the coding standards
4. Test thoroughly: `fivetran debug --configuration configuration.json`
5. Submit a pull request with clear description

## 🆘 Support

- **Fivetran SDK:** https://fivetran.com/docs/connector-sdk
- **Fivetran Support:** https://support.fivetran.com
- **Snowflake Cortex:** https://docs.snowflake.com/en/user-guide/snowflake-cortex
- **This Connector:** Open an issue in the repository
