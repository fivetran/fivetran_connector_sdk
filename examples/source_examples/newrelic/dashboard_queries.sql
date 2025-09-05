-- dashboard_queries.sql - Sample queries for building dashboards with New Relic Feature APIs Connector data

-- ============================================================================
-- EXECUTIVE PERFORMANCE SUMMARY QUERIES
-- ============================================================================

-- Executive Performance Summary (Last 30 Days)
SELECT 
    DATE_TRUNC('day', timestamp::timestamp) as date,
    COUNT(DISTINCT transaction_name) as active_transactions,
    AVG(duration) as avg_response_time,
    AVG(error_rate) as avg_error_rate,
    AVG(throughput) as avg_throughput,
    AVG(apdex_score) as avg_apdex_score
FROM apm_data 
WHERE timestamp::timestamp >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY DATE_TRUNC('day', timestamp::timestamp)
ORDER BY date DESC;

-- Application Performance Overview (Current Day)
SELECT 
    transaction_name,
    AVG(duration) as avg_duration,
    AVG(error_rate) as avg_error_rate,
    AVG(throughput) as avg_throughput,
    AVG(apdex_score) as avg_apdex_score,
    COUNT(*) as data_points
FROM apm_data 
WHERE DATE_TRUNC('day', timestamp::timestamp) = CURRENT_DATE
GROUP BY transaction_name
ORDER BY avg_duration DESC
LIMIT 20;

-- Infrastructure Health Summary
SELECT 
    DATE_TRUNC('day', timestamp::timestamp) as date,
    COUNT(DISTINCT host_name) as total_hosts,
    COUNT(CASE WHEN reporting = true THEN 1 END) as reporting_hosts,
    COUNT(CASE WHEN reporting = false THEN 1 END) as non_reporting_hosts,
    COUNT(DISTINCT domain) as domains,
    COUNT(DISTINCT type) as host_types
FROM infrastructure_data 
WHERE timestamp::timestamp >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY DATE_TRUNC('day', timestamp::timestamp)
ORDER BY date DESC;

-- ============================================================================
-- APM PERFORMANCE ANALYSIS
-- ============================================================================

-- Transaction Performance Trends (Daily)
SELECT 
    DATE_TRUNC('day', timestamp::timestamp) as date,
    transaction_name,
    AVG(duration) as avg_duration,
    AVG(error_rate) as avg_error_rate,
    AVG(throughput) as avg_throughput,
    AVG(apdex_score) as avg_apdex_score,
    LAG(AVG(duration), 1) OVER (PARTITION BY transaction_name ORDER BY DATE_TRUNC('day', timestamp::timestamp)) as prev_day_duration,
    CASE 
        WHEN LAG(AVG(duration), 1) OVER (PARTITION BY transaction_name ORDER BY DATE_TRUNC('day', timestamp::timestamp)) > 0 
        THEN (AVG(duration) - LAG(AVG(duration), 1) OVER (PARTITION BY transaction_name ORDER BY DATE_TRUNC('day', timestamp::timestamp))) / 
             LAG(AVG(duration), 1) OVER (PARTITION BY transaction_name ORDER BY DATE_TRUNC('day', timestamp::timestamp)) * 100 
        ELSE 0 
    END as duration_change_percent
FROM apm_data 
WHERE timestamp::timestamp >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY DATE_TRUNC('day', timestamp::timestamp), transaction_name
ORDER BY date DESC, avg_duration DESC;

-- Top Performing Transactions
SELECT 
    transaction_name,
    AVG(duration) as avg_duration,
    AVG(error_rate) as avg_error_rate,
    AVG(throughput) as avg_throughput,
    AVG(apdex_score) as avg_apdex_score,
    COUNT(*) as data_points,
    MIN(duration) as min_duration,
    MAX(duration) as max_duration
FROM apm_data 
WHERE timestamp::timestamp >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY transaction_name
HAVING COUNT(*) >= 10  -- Only transactions with sufficient data
ORDER BY avg_apdex_score DESC
LIMIT 20;

-- Error Rate Analysis
SELECT 
    transaction_name,
    AVG(error_rate) as avg_error_rate,
    MAX(error_rate) as max_error_rate,
    COUNT(CASE WHEN error_rate > 0.05 THEN 1 END) as high_error_occurrences,
    COUNT(*) as total_occurrences,
    (COUNT(CASE WHEN error_rate > 0.05 THEN 1 END) * 100.0 / COUNT(*)) as error_percentage
FROM apm_data 
WHERE timestamp::timestamp >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY transaction_name
HAVING COUNT(*) >= 5
ORDER BY avg_error_rate DESC
LIMIT 15;

-- ============================================================================
-- INFRASTRUCTURE MONITORING ANALYSIS
-- ============================================================================

-- Host Status Summary
SELECT 
    domain,
    type,
    COUNT(*) as total_hosts,
    COUNT(CASE WHEN reporting = true THEN 1 END) as reporting_hosts,
    COUNT(CASE WHEN reporting = false THEN 1 END) as non_reporting_hosts,
    (COUNT(CASE WHEN reporting = true THEN 1 END) * 100.0 / COUNT(*)) as reporting_percentage
FROM infrastructure_data 
WHERE timestamp::timestamp >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY domain, type
ORDER BY total_hosts DESC;

-- Non-Reporting Hosts Alert
SELECT 
    host_name,
    domain,
    type,
    timestamp::timestamp as last_seen,
    EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - timestamp::timestamp)) / 3600 as hours_since_last_report
FROM infrastructure_data 
WHERE reporting = false 
  AND timestamp::timestamp >= CURRENT_DATE - INTERVAL '24 hours'
ORDER BY hours_since_last_report DESC;

-- Infrastructure by Domain
SELECT 
    domain,
    COUNT(DISTINCT host_name) as host_count,
    COUNT(DISTINCT type) as host_types,
    COUNT(CASE WHEN reporting = true THEN 1 END) as active_hosts,
    COUNT(CASE WHEN reporting = false THEN 1 END) as inactive_hosts
FROM infrastructure_data 
WHERE timestamp::timestamp >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY domain
ORDER BY host_count DESC;

-- ============================================================================
-- BROWSER PERFORMANCE ANALYSIS
-- ============================================================================

-- Browser Performance Summary
SELECT 
    browser_name,
    browser_version,
    COUNT(*) as page_views,
    AVG(load_time) as avg_load_time,
    AVG(dom_content_loaded) as avg_dom_content_loaded,
    COUNT(DISTINCT page_url) as unique_pages,
    COUNT(DISTINCT device_type) as device_types
FROM browser_data 
WHERE timestamp::timestamp >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY browser_name, browser_version
ORDER BY page_views DESC;

-- Page Performance Analysis
SELECT 
    page_url,
    COUNT(*) as page_views,
    AVG(load_time) as avg_load_time,
    AVG(dom_content_loaded) as avg_dom_content_loaded,
    COUNT(DISTINCT browser_name) as browsers_used,
    COUNT(DISTINCT device_type) as device_types,
    MIN(load_time) as min_load_time,
    MAX(load_time) as max_load_time
FROM browser_data 
WHERE timestamp::timestamp >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY page_url
HAVING COUNT(*) >= 5
ORDER BY avg_load_time DESC
LIMIT 20;

-- Device Performance Comparison
SELECT 
    device_type,
    COUNT(*) as page_views,
    AVG(load_time) as avg_load_time,
    AVG(dom_content_loaded) as avg_dom_content_loaded,
    COUNT(DISTINCT browser_name) as browsers_used,
    COUNT(DISTINCT page_url) as pages_accessed
FROM browser_data 
WHERE timestamp::timestamp >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY device_type
ORDER BY avg_load_time;

-- ============================================================================
-- MOBILE APP PERFORMANCE ANALYSIS
-- ============================================================================

-- Mobile App Performance Summary
SELECT 
    app_name,
    platform,
    version,
    COUNT(*) as data_points,
    AVG(crash_count) as avg_crash_count,
    COUNT(DISTINCT device_model) as device_models,
    COUNT(DISTINCT os_version) as os_versions
FROM mobile_data 
WHERE timestamp::timestamp >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY app_name, platform, version
ORDER BY avg_crash_count DESC;

-- Crash Analysis by Platform
SELECT 
    platform,
    app_name,
    AVG(crash_count) as avg_crash_count,
    MAX(crash_count) as max_crash_count,
    COUNT(*) as data_points,
    COUNT(CASE WHEN crash_count > 0 THEN 1 END) as crash_occurrences,
    (COUNT(CASE WHEN crash_count > 0 THEN 1 END) * 100.0 / COUNT(*)) as crash_percentage
FROM mobile_data 
WHERE timestamp::timestamp >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY platform, app_name
HAVING COUNT(*) >= 5
ORDER BY avg_crash_count DESC;

-- Device and OS Analysis
SELECT 
    device_model,
    os_version,
    COUNT(*) as data_points,
    AVG(crash_count) as avg_crash_count,
    COUNT(DISTINCT app_name) as apps_using
FROM mobile_data 
WHERE timestamp::timestamp >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY device_model, os_version
HAVING COUNT(*) >= 3
ORDER BY avg_crash_count DESC
LIMIT 20;

-- ============================================================================
-- SYNTHETIC MONITORING ANALYSIS
-- ============================================================================

-- Synthetic Monitor Status Summary
SELECT 
    monitor_name,
    monitor_type,
    COUNT(*) as checks_performed,
    COUNT(CASE WHEN status = 'SUCCESS' THEN 1 END) as successful_checks,
    COUNT(CASE WHEN status != 'SUCCESS' THEN 1 END) as failed_checks,
    AVG(response_time) as avg_response_time,
    (COUNT(CASE WHEN status = 'SUCCESS' THEN 1 END) * 100.0 / COUNT(*)) as success_rate
FROM synthetic_data 
WHERE timestamp::timestamp >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY monitor_name, monitor_type
ORDER BY success_rate ASC;

-- Failed Monitor Analysis
SELECT 
    monitor_name,
    monitor_type,
    location,
    status,
    response_time,
    error_message,
    timestamp::timestamp as failure_time
FROM synthetic_data 
WHERE status != 'SUCCESS' 
  AND timestamp::timestamp >= CURRENT_DATE - INTERVAL '24 hours'
ORDER BY timestamp::timestamp DESC;

-- Monitor Performance by Location
SELECT 
    location,
    monitor_type,
    COUNT(*) as total_checks,
    COUNT(CASE WHEN status = 'SUCCESS' THEN 1 END) as successful_checks,
    AVG(response_time) as avg_response_time,
    MAX(response_time) as max_response_time,
    MIN(response_time) as min_response_time,
    (COUNT(CASE WHEN status = 'SUCCESS' THEN 1 END) * 100.0 / COUNT(*)) as success_rate
FROM synthetic_data 
WHERE timestamp::timestamp >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY location, monitor_type
ORDER BY success_rate ASC;

-- ============================================================================
-- CROSS-SERVICE PERFORMANCE ANALYSIS
-- ============================================================================

-- Performance Correlation Analysis
WITH daily_metrics AS (
    SELECT 
        DATE_TRUNC('day', timestamp::timestamp) as date,
        'APM' as service_type,
        AVG(duration) as avg_response_time,
        AVG(error_rate) as avg_error_rate
    FROM apm_data 
    WHERE timestamp::timestamp >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY DATE_TRUNC('day', timestamp::timestamp)
    
    UNION ALL
    
    SELECT 
        DATE_TRUNC('day', timestamp::timestamp) as date,
        'Browser' as service_type,
        AVG(load_time) as avg_response_time,
        0 as avg_error_rate
    FROM browser_data 
    WHERE timestamp::timestamp >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY DATE_TRUNC('day', timestamp::timestamp)
    
    UNION ALL
    
    SELECT 
        DATE_TRUNC('day', timestamp::timestamp) as date,
        'Synthetic' as service_type,
        AVG(response_time) as avg_response_time,
        (COUNT(CASE WHEN status != 'SUCCESS' THEN 1 END) * 100.0 / COUNT(*)) as avg_error_rate
    FROM synthetic_data 
    WHERE timestamp::timestamp >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY DATE_TRUNC('day', timestamp::timestamp)
)
SELECT 
    date,
    service_type,
    avg_response_time,
    avg_error_rate
FROM daily_metrics
ORDER BY date DESC, service_type;

-- ============================================================================
-- ALERTING AND THRESHOLD ANALYSIS
-- ============================================================================

-- High Error Rate Alerts
SELECT 
    'APM' as service_type,
    transaction_name as entity_name,
    AVG(error_rate) as current_error_rate,
    'High error rate detected' as alert_message,
    timestamp::timestamp as last_updated
FROM apm_data 
WHERE timestamp::timestamp >= CURRENT_DATE - INTERVAL '1 hour'
GROUP BY transaction_name, timestamp::timestamp
HAVING AVG(error_rate) > 0.05

UNION ALL

SELECT 
    'Mobile' as service_type,
    app_name as entity_name,
    AVG(crash_count) as current_crash_count,
    'High crash rate detected' as alert_message,
    timestamp::timestamp as last_updated
FROM mobile_data 
WHERE timestamp::timestamp >= CURRENT_DATE - INTERVAL '1 hour'
GROUP BY app_name, timestamp::timestamp
HAVING AVG(crash_count) > 5

UNION ALL

SELECT 
    'Synthetic' as service_type,
    monitor_name as entity_name,
    (COUNT(CASE WHEN status != 'SUCCESS' THEN 1 END) * 100.0 / COUNT(*)) as failure_rate,
    'Monitor failures detected' as alert_message,
    timestamp::timestamp as last_updated
FROM synthetic_data 
WHERE timestamp::timestamp >= CURRENT_DATE - INTERVAL '1 hour'
GROUP BY monitor_name, timestamp::timestamp
HAVING (COUNT(CASE WHEN status != 'SUCCESS' THEN 1 END) * 100.0 / COUNT(*)) > 20;

-- Performance Degradation Detection
SELECT 
    transaction_name,
    DATE_TRUNC('hour', timestamp::timestamp) as hour,
    AVG(duration) as current_avg_duration,
    LAG(AVG(duration), 1) OVER (PARTITION BY transaction_name ORDER BY DATE_TRUNC('hour', timestamp::timestamp)) as previous_hour_duration,
    CASE 
        WHEN LAG(AVG(duration), 1) OVER (PARTITION BY transaction_name ORDER BY DATE_TRUNC('hour', timestamp::timestamp)) > 0 
        THEN (AVG(duration) - LAG(AVG(duration), 1) OVER (PARTITION BY transaction_name ORDER BY DATE_TRUNC('hour', timestamp::timestamp))) / 
             LAG(AVG(duration), 1) OVER (PARTITION BY transaction_name ORDER BY DATE_TRUNC('hour', timestamp::timestamp)) * 100 
        ELSE 0 
    END as performance_change_percent
FROM apm_data 
WHERE timestamp::timestamp >= CURRENT_DATE - INTERVAL '24 hours'
GROUP BY transaction_name, DATE_TRUNC('hour', timestamp::timestamp)
HAVING COUNT(*) >= 5  -- Ensure sufficient data points
ORDER BY performance_change_percent DESC
LIMIT 20;

-- ============================================================================
-- TREND ANALYSIS AND FORECASTING
-- ============================================================================

-- Weekly Performance Trends
SELECT 
    DATE_TRUNC('week', timestamp::timestamp) as week,
    'APM' as service_type,
    AVG(duration) as avg_response_time,
    AVG(error_rate) as avg_error_rate,
    AVG(apdex_score) as avg_apdex_score
FROM apm_data 
WHERE timestamp::timestamp >= CURRENT_DATE - INTERVAL '12 weeks'
GROUP BY DATE_TRUNC('week', timestamp::timestamp)

UNION ALL

SELECT 
    DATE_TRUNC('week', timestamp::timestamp) as week,
    'Browser' as service_type,
    AVG(load_time) as avg_response_time,
    0 as avg_error_rate,
    0 as avg_apdex_score
FROM browser_data 
WHERE timestamp::timestamp >= CURRENT_DATE - INTERVAL '12 weeks'
GROUP BY DATE_TRUNC('week', timestamp::timestamp)

UNION ALL

SELECT 
    DATE_TRUNC('week', timestamp::timestamp) as week,
    'Synthetic' as service_type,
    AVG(response_time) as avg_response_time,
    (COUNT(CASE WHEN status != 'SUCCESS' THEN 1 END) * 100.0 / COUNT(*)) as avg_error_rate,
    0 as avg_apdex_score
FROM synthetic_data 
WHERE timestamp::timestamp >= CURRENT_DATE - INTERVAL '12 weeks'
GROUP BY DATE_TRUNC('week', timestamp::timestamp)
ORDER BY week DESC, service_type;

-- ============================================================================
-- COMPLIANCE AND AUDITING
-- ============================================================================

-- Daily Performance Summary for Auditing
SELECT 
    DATE_TRUNC('day', timestamp::timestamp) as date,
    'APM' as data_source,
    COUNT(DISTINCT transaction_name) as entities_monitored,
    AVG(duration) as avg_response_time,
    AVG(error_rate) as avg_error_rate,
    COUNT(*) as data_points
FROM apm_data 
WHERE timestamp::timestamp >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY DATE_TRUNC('day', timestamp::timestamp)

UNION ALL

SELECT 
    DATE_TRUNC('day', timestamp::timestamp) as date,
    'Infrastructure' as data_source,
    COUNT(DISTINCT host_name) as entities_monitored,
    0 as avg_response_time,
    0 as avg_error_rate,
    COUNT(*) as data_points
FROM infrastructure_data 
WHERE timestamp::timestamp >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY DATE_TRUNC('day', timestamp::timestamp)

UNION ALL

SELECT 
    DATE_TRUNC('day', timestamp::timestamp) as date,
    'Browser' as data_source,
    COUNT(DISTINCT page_url) as entities_monitored,
    AVG(load_time) as avg_response_time,
    0 as avg_error_rate,
    COUNT(*) as data_points
FROM browser_data 
WHERE timestamp::timestamp >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY DATE_TRUNC('day', timestamp::timestamp)

UNION ALL

SELECT 
    DATE_TRUNC('day', timestamp::timestamp) as date,
    'Mobile' as data_source,
    COUNT(DISTINCT app_name) as entities_monitored,
    0 as avg_response_time,
    AVG(crash_count) as avg_error_rate,
    COUNT(*) as data_points
FROM mobile_data 
WHERE timestamp::timestamp >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY DATE_TRUNC('day', timestamp::timestamp)

UNION ALL

SELECT 
    DATE_TRUNC('day', timestamp::timestamp) as date,
    'Synthetic' as data_source,
    COUNT(DISTINCT monitor_name) as entities_monitored,
    AVG(response_time) as avg_response_time,
    (COUNT(CASE WHEN status != 'SUCCESS' THEN 1 END) * 100.0 / COUNT(*)) as avg_error_rate,
    COUNT(*) as data_points
FROM synthetic_data 
WHERE timestamp::timestamp >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY DATE_TRUNC('day', timestamp::timestamp)
ORDER BY date DESC, data_source;

-- Data Quality Assessment
SELECT 
    'APM' as table_name,
    COUNT(*) as total_records,
    COUNT(CASE WHEN transaction_name IS NOT NULL AND transaction_name != '' THEN 1 END) as valid_transaction_names,
    COUNT(CASE WHEN duration > 0 THEN 1 END) as valid_durations,
    COUNT(CASE WHEN error_rate >= 0 AND error_rate <= 1 THEN 1 END) as valid_error_rates,
    (COUNT(CASE WHEN transaction_name IS NOT NULL AND transaction_name != '' THEN 1 END) * 100.0 / COUNT(*)) as data_quality_score
FROM apm_data 
WHERE timestamp::timestamp >= CURRENT_DATE - INTERVAL '7 days'

UNION ALL

SELECT 
    'Infrastructure' as table_name,
    COUNT(*) as total_records,
    COUNT(CASE WHEN host_name IS NOT NULL AND host_name != '' THEN 1 END) as valid_host_names,
    COUNT(CASE WHEN domain IS NOT NULL AND domain != '' THEN 1 END) as valid_domains,
    COUNT(CASE WHEN type IS NOT NULL AND type != '' THEN 1 END) as valid_types,
    (COUNT(CASE WHEN host_name IS NOT NULL AND host_name != '' THEN 1 END) * 100.0 / COUNT(*)) as data_quality_score
FROM infrastructure_data 
WHERE timestamp::timestamp >= CURRENT_DATE - INTERVAL '7 days'

UNION ALL

SELECT 
    'Browser' as table_name,
    COUNT(*) as total_records,
    COUNT(CASE WHEN page_url IS NOT NULL AND page_url != '' THEN 1 END) as valid_urls,
    COUNT(CASE WHEN browser_name IS NOT NULL AND browser_name != '' THEN 1 END) as valid_browsers,
    COUNT(CASE WHEN load_time > 0 THEN 1 END) as valid_load_times,
    (COUNT(CASE WHEN page_url IS NOT NULL AND page_url != '' THEN 1 END) * 100.0 / COUNT(*)) as data_quality_score
FROM browser_data 
WHERE timestamp::timestamp >= CURRENT_DATE - INTERVAL '7 days'

UNION ALL

SELECT 
    'Mobile' as table_name,
    COUNT(*) as total_records,
    COUNT(CASE WHEN app_name IS NOT NULL AND app_name != '' THEN 1 END) as valid_app_names,
    COUNT(CASE WHEN platform IS NOT NULL AND platform != '' THEN 1 END) as valid_platforms,
    COUNT(CASE WHEN crash_count >= 0 THEN 1 END) as valid_crash_counts,
    (COUNT(CASE WHEN app_name IS NOT NULL AND app_name != '' THEN 1 END) * 100.0 / COUNT(*)) as data_quality_score
FROM mobile_data 
WHERE timestamp::timestamp >= CURRENT_DATE - INTERVAL '7 days'

UNION ALL

SELECT 
    'Synthetic' as table_name,
    COUNT(*) as total_records,
    COUNT(CASE WHEN monitor_name IS NOT NULL AND monitor_name != '' THEN 1 END) as valid_monitor_names,
    COUNT(CASE WHEN monitor_type IS NOT NULL AND monitor_type != '' THEN 1 END) as valid_monitor_types,
    COUNT(CASE WHEN response_time > 0 THEN 1 END) as valid_response_times,
    (COUNT(CASE WHEN monitor_name IS NOT NULL AND monitor_name != '' THEN 1 END) * 100.0 / COUNT(*)) as data_quality_score
FROM synthetic_data 
WHERE timestamp::timestamp >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY data_quality_score DESC;
