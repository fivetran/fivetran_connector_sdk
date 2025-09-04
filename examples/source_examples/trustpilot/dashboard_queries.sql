-- dashboard_queries.sql - Sample queries for building dashboards with Trustpilot API Connector data

-- ============================================================================
-- EXECUTIVE REPUTATION SUMMARY QUERIES
-- ============================================================================

-- Executive Reputation Summary (Last 30 Days)
SELECT 
    DATE_TRUNC('day', timestamp::timestamp) as date,
    COUNT(DISTINCT review_id) as total_reviews,
    AVG(stars) as avg_rating,
    COUNT(CASE WHEN stars >= 4 THEN 1 END) as positive_reviews,
    COUNT(CASE WHEN stars <= 2 THEN 1 END) as negative_reviews,
    (COUNT(CASE WHEN stars >= 4 THEN 1 END) * 100.0 / COUNT(*)) as positive_percentage
FROM reviews 
WHERE timestamp::timestamp >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY DATE_TRUNC('day', timestamp::timestamp)
ORDER BY date DESC;

-- Business Reputation Overview (Current Day)
SELECT 
    bu.name as business_name,
    bu.trust_score,
    bu.stars as avg_rating,
    bu.number_of_reviews,
    COUNT(r.review_id) as reviews_today,
    AVG(r.stars) as today_avg_rating
FROM business_units bu
LEFT JOIN reviews r ON bu.business_unit_id = r.business_unit_id
WHERE DATE_TRUNC('day', r.timestamp::timestamp) = CURRENT_DATE
GROUP BY bu.business_unit_id, bu.name, bu.trust_score, bu.stars, bu.number_of_reviews
ORDER BY bu.trust_score DESC;

-- ============================================================================
-- REVIEW ANALYSIS QUERIES
-- ============================================================================

-- Review Performance Trends (Daily)
SELECT 
    DATE_TRUNC('day', timestamp::timestamp) as date,
    AVG(stars) as avg_rating,
    COUNT(*) as review_count,
    COUNT(CASE WHEN is_verified = true THEN 1 END) as verified_reviews,
    COUNT(CASE WHEN helpful_count > 0 THEN 1 END) as helpful_reviews,
    AVG(helpful_count) as avg_helpful_count
FROM reviews 
WHERE timestamp::timestamp >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY DATE_TRUNC('day', timestamp::timestamp)
ORDER BY date DESC;

-- Top Performing Reviews
SELECT 
    review_id,
    title,
    stars,
    helpful_count,
    is_verified,
    created_at::timestamp as review_date,
    consumer_name
FROM reviews 
WHERE timestamp::timestamp >= CURRENT_DATE - INTERVAL '7 days'
  AND stars >= 4
  AND helpful_count > 0
ORDER BY helpful_count DESC, stars DESC
LIMIT 20;

-- Review Sentiment Analysis
SELECT 
    CASE 
        WHEN stars = 5 THEN 'Excellent (5 stars)'
        WHEN stars = 4 THEN 'Good (4 stars)'
        WHEN stars = 3 THEN 'Average (3 stars)'
        WHEN stars = 2 THEN 'Poor (2 stars)'
        WHEN stars = 1 THEN 'Very Poor (1 star)'
    END as rating_category,
    COUNT(*) as review_count,
    (COUNT(*) * 100.0 / (SELECT COUNT(*) FROM reviews WHERE timestamp::timestamp >= CURRENT_DATE - INTERVAL '30 days')) as percentage,
    AVG(helpful_count) as avg_helpful_count,
    COUNT(CASE WHEN is_verified = true THEN 1 END) as verified_count
FROM reviews 
WHERE timestamp::timestamp >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY stars
ORDER BY stars DESC;

-- ============================================================================
-- BUSINESS INTELLIGENCE QUERIES
-- ============================================================================

-- Business Performance Metrics
SELECT 
    bu.name,
    bu.display_name,
    bu.trust_score,
    bu.stars as avg_rating,
    bu.number_of_reviews,
    bu.country_code,
    bu.language,
    bu.created_at::timestamp as business_created,
    bu.updated_at::timestamp as last_updated
FROM business_units bu
ORDER BY bu.trust_score DESC;

-- Category Performance Analysis
SELECT 
    c.name as category_name,
    c.localized_name,
    c.level,
    COUNT(DISTINCT bu.business_unit_id) as business_count,
    AVG(bu.trust_score) as avg_trust_score,
    AVG(bu.stars) as avg_rating
FROM categories c
LEFT JOIN business_units bu ON c.category_id = bu.category_id
WHERE bu.business_unit_id IS NOT NULL
GROUP BY c.category_id, c.name, c.localized_name, c.level
ORDER BY avg_trust_score DESC;

-- ============================================================================
-- CONSUMER ANALYSIS QUERIES
-- ============================================================================

-- Consumer Engagement Summary
SELECT 
    DATE_TRUNC('day', timestamp::timestamp) as date,
    COUNT(DISTINCT consumer_id) as unique_consumers,
    COUNT(DISTINCT business_unit_id) as businesses_reviewed,
    COUNT(*) as total_reviews,
    AVG(stars) as avg_rating,
    COUNT(CASE WHEN is_verified = true THEN 1 END) as verified_reviews
FROM reviews 
WHERE timestamp::timestamp >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY DATE_TRUNC('day', timestamp::timestamp)
ORDER BY date DESC;

-- Top Reviewers
SELECT 
    r.consumer_name,
    COUNT(r.review_id) as review_count,
    AVG(r.stars) as avg_rating,
    COUNT(CASE WHEN r.is_verified = true THEN 1 END) as verified_reviews,
    SUM(r.helpful_count) as total_helpful_votes,
    MAX(r.created_at::timestamp) as last_review_date
FROM reviews r
WHERE r.timestamp::timestamp >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY r.consumer_id, r.consumer_name
HAVING COUNT(r.review_id) >= 3
ORDER BY review_count DESC, avg_rating DESC
LIMIT 20;

-- Consumer Demographics
SELECT 
    c.country_code,
    c.language,
    COUNT(DISTINCT c.consumer_id) as consumer_count,
    COUNT(r.review_id) as review_count,
    AVG(r.stars) as avg_rating,
    COUNT(CASE WHEN c.is_verified = true THEN 1 END) as verified_consumers
FROM consumers c
LEFT JOIN reviews r ON c.consumer_id = r.consumer_id AND c.business_unit_id = r.business_unit_id
WHERE c.timestamp::timestamp >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY c.country_code, c.language
ORDER BY consumer_count DESC;

-- ============================================================================
-- INVITATION MANAGEMENT QUERIES
-- ============================================================================

-- Invitation Campaign Performance
SELECT 
    DATE_TRUNC('day', timestamp::timestamp) as date,
    type as invitation_type,
    COUNT(*) as invitations_sent,
    COUNT(CASE WHEN status = 'RESPONDED' THEN 1 END) as responses_received,
    COUNT(CASE WHEN status = 'SENT' THEN 1 END) as pending_responses,
    (COUNT(CASE WHEN status = 'RESPONDED' THEN 1 END) * 100.0 / COUNT(*)) as response_rate
FROM invitations 
WHERE timestamp::timestamp >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY DATE_TRUNC('day', timestamp::timestamp), type
ORDER BY date DESC, type;

-- Invitation Response Analysis
SELECT 
    i.type as invitation_type,
    i.status,
    COUNT(*) as count,
    (COUNT(*) * 100.0 / (SELECT COUNT(*) FROM invitations WHERE timestamp::timestamp >= CURRENT_DATE - INTERVAL '30 days')) as percentage,
    AVG(EXTRACT(EPOCH FROM (i.responded_at::timestamp - i.sent_at::timestamp)) / 3600) as avg_response_hours
FROM invitations i
WHERE i.timestamp::timestamp >= CURRENT_DATE - INTERVAL '30 days'
  AND i.sent_at IS NOT NULL
GROUP BY i.type, i.status
ORDER BY i.type, count DESC;

-- ============================================================================
-- CROSS-SERVICE PERFORMANCE ANALYSIS
-- ============================================================================

-- Business Unit Performance Correlation
WITH business_metrics AS (
    SELECT 
        bu.business_unit_id,
        bu.name,
        bu.trust_score,
        bu.stars as business_rating,
        bu.number_of_reviews,
        COUNT(r.review_id) as recent_reviews,
        AVG(r.stars) as recent_avg_rating,
        COUNT(CASE WHEN r.is_verified = true THEN 1 END) as verified_reviews,
        COUNT(i.invitation_id) as invitations_sent,
        COUNT(CASE WHEN i.status = 'RESPONDED' THEN 1 END) as invitations_responded
    FROM business_units bu
    LEFT JOIN reviews r ON bu.business_unit_id = r.business_unit_id 
        AND r.timestamp::timestamp >= CURRENT_DATE - INTERVAL '30 days'
    LEFT JOIN invitations i ON bu.business_unit_id = i.business_unit_id 
        AND i.timestamp::timestamp >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY bu.business_unit_id, bu.name, bu.trust_score, bu.stars, bu.number_of_reviews
)
SELECT 
    business_unit_id,
    name,
    trust_score,
    business_rating,
    number_of_reviews,
    recent_reviews,
    recent_avg_rating,
    verified_reviews,
    invitations_sent,
    invitations_responded,
    CASE 
        WHEN invitations_sent > 0 THEN (invitations_responded * 100.0 / invitations_sent)
        ELSE 0 
    END as invitation_response_rate
FROM business_metrics
ORDER BY trust_score DESC;

-- ============================================================================
-- TREND ANALYSIS AND FORECASTING
-- ============================================================================

-- Weekly Performance Trends
SELECT 
    DATE_TRUNC('week', timestamp::timestamp) as week,
    COUNT(DISTINCT review_id) as review_count,
    AVG(stars) as avg_rating,
    COUNT(CASE WHEN stars >= 4 THEN 1 END) as positive_reviews,
    COUNT(CASE WHEN stars <= 2 THEN 1 END) as negative_reviews,
    COUNT(CASE WHEN is_verified = true THEN 1 END) as verified_reviews,
    AVG(helpful_count) as avg_helpful_count
FROM reviews 
WHERE timestamp::timestamp >= CURRENT_DATE - INTERVAL '12 weeks'
GROUP BY DATE_TRUNC('week', timestamp::timestamp)
ORDER BY week DESC;

-- Monthly Business Growth
SELECT 
    DATE_TRUNC('month', timestamp::timestamp) as month,
    COUNT(DISTINCT business_unit_id) as active_businesses,
    COUNT(DISTINCT review_id) as total_reviews,
    AVG(stars) as avg_rating,
    COUNT(DISTINCT consumer_id) as unique_consumers
FROM reviews 
WHERE timestamp::timestamp >= CURRENT_DATE - INTERVAL '12 months'
GROUP BY DATE_TRUNC('month', timestamp::timestamp)
ORDER BY month DESC;

-- ============================================================================
-- ALERTING AND THRESHOLD ANALYSIS
-- ============================================================================

-- Low Rating Alerts
SELECT 
    'Low Rating Alert' as alert_type,
    business_unit_id,
    AVG(stars) as current_avg_rating,
    COUNT(*) as review_count,
    'Average rating below 3.0' as alert_message,
    timestamp::timestamp as last_updated
FROM reviews 
WHERE timestamp::timestamp >= CURRENT_DATE - INTERVAL '24 hours'
GROUP BY business_unit_id, timestamp::timestamp
HAVING AVG(stars) < 3.0 AND COUNT(*) >= 5

UNION ALL

SELECT 
    'High Negative Review Rate' as alert_type,
    business_unit_id,
    (COUNT(CASE WHEN stars <= 2 THEN 1 END) * 100.0 / COUNT(*)) as negative_percentage,
    COUNT(*) as review_count,
    'Negative review rate above 20%' as alert_message,
    timestamp::timestamp as last_updated
FROM reviews 
WHERE timestamp::timestamp >= CURRENT_DATE - INTERVAL '24 hours'
GROUP BY business_unit_id, timestamp::timestamp
HAVING (COUNT(CASE WHEN stars <= 2 THEN 1 END) * 100.0 / COUNT(*)) > 20 AND COUNT(*) >= 10;

-- Performance Degradation Detection
SELECT 
    business_unit_id,
    DATE_TRUNC('hour', timestamp::timestamp) as hour,
    AVG(stars) as current_avg_rating,
    LAG(AVG(stars), 1) OVER (PARTITION BY business_unit_id ORDER BY DATE_TRUNC('hour', timestamp::timestamp)) as previous_hour_rating,
    CASE 
        WHEN LAG(AVG(stars), 1) OVER (PARTITION BY business_unit_id ORDER BY DATE_TRUNC('hour', timestamp::timestamp)) > 0 
        THEN (AVG(stars) - LAG(AVG(stars), 1) OVER (PARTITION BY business_unit_id ORDER BY DATE_TRUNC('hour', timestamp::timestamp))) / 
             LAG(AVG(stars), 1) OVER (PARTITION BY business_unit_id ORDER BY DATE_TRUNC('hour', timestamp::timestamp)) * 100 
        ELSE 0 
    END as rating_change_percent
FROM reviews 
WHERE timestamp::timestamp >= CURRENT_DATE - INTERVAL '24 hours'
GROUP BY business_unit_id, DATE_TRUNC('hour', timestamp::timestamp)
HAVING COUNT(*) >= 5  -- Ensure sufficient data points
ORDER BY rating_change_percent ASC
LIMIT 20;

-- ============================================================================
-- COMPLIANCE AND AUDITING
-- ============================================================================

-- Daily Performance Summary for Auditing
SELECT 
    DATE_TRUNC('day', timestamp::timestamp) as date,
    'Reviews' as data_source,
    COUNT(DISTINCT review_id) as total_records,
    COUNT(DISTINCT business_unit_id) as businesses_reviewed,
    COUNT(DISTINCT consumer_id) as unique_consumers,
    AVG(stars) as avg_rating,
    COUNT(CASE WHEN is_verified = true THEN 1 END) as verified_reviews
FROM reviews 
WHERE timestamp::timestamp >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY DATE_TRUNC('day', timestamp::timestamp)

UNION ALL

SELECT 
    DATE_TRUNC('day', timestamp::timestamp) as date,
    'Invitations' as data_source,
    COUNT(DISTINCT invitation_id) as total_records,
    COUNT(DISTINCT business_unit_id) as businesses_invited,
    COUNT(DISTINCT consumer_id) as unique_consumers,
    0 as avg_rating,
    COUNT(CASE WHEN status = 'RESPONDED' THEN 1 END) as responded_invitations
FROM invitations 
WHERE timestamp::timestamp >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY DATE_TRUNC('day', timestamp::timestamp)
ORDER BY date DESC, data_source;

-- Data Quality Assessment
SELECT 
    'Reviews' as table_name,
    COUNT(*) as total_records,
    COUNT(CASE WHEN review_id IS NOT NULL AND review_id != '' THEN 1 END) as valid_review_ids,
    COUNT(CASE WHEN stars >= 1 AND stars <= 5 THEN 1 END) as valid_ratings,
    COUNT(CASE WHEN created_at IS NOT NULL AND created_at != '' THEN 1 END) as valid_dates,
    (COUNT(CASE WHEN review_id IS NOT NULL AND review_id != '' THEN 1 END) * 100.0 / COUNT(*)) as data_quality_score
FROM reviews 
WHERE timestamp::timestamp >= CURRENT_DATE - INTERVAL '7 days'

UNION ALL

SELECT 
    'Business Units' as table_name,
    COUNT(*) as total_records,
    COUNT(CASE WHEN business_unit_id IS NOT NULL AND business_unit_id != '' THEN 1 END) as valid_business_ids,
    COUNT(CASE WHEN name IS NOT NULL AND name != '' THEN 1 END) as valid_names,
    COUNT(CASE WHEN trust_score >= 0 THEN 1 END) as valid_trust_scores,
    (COUNT(CASE WHEN business_unit_id IS NOT NULL AND business_unit_id != '' THEN 1 END) * 100.0 / COUNT(*)) as data_quality_score
FROM business_units 
WHERE timestamp::timestamp >= CURRENT_DATE - INTERVAL '7 days'

UNION ALL

SELECT 
    'Invitations' as table_name,
    COUNT(*) as total_records,
    COUNT(CASE WHEN invitation_id IS NOT NULL AND invitation_id != '' THEN 1 END) as valid_invitation_ids,
    COUNT(CASE WHEN status IS NOT NULL AND status != '' THEN 1 END) as valid_statuses,
    COUNT(CASE WHEN type IS NOT NULL AND type != '' THEN 1 END) as valid_types,
    (COUNT(CASE WHEN invitation_id IS NOT NULL AND invitation_id != '' THEN 1 END) * 100.0 / COUNT(*)) as data_quality_score
FROM invitations 
WHERE timestamp::timestamp >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY data_quality_score DESC;
