-- =============================================================================
-- ML PLATFORM TIMEOUT / DEFAULT SCORE ANALYSIS
-- Identifies cases where propensity model timed out and default scores
-- were assigned, analyzing impact on targeting quality
-- =============================================================================

WITH scoring_events AS (
    SELECT
        t.user_id,
        t.channel,
        t.campaign_id,
        t.decision_date,
        t.decision_timestamp,
        t.propensity_score,
        t.score_source,          -- 'model' vs 'default'
        t.model_latency_ms,
        t.model_version,
        t.was_targeted,
        t.suppression_reason,
        HOUR(t.decision_timestamp) AS decision_hour
    FROM {{ database }}.{{ schema }}.targeting_decisions t
    WHERE t.intent_name = '{{ intent_name }}'
      AND t.decision_date BETWEEN '{{ start_date }}' AND '{{ end_date }}'
),

-- Downstream outcomes for default-scored vs model-scored users
outcomes AS (
    SELECT
        se.user_id,
        se.channel,
        se.score_source,
        se.decision_date,
        CASE WHEN de.delivery_status = 'delivered' THEN 1 ELSE 0 END AS was_delivered,
        CASE WHEN ee.event_type IN ('open', 'impression') THEN 1 ELSE 0 END AS was_opened,
        CASE WHEN ee_click.event_type = 'click' THEN 1 ELSE 0 END AS was_clicked,
        CASE WHEN cv.user_id IS NOT NULL THEN 1 ELSE 0 END AS was_converted,
        cv.revenue_amount
    FROM scoring_events se
    LEFT JOIN {{ database }}.{{ schema }}.message_delivery de
        ON se.user_id = de.user_id AND se.channel = de.channel AND se.campaign_id = de.campaign_id
    LEFT JOIN {{ database }}.{{ schema }}.message_engagement ee
        ON se.user_id = ee.user_id AND se.channel = ee.channel
        AND ee.event_type IN ('open', 'impression')
    LEFT JOIN {{ database }}.{{ schema }}.message_engagement ee_click
        ON se.user_id = ee_click.user_id AND se.channel = ee_click.channel
        AND ee_click.event_type = 'click'
    LEFT JOIN {{ database }}.{{ schema }}.conversions cv
        ON se.user_id = cv.user_id AND se.channel = cv.attribution_channel
)

SELECT
    se.decision_date AS report_date,
    se.channel,
    se.decision_hour,

    -- Volume metrics
    COUNT(*) AS total_decisions,
    COUNT(CASE WHEN se.score_source = 'default' THEN 1 END) AS default_score_count,
    COUNT(CASE WHEN se.score_source = 'model' THEN 1 END) AS model_score_count,
    ROUND(COUNT(CASE WHEN se.score_source = 'default' THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 2)
        AS default_score_pct,

    -- Latency diagnostics
    AVG(se.model_latency_ms) AS avg_latency_ms,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY se.model_latency_ms) AS p50_latency_ms,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY se.model_latency_ms) AS p95_latency_ms,
    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY se.model_latency_ms) AS p99_latency_ms,
    COUNT(CASE WHEN se.model_latency_ms > {{ timeout_threshold_ms }} THEN 1 END) AS timeout_count,

    -- Targeting impact
    COUNT(CASE WHEN se.score_source = 'default' AND se.was_targeted THEN 1 END) AS default_targeted,
    COUNT(CASE WHEN se.score_source = 'model' AND se.was_targeted THEN 1 END) AS model_targeted,

    -- Outcome comparison: default vs model scored
    AVG(CASE WHEN o.score_source = 'default' THEN o.was_opened END) AS default_open_rate,
    AVG(CASE WHEN o.score_source = 'model' THEN o.was_opened END) AS model_open_rate,
    AVG(CASE WHEN o.score_source = 'default' THEN o.was_clicked END) AS default_click_rate,
    AVG(CASE WHEN o.score_source = 'model' THEN o.was_clicked END) AS model_click_rate,
    AVG(CASE WHEN o.score_source = 'default' THEN o.was_converted END) AS default_conversion_rate,
    AVG(CASE WHEN o.score_source = 'model' THEN o.was_converted END) AS model_conversion_rate,

    -- Revenue impact estimate
    SUM(CASE WHEN o.score_source = 'default' THEN o.revenue_amount ELSE 0 END) AS default_revenue,
    SUM(CASE WHEN o.score_source = 'model' THEN o.revenue_amount ELSE 0 END) AS model_revenue

FROM scoring_events se
LEFT JOIN outcomes o ON se.user_id = o.user_id AND se.channel = o.channel AND se.decision_date = o.decision_date

GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;
