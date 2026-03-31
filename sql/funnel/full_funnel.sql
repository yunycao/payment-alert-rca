-- =============================================================================
-- PAYMENT ALERT: Full Funnel Analysis
-- Tracks users from eligibility through conversion across all channels
-- =============================================================================

WITH eligible_audience AS (
    SELECT
        e.user_id,
        e.eligibility_date,
        e.segment,
        e.channel,
        e.propensity_score,
        e.model_version,
        e.campaign_id,
        e.campaign_name
    FROM {{ database }}.{{ schema }}.messaging_eligibility e
    WHERE e.intent_name = '{{ intent_name }}'
      AND e.eligibility_date BETWEEN '{{ start_date }}' AND '{{ end_date }}'
      AND e.channel IN ('email', 'push', 'in_app')
),

targeting_decisions AS (
    SELECT
        t.user_id,
        t.decision_timestamp,
        t.channel,
        t.campaign_id,
        t.was_targeted,
        t.suppression_reason,
        t.competing_intent,
        t.propensity_score AS targeting_score,
        t.score_source,  -- 'model' or 'default'
        t.model_latency_ms
    FROM {{ database }}.{{ schema }}.targeting_decisions t
    WHERE t.intent_name = '{{ intent_name }}'
      AND t.decision_date BETWEEN '{{ start_date }}' AND '{{ end_date }}'
),

delivery_events AS (
    SELECT
        d.user_id,
        d.channel,
        d.campaign_id,
        d.send_timestamp,
        d.delivery_status,  -- 'sent', 'delivered', 'bounced', 'failed'
        d.delivery_timestamp,
        d.message_id
    FROM {{ database }}.{{ schema }}.message_delivery d
    WHERE d.intent_name = '{{ intent_name }}'
      AND d.send_date BETWEEN '{{ start_date }}' AND '{{ end_date }}'
),

engagement_events AS (
    SELECT
        g.user_id,
        g.channel,
        g.campaign_id,
        g.message_id,
        g.event_type,  -- 'open', 'click', 'dismiss', 'impression'
        g.event_timestamp
    FROM {{ database }}.{{ schema }}.message_engagement g
    WHERE g.intent_name = '{{ intent_name }}'
      AND g.event_date BETWEEN '{{ start_date }}' AND '{{ end_date }}'
),

conversions AS (
    SELECT
        c.user_id,
        c.conversion_timestamp,
        c.conversion_type,  -- 'payment_completed', 'payment_scheduled', 'payment_viewed'
        c.attribution_channel,
        c.attribution_campaign_id,
        c.attribution_message_id,
        c.revenue_amount
    FROM {{ database }}.{{ schema }}.conversions c
    WHERE c.intent_name = '{{ intent_name }}'
      AND c.conversion_date BETWEEN '{{ start_date }}' AND '{{ end_date }}'
)

-- FULL FUNNEL AGGREGATION BY DAY x CHANNEL x SEGMENT
SELECT
    ea.eligibility_date AS report_date,
    ea.channel,
    ea.segment,
    ea.campaign_id,
    ea.campaign_name,

    -- Stage 1: Eligible
    COUNT(DISTINCT ea.user_id) AS eligible_users,

    -- Stage 2: Targeted (not suppressed)
    COUNT(DISTINCT CASE WHEN td.was_targeted = TRUE THEN ea.user_id END) AS targeted_users,

    -- Stage 2b: Suppressed breakdown
    COUNT(DISTINCT CASE WHEN td.was_targeted = FALSE THEN ea.user_id END) AS suppressed_users,
    COUNT(DISTINCT CASE WHEN td.suppression_reason = 'frequency_cap' THEN ea.user_id END) AS suppressed_frequency_cap,
    COUNT(DISTINCT CASE WHEN td.suppression_reason = 'priority_suppression' THEN ea.user_id END) AS suppressed_priority,
    COUNT(DISTINCT CASE WHEN td.suppression_reason = 'channel_fatigue' THEN ea.user_id END) AS suppressed_fatigue,
    COUNT(DISTINCT CASE WHEN td.suppression_reason = 'holdout_group' THEN ea.user_id END) AS suppressed_holdout,
    COUNT(DISTINCT CASE WHEN td.suppression_reason = 'competitor_won' THEN ea.user_id END) AS suppressed_competitor,

    -- Stage 3: Sent
    COUNT(DISTINCT CASE WHEN de.delivery_status IN ('sent', 'delivered') THEN ea.user_id END) AS sent_users,

    -- Stage 4: Delivered
    COUNT(DISTINCT CASE WHEN de.delivery_status = 'delivered' THEN ea.user_id END) AS delivered_users,

    -- Stage 5: Opened / Impression
    COUNT(DISTINCT CASE WHEN ee_open.event_type IN ('open', 'impression') THEN ea.user_id END) AS opened_users,

    -- Stage 6: Clicked
    COUNT(DISTINCT CASE WHEN ee_click.event_type = 'click' THEN ea.user_id END) AS clicked_users,

    -- Stage 7: Converted
    COUNT(DISTINCT cv.user_id) AS converted_users,
    SUM(COALESCE(cv.revenue_amount, 0)) AS total_revenue,

    -- Scoring diagnostics
    AVG(ea.propensity_score) AS avg_propensity_score,
    MEDIAN(ea.propensity_score) AS median_propensity_score,
    COUNT(DISTINCT CASE WHEN td.score_source = 'default' THEN ea.user_id END) AS default_score_users,
    AVG(td.model_latency_ms) AS avg_model_latency_ms

FROM eligible_audience ea
LEFT JOIN targeting_decisions td
    ON ea.user_id = td.user_id
    AND ea.channel = td.channel
    AND ea.campaign_id = td.campaign_id
LEFT JOIN delivery_events de
    ON ea.user_id = de.user_id
    AND ea.channel = de.channel
    AND ea.campaign_id = de.campaign_id
LEFT JOIN engagement_events ee_open
    ON ea.user_id = ee_open.user_id
    AND ea.channel = ee_open.channel
    AND ea.campaign_id = ee_open.campaign_id
    AND ee_open.event_type IN ('open', 'impression')
LEFT JOIN engagement_events ee_click
    ON ea.user_id = ee_click.user_id
    AND ea.channel = ee_click.channel
    AND ea.campaign_id = ee_click.campaign_id
    AND ee_click.event_type = 'click'
LEFT JOIN conversions cv
    ON ea.user_id = cv.user_id
    AND ea.campaign_id = cv.attribution_campaign_id

GROUP BY 1, 2, 3, 4, 5
ORDER BY 1, 2, 3;
