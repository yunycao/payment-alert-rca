-- =============================================================================
-- COMPETITOR MESSAGING ANALYSIS
-- Identifies competing intents that suppress or displace payment alert messages
-- within the eligible audience
-- =============================================================================

WITH payment_alert_eligible AS (
    SELECT DISTINCT
        e.user_id,
        e.eligibility_date,
        e.channel,
        e.segment,
        e.propensity_score AS pa_propensity_score,
        e.campaign_id AS pa_campaign_id
    FROM {{ database }}.{{ schema }}.messaging_eligibility e
    WHERE e.intent_name = '{{ intent_name }}'
      AND e.eligibility_date BETWEEN '{{ start_date }}' AND '{{ end_date }}'
),

-- All messages sent to payment-alert-eligible users within the analysis window
all_messages_to_eligible AS (
    SELECT
        d.user_id,
        d.intent_name,
        d.channel,
        d.campaign_id,
        d.campaign_name,
        d.send_timestamp,
        d.delivery_status,
        d.message_priority
    FROM {{ database }}.{{ schema }}.message_delivery d
    INNER JOIN payment_alert_eligible pae
        ON d.user_id = pae.user_id
    WHERE d.send_date BETWEEN DATEADD(day, -1, '{{ start_date }}') AND DATEADD(day, 1, '{{ end_date }}')
      AND d.intent_name != '{{ intent_name }}'
),

-- Suppression events where payment alert lost to a competitor
suppression_events AS (
    SELECT
        t.user_id,
        t.channel,
        t.decision_date,
        t.suppression_reason,
        t.competing_intent,
        t.competing_priority,
        t.propensity_score AS pa_score_at_decision
    FROM {{ database }}.{{ schema }}.targeting_decisions t
    WHERE t.intent_name = '{{ intent_name }}'
      AND t.was_targeted = FALSE
      AND t.suppression_reason IN ('priority_suppression', 'competitor_won', 'channel_fatigue')
      AND t.decision_date BETWEEN '{{ start_date }}' AND '{{ end_date }}'
)

-- COMPETITOR IMPACT SUMMARY
SELECT
    ame.intent_name AS competitor_intent,
    ame.channel,
    pae.segment,
    pae.eligibility_date AS report_date,

    -- Overlap metrics
    COUNT(DISTINCT ame.user_id) AS users_receiving_competitor_msg,
    COUNT(DISTINCT pae.user_id) AS eligible_audience_size,
    ROUND(COUNT(DISTINCT ame.user_id) * 100.0 / NULLIF(COUNT(DISTINCT pae.user_id), 0), 2)
        AS competitor_overlap_pct,

    -- Suppression metrics
    COUNT(DISTINCT se.user_id) AS users_suppressed_by_competitor,
    ROUND(COUNT(DISTINCT se.user_id) * 100.0 / NULLIF(COUNT(DISTINCT pae.user_id), 0), 2)
        AS suppression_rate_pct,

    -- Timing analysis (competitor sent before/after PA window)
    COUNT(DISTINCT CASE
        WHEN DATEDIFF(hour, ame.send_timestamp, pae.eligibility_date) BETWEEN 0 AND {{ window_hours }}
        THEN ame.user_id END) AS competitor_within_window,

    -- Priority comparison
    AVG(ame.message_priority) AS avg_competitor_priority,
    AVG(pae.pa_propensity_score) AS avg_pa_propensity_suppressed_users,

    -- Campaign granularity
    COUNT(DISTINCT ame.campaign_id) AS competitor_campaign_count

FROM payment_alert_eligible pae
LEFT JOIN all_messages_to_eligible ame
    ON pae.user_id = ame.user_id
    AND pae.channel = ame.channel
LEFT JOIN suppression_events se
    ON pae.user_id = se.user_id
    AND pae.channel = se.channel
    AND ame.intent_name = se.competing_intent

WHERE ame.intent_name IS NOT NULL

GROUP BY 1, 2, 3, 4
ORDER BY users_suppressed_by_competitor DESC;
