-- =============================================================================
-- ROOT CAUSE ANALYSIS: On-Time Payment Rate Drop Decomposition
-- Isolates whether on-time rate decline is driven by:
--   (1) Population mix shift (different users entering the funnel)
--   (2) Within-group rate change (same users, worse outcomes)
--   (3) Messaging effectiveness change (channel/campaign performance)
--   (4) External factors (due date distribution, payment category)
--
-- Uses period-over-period comparison with additive decomposition.
-- =============================================================================

WITH current_period AS (
    SELECT
        po.user_id,
        ea.segment,
        ea.channel,
        ea.campaign_id,
        td.score_source,
        td.was_targeted,
        td.suppression_reason,
        ea.propensity_score,
        NTILE(10) OVER (ORDER BY ea.propensity_score) AS propensity_decile,
        po.payment_amount,
        po.payment_status,
        po.payment_category,
        CASE WHEN po.payment_completed_date <= po.payment_due_date THEN 1 ELSE 0 END AS is_on_time,
        DATEDIFF(day, ea.eligibility_date, po.payment_due_date) AS days_to_due_at_messaging,
        -- Was the user actually messaged (targeted + delivered)?
        CASE WHEN de.delivery_status = 'delivered' THEN 1 ELSE 0 END AS was_messaged,
        CASE WHEN ee.event_type = 'click' THEN 1 ELSE 0 END AS clicked_message
    FROM {{ database }}.{{ schema }}.payment_outcomes po
    INNER JOIN {{ database }}.{{ schema }}.messaging_eligibility ea
        ON po.user_id = ea.user_id AND ea.intent_name = '{{ intent_name }}'
    LEFT JOIN {{ database }}.{{ schema }}.targeting_decisions td
        ON ea.user_id = td.user_id AND ea.channel = td.channel
        AND td.intent_name = '{{ intent_name }}'
    LEFT JOIN {{ database }}.{{ schema }}.message_delivery de
        ON ea.user_id = de.user_id AND ea.channel = de.channel
        AND de.intent_name = '{{ intent_name }}'
    LEFT JOIN {{ database }}.{{ schema }}.message_engagement ee
        ON ea.user_id = ee.user_id AND ea.channel = ee.channel
        AND ee.intent_name = '{{ intent_name }}' AND ee.event_type = 'click'
    WHERE po.payment_due_date BETWEEN '{{ current_start }}' AND '{{ current_end }}'
      AND ea.eligibility_date BETWEEN '{{ current_start }}' AND '{{ current_end }}'
),

baseline_period AS (
    SELECT
        po.user_id,
        ea.segment,
        ea.channel,
        ea.campaign_id,
        td.score_source,
        td.was_targeted,
        td.suppression_reason,
        ea.propensity_score,
        NTILE(10) OVER (ORDER BY ea.propensity_score) AS propensity_decile,
        po.payment_amount,
        po.payment_status,
        po.payment_category,
        CASE WHEN po.payment_completed_date <= po.payment_due_date THEN 1 ELSE 0 END AS is_on_time,
        DATEDIFF(day, ea.eligibility_date, po.payment_due_date) AS days_to_due_at_messaging,
        CASE WHEN de.delivery_status = 'delivered' THEN 1 ELSE 0 END AS was_messaged,
        CASE WHEN ee.event_type = 'click' THEN 1 ELSE 0 END AS clicked_message
    FROM {{ database }}.{{ schema }}.payment_outcomes po
    INNER JOIN {{ database }}.{{ schema }}.messaging_eligibility ea
        ON po.user_id = ea.user_id AND ea.intent_name = '{{ intent_name }}'
    LEFT JOIN {{ database }}.{{ schema }}.targeting_decisions td
        ON ea.user_id = td.user_id AND ea.channel = td.channel
        AND td.intent_name = '{{ intent_name }}'
    LEFT JOIN {{ database }}.{{ schema }}.message_delivery de
        ON ea.user_id = de.user_id AND ea.channel = de.channel
        AND de.intent_name = '{{ intent_name }}'
    LEFT JOIN {{ database }}.{{ schema }}.message_engagement ee
        ON ea.user_id = ee.user_id AND ea.channel = ee.channel
        AND ee.intent_name = '{{ intent_name }}' AND ee.event_type = 'click'
    WHERE po.payment_due_date BETWEEN '{{ baseline_start }}' AND '{{ baseline_end }}'
      AND ea.eligibility_date BETWEEN '{{ baseline_start }}' AND '{{ baseline_end }}'
)

-- Multi-dimensional summary for decomposition
SELECT
    'current' AS period,
    segment,
    channel,
    score_source,
    propensity_decile,
    payment_category,
    was_messaged,
    COUNT(*) AS n_users,
    AVG(is_on_time) AS on_time_rate,
    AVG(payment_amount) AS avg_spend,
    SUM(payment_amount) AS total_spend,
    AVG(days_to_due_at_messaging) AS avg_days_to_due,
    AVG(clicked_message) AS click_rate,
    SUM(CASE WHEN payment_status = 'missed' THEN 1 ELSE 0 END) AS missed_payments,
    SUM(CASE WHEN payment_status = 'late' THEN 1 ELSE 0 END) AS late_payments
FROM current_period
GROUP BY 1, 2, 3, 4, 5, 6, 7

UNION ALL

SELECT
    'baseline' AS period,
    segment,
    channel,
    score_source,
    propensity_decile,
    payment_category,
    was_messaged,
    COUNT(*) AS n_users,
    AVG(is_on_time) AS on_time_rate,
    AVG(payment_amount) AS avg_spend,
    SUM(payment_amount) AS total_spend,
    AVG(days_to_due_at_messaging) AS avg_days_to_due,
    AVG(clicked_message) AS click_rate,
    SUM(CASE WHEN payment_status = 'missed' THEN 1 ELSE 0 END) AS missed_payments,
    SUM(CASE WHEN payment_status = 'late' THEN 1 ELSE 0 END) AS late_payments
FROM baseline_period
GROUP BY 1, 2, 3, 4, 5, 6, 7

ORDER BY period, segment, channel;
