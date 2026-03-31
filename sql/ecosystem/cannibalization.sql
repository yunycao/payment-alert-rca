-- =============================================================================
-- CANNIBALIZATION ANALYSIS
-- Measures whether payment alert messaging steals conversions from
-- related intents (bill_reminder, account_balance, etc.) rather than
-- generating truly incremental outcomes.
--
-- Key question: When a user converts after a payment_alert message,
-- would they have converted anyway via a different intent pathway?
-- =============================================================================

WITH pa_converted_users AS (
    -- Users who received payment alert AND converted
    SELECT
        c.user_id,
        c.conversion_timestamp,
        c.attribution_channel,
        c.revenue_amount,
        c.attribution_campaign_id,
        d.send_timestamp AS pa_send_time
    FROM {{ database }}.{{ schema }}.conversions c
    INNER JOIN {{ database }}.{{ schema }}.message_delivery d
        ON c.user_id = d.user_id
        AND c.attribution_message_id = d.message_id
    WHERE c.intent_name = '{{ intent_name }}'
      AND c.conversion_date BETWEEN '{{ start_date }}' AND '{{ end_date }}'
),

-- Other intents that ALSO messaged these same users in the attribution window
cross_intent_messages AS (
    SELECT
        pau.user_id,
        pau.conversion_timestamp,
        pau.revenue_amount AS pa_revenue,
        d.intent_name AS other_intent,
        d.channel AS other_channel,
        d.send_timestamp AS other_send_time,
        DATEDIFF(hour, d.send_timestamp, pau.conversion_timestamp) AS hours_before_conversion,
        -- Did the user engage with the other intent's message?
        MAX(CASE WHEN e.event_type IN ('open', 'impression') THEN 1 ELSE 0 END) AS opened_other,
        MAX(CASE WHEN e.event_type = 'click' THEN 1 ELSE 0 END) AS clicked_other
    FROM pa_converted_users pau
    INNER JOIN {{ database }}.{{ schema }}.message_delivery d
        ON pau.user_id = d.user_id
        AND d.intent_name != '{{ intent_name }}'
        AND d.send_timestamp BETWEEN
            DATEADD(hour, -{{ attribution_window_hours }}, pau.conversion_timestamp)
            AND pau.conversion_timestamp
        AND d.delivery_status = 'delivered'
    LEFT JOIN {{ database }}.{{ schema }}.message_engagement e
        ON d.message_id = e.message_id
    GROUP BY 1, 2, 3, 4, 5, 6, 7
),

-- Organic conversion rate: users who converted WITHOUT any messaging
organic_baseline AS (
    SELECT
        e.segment,
        COUNT(DISTINCT e.user_id) AS eligible_no_msg,
        COUNT(DISTINCT c.user_id) AS converted_no_msg,
        COUNT(DISTINCT c.user_id) * 1.0 / NULLIF(COUNT(DISTINCT e.user_id), 0) AS organic_rate,
        AVG(c.revenue_amount) AS organic_avg_revenue
    FROM {{ database }}.{{ schema }}.messaging_eligibility e
    LEFT JOIN {{ database }}.{{ schema }}.targeting_decisions t
        ON e.user_id = t.user_id AND t.was_targeted = FALSE
        AND t.suppression_reason = 'holdout_group'
    INNER JOIN {{ database }}.{{ schema }}.conversions c
        ON e.user_id = c.user_id
        AND DATEDIFF(day, e.eligibility_date, c.conversion_date) <= {{ organic_window_days }}
    WHERE e.intent_name = '{{ intent_name }}'
      AND e.eligibility_date BETWEEN
          DATEADD(day, -{{ organic_baseline_window_days }}, '{{ start_date }}')
          AND '{{ start_date }}'
    GROUP BY 1
)

-- CANNIBALIZATION SUMMARY
SELECT
    cim.other_intent,
    cim.other_channel,

    -- How many PA conversions also had this intent's message?
    COUNT(DISTINCT cim.user_id) AS dual_exposed_converters,
    COUNT(DISTINCT pau.user_id) AS total_pa_converters,
    ROUND(COUNT(DISTINCT cim.user_id) * 100.0 / NULLIF(COUNT(DISTINCT pau.user_id), 0), 2)
        AS dual_exposure_pct,

    -- Engagement with the other intent
    AVG(cim.opened_other) AS other_intent_open_rate,
    AVG(cim.clicked_other) AS other_intent_click_rate,

    -- Timing: did the other intent message arrive first?
    AVG(cim.hours_before_conversion) AS avg_hours_before_conversion,
    COUNT(CASE WHEN cim.other_send_time < pau.pa_send_time THEN 1 END) AS other_sent_first_count,

    -- Revenue at risk if this is cannibalized (not incremental)
    SUM(cim.pa_revenue) AS pa_attributed_revenue_overlap,

    -- Organic baseline for context
    ob.organic_rate AS segment_organic_rate

FROM pa_converted_users pau
LEFT JOIN cross_intent_messages cim ON pau.user_id = cim.user_id
LEFT JOIN {{ database }}.{{ schema }}.messaging_eligibility me
    ON pau.user_id = me.user_id AND me.intent_name = '{{ intent_name }}'
LEFT JOIN organic_baseline ob ON me.segment = ob.segment

WHERE cim.other_intent IS NOT NULL

GROUP BY cim.other_intent, cim.other_channel, ob.organic_rate
ORDER BY dual_exposed_converters DESC;
