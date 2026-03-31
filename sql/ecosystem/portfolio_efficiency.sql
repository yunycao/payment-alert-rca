-- =============================================================================
-- PORTFOLIO EFFICIENCY: CROSS-INTENT RESOURCE ALLOCATION
-- Measures how efficiently messaging inventory is allocated across intents.
-- Answers: "Are we sending the RIGHT messages to the RIGHT users?"
--
-- This is a system-level view — not optimizing one intent,
-- but measuring whether the portfolio of intents collectively maximizes
-- user-level conversion probability given messaging constraints.
-- =============================================================================

WITH user_daily_messages AS (
    -- All messages a user receives across ALL intents in a day
    SELECT
        d.user_id,
        d.send_date,
        d.intent_name,
        d.channel,
        d.campaign_id,
        d.message_priority,
        t.propensity_score,
        t.score_source,
        ROW_NUMBER() OVER (
            PARTITION BY d.user_id, d.send_date
            ORDER BY d.message_priority DESC, t.propensity_score DESC
        ) AS msg_rank_in_day
    FROM {{ database }}.{{ schema }}.message_delivery d
    INNER JOIN {{ database }}.{{ schema }}.targeting_decisions t
        ON d.user_id = t.user_id AND d.campaign_id = t.campaign_id
    WHERE d.send_date BETWEEN '{{ start_date }}' AND '{{ end_date }}'
      AND d.delivery_status IN ('sent', 'delivered')
),

-- User-level daily outcome: did they convert on ANY intent?
user_daily_outcomes AS (
    SELECT
        c.user_id,
        c.conversion_date,
        c.intent_name AS converting_intent,
        c.revenue_amount,
        c.attribution_channel
    FROM {{ database }}.{{ schema }}.conversions c
    WHERE c.conversion_date BETWEEN '{{ start_date }}' AND '{{ end_date }}'
),

-- Per-intent efficiency
intent_efficiency AS (
    SELECT
        udm.send_date,
        udm.intent_name,
        udm.channel,

        -- Volume
        COUNT(DISTINCT udm.user_id) AS users_messaged,
        COUNT(*) AS messages_sent,

        -- Intent-level conversion
        COUNT(DISTINCT CASE
            WHEN udo.converting_intent = udm.intent_name THEN udm.user_id
        END) AS same_intent_conversions,

        -- Cross-intent conversion (user converted, but on a DIFFERENT intent)
        COUNT(DISTINCT CASE
            WHEN udo.user_id IS NOT NULL AND udo.converting_intent != udm.intent_name
            THEN udm.user_id
        END) AS cross_intent_conversions,

        -- No conversion at all
        COUNT(DISTINCT CASE
            WHEN udo.user_id IS NULL THEN udm.user_id
        END) AS no_conversion,

        -- Revenue
        SUM(CASE WHEN udo.converting_intent = udm.intent_name
            THEN udo.revenue_amount ELSE 0 END) AS attributed_revenue,

        -- Messaging load: how many total messages did these users get?
        AVG(udm.msg_rank_in_day) AS avg_msg_position,

        -- Score quality
        AVG(udm.propensity_score) AS avg_propensity,
        COUNT(CASE WHEN udm.score_source = 'default' THEN 1 END) * 1.0
            / NULLIF(COUNT(*), 0) AS default_score_pct

    FROM user_daily_messages udm
    LEFT JOIN user_daily_outcomes udo
        ON udm.user_id = udo.user_id
        AND udm.send_date = udo.conversion_date
    GROUP BY 1, 2, 3
),

-- Frequency impact: user-level message load vs outcome
frequency_impact AS (
    SELECT
        udm.send_date,
        udm.user_id,
        COUNT(DISTINCT udm.intent_name) AS intents_received,
        COUNT(*) AS total_messages,
        MAX(CASE WHEN udo.user_id IS NOT NULL THEN 1 ELSE 0 END) AS any_conversion,
        MAX(udo.revenue_amount) AS max_conversion_revenue
    FROM user_daily_messages udm
    LEFT JOIN user_daily_outcomes udo
        ON udm.user_id = udo.user_id AND udm.send_date = udo.conversion_date
    GROUP BY 1, 2
)

-- PORTFOLIO SUMMARY
SELECT
    ie.send_date,
    ie.intent_name,
    ie.channel,
    ie.users_messaged,
    ie.messages_sent,
    ie.same_intent_conversions,
    ie.cross_intent_conversions,
    ie.no_conversion,

    -- Efficiency ratios
    ROUND(ie.same_intent_conversions * 1.0 / NULLIF(ie.users_messaged, 0), 4)
        AS intent_conversion_rate,
    ROUND((ie.same_intent_conversions + ie.cross_intent_conversions) * 1.0
        / NULLIF(ie.users_messaged, 0), 4) AS any_conversion_rate,
    ROUND(ie.attributed_revenue / NULLIF(ie.users_messaged, 0), 2)
        AS revenue_per_user,

    -- Portfolio health
    ie.avg_msg_position,
    ie.avg_propensity,
    ie.default_score_pct,

    -- Frequency stats (aggregated)
    fi.avg_intents_per_user,
    fi.avg_messages_per_user,
    fi.conversion_rate_by_frequency

FROM intent_efficiency ie
LEFT JOIN (
    SELECT
        send_date,
        AVG(intents_received) AS avg_intents_per_user,
        AVG(total_messages) AS avg_messages_per_user,
        AVG(any_conversion) AS conversion_rate_by_frequency
    FROM frequency_impact
    GROUP BY 1
) fi ON ie.send_date = fi.send_date

ORDER BY ie.send_date, ie.intent_name, ie.channel;
