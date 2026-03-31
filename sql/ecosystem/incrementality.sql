-- =============================================================================
-- INCREMENTALITY MEASUREMENT
-- Compares treatment (messaged) vs holdout (eligible but withheld) groups
-- to estimate the true causal lift of payment alert messaging.
--
-- This is not a funnel metric — it answers: "What would have happened
-- without the message?" using the randomized holdout population.
-- =============================================================================

WITH holdout_assignment AS (
    -- Holdout is assigned at eligibility time, upstream of targeting
    SELECT
        h.user_id,
        h.assignment_date,
        h.holdout_group,        -- 'treatment' or 'holdout'
        h.channel,
        h.segment,
        h.propensity_score,
        h.ltv_segment           -- high / medium / low predicted LTV
    FROM {{ database }}.{{ schema }}.holdout_assignments h
    WHERE h.intent_name = '{{ intent_name }}'
      AND h.assignment_date BETWEEN '{{ start_date }}' AND '{{ end_date }}'
),

-- Conversion outcomes regardless of whether message was sent
user_outcomes AS (
    SELECT
        u.user_id,
        u.outcome_date,
        -- Primary metric: did user complete payment within attribution window?
        MAX(CASE WHEN u.event_type = 'payment_completed'
                  AND DATEDIFF(hour, ha.assignment_date, u.outcome_date) <= {{ attribution_window_hours }}
            THEN 1 ELSE 0 END) AS converted,

        -- Revenue within window
        SUM(CASE WHEN u.event_type = 'payment_completed'
                  AND DATEDIFF(hour, ha.assignment_date, u.outcome_date) <= {{ attribution_window_hours }}
            THEN u.revenue_amount ELSE 0 END) AS revenue,

        -- Engagement health: time to next payment (lower = better)
        MIN(CASE WHEN u.event_type = 'payment_completed'
            THEN DATEDIFF(day, ha.assignment_date, u.outcome_date) END) AS days_to_payment,

        -- LTV proxies
        SUM(CASE WHEN u.event_type = 'payment_completed'
                  AND DATEDIFF(day, ha.assignment_date, u.outcome_date) <= 30
            THEN u.revenue_amount ELSE 0 END) AS ltv_30d,
        SUM(CASE WHEN u.event_type = 'payment_completed'
                  AND DATEDIFF(day, ha.assignment_date, u.outcome_date) <= 90
            THEN u.revenue_amount ELSE 0 END) AS ltv_90d,

        -- Fatigue / negative signals
        MAX(CASE WHEN u.event_type = 'unsubscribe' THEN 1 ELSE 0 END) AS unsubscribed,
        MAX(CASE WHEN u.event_type = 'opt_out' THEN 1 ELSE 0 END) AS opted_out

    FROM {{ database }}.{{ schema }}.user_events u
    INNER JOIN holdout_assignment ha ON u.user_id = ha.user_id
    WHERE u.outcome_date BETWEEN ha.assignment_date
          AND DATEADD(day, {{ max_outcome_window_days }}, ha.assignment_date)
    GROUP BY 1, 2
),

-- Pre-period activity for difference-in-differences
pre_period_activity AS (
    SELECT
        ha.user_id,
        ha.holdout_group,
        COUNT(DISTINCT pe.event_date) AS pre_active_days,
        SUM(CASE WHEN pe.event_type = 'payment_completed' THEN 1 ELSE 0 END) AS pre_payments,
        SUM(CASE WHEN pe.event_type = 'payment_completed' THEN pe.revenue_amount ELSE 0 END) AS pre_revenue,
        AVG(pe.app_session_count) AS pre_avg_sessions
    FROM holdout_assignment ha
    LEFT JOIN {{ database }}.{{ schema }}.user_events pe
        ON ha.user_id = pe.user_id
        AND pe.event_date BETWEEN DATEADD(day, -{{ pre_period_days }}, ha.assignment_date)
                               AND DATEADD(day, -1, ha.assignment_date)
    GROUP BY 1, 2
)

SELECT
    ha.assignment_date,
    ha.holdout_group,
    ha.channel,
    ha.segment,
    ha.ltv_segment,

    -- Sample sizes
    COUNT(DISTINCT ha.user_id) AS n_users,

    -- Primary: conversion rate
    AVG(COALESCE(uo.converted, 0)) AS conversion_rate,
    STDDEV(COALESCE(uo.converted, 0)) AS conversion_stddev,

    -- Revenue per eligible user (not per converted — intent-level efficiency)
    AVG(COALESCE(uo.revenue, 0)) AS revenue_per_eligible,

    -- Time to payment (among converters)
    AVG(uo.days_to_payment) AS avg_days_to_payment,
    MEDIAN(uo.days_to_payment) AS median_days_to_payment,

    -- LTV signals
    AVG(COALESCE(uo.ltv_30d, 0)) AS avg_ltv_30d,
    AVG(COALESCE(uo.ltv_90d, 0)) AS avg_ltv_90d,

    -- Negative externalities
    AVG(COALESCE(uo.unsubscribed, 0)) AS unsubscribe_rate,
    AVG(COALESCE(uo.opted_out, 0)) AS opt_out_rate,

    -- Pre-period covariates (for DiD and balance checks)
    AVG(ppa.pre_payments) AS avg_pre_payments,
    AVG(ppa.pre_revenue) AS avg_pre_revenue,
    AVG(ppa.pre_avg_sessions) AS avg_pre_sessions,

    -- Propensity score distribution (for stratified estimation)
    AVG(ha.propensity_score) AS avg_propensity_score,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY ha.propensity_score) AS p25_propensity,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY ha.propensity_score) AS p75_propensity

FROM holdout_assignment ha
LEFT JOIN user_outcomes uo ON ha.user_id = uo.user_id
LEFT JOIN pre_period_activity ppa ON ha.user_id = ppa.user_id

GROUP BY 1, 2, 3, 4, 5
ORDER BY 1, 2, 3, 4, 5;
