-- =============================================================================
-- LONG-TERM VALUE (LTV) AND ENGAGEMENT HEALTH EFFECTS
-- Measures whether messaging helps or hurts user engagement over time.
-- Compares messaged cohorts against holdout at 7d, 30d, 90d windows.
--
-- Key risk: short-term conversion lift that destroys long-term engagement.
-- =============================================================================

WITH messaging_cohorts AS (
    SELECT
        ha.user_id,
        ha.holdout_group,
        ha.assignment_date AS cohort_date,
        DATE_TRUNC('week', ha.assignment_date) AS cohort_week,
        ha.channel,
        ha.segment,
        ha.propensity_score
    FROM {{ database }}.{{ schema }}.holdout_assignments ha
    WHERE ha.intent_name = '{{ intent_name }}'
      AND ha.assignment_date BETWEEN '{{ start_date }}' AND '{{ end_date }}'
),

-- Forward-looking engagement metrics per user
forward_engagement AS (
    SELECT
        mc.user_id,
        mc.holdout_group,
        mc.cohort_week,
        mc.channel,
        mc.segment,

        -- 7-day window
        SUM(CASE WHEN DATEDIFF(day, mc.cohort_date, ue.event_date) BETWEEN 0 AND 7
            AND ue.event_type = 'payment_completed' THEN ue.revenue_amount ELSE 0 END) AS revenue_7d,
        COUNT(DISTINCT CASE WHEN DATEDIFF(day, mc.cohort_date, ue.event_date) BETWEEN 0 AND 7
            AND ue.event_type = 'app_session' THEN ue.event_date END) AS active_days_7d,

        -- 30-day window
        SUM(CASE WHEN DATEDIFF(day, mc.cohort_date, ue.event_date) BETWEEN 0 AND 30
            AND ue.event_type = 'payment_completed' THEN ue.revenue_amount ELSE 0 END) AS revenue_30d,
        COUNT(DISTINCT CASE WHEN DATEDIFF(day, mc.cohort_date, ue.event_date) BETWEEN 0 AND 30
            AND ue.event_type = 'app_session' THEN ue.event_date END) AS active_days_30d,

        -- 90-day window
        SUM(CASE WHEN DATEDIFF(day, mc.cohort_date, ue.event_date) BETWEEN 0 AND 90
            AND ue.event_type = 'payment_completed' THEN ue.revenue_amount ELSE 0 END) AS revenue_90d,
        COUNT(DISTINCT CASE WHEN DATEDIFF(day, mc.cohort_date, ue.event_date) BETWEEN 0 AND 90
            AND ue.event_type = 'app_session' THEN ue.event_date END) AS active_days_90d,

        -- Fatigue signals
        MAX(CASE WHEN ue.event_type = 'unsubscribe' THEN 1 ELSE 0 END) AS unsubscribed,
        MAX(CASE WHEN ue.event_type = 'opt_out' THEN 1 ELSE 0 END) AS opted_out,
        MAX(CASE WHEN ue.event_type = 'app_delete' THEN 1 ELSE 0 END) AS app_deleted,

        -- Repeat behavior: did they return for a second payment?
        COUNT(DISTINCT CASE WHEN ue.event_type = 'payment_completed'
            AND DATEDIFF(day, mc.cohort_date, ue.event_date) BETWEEN 0 AND 90
            THEN ue.event_date END) AS payment_events_90d,

        -- Spend trajectory by window
        SUM(CASE WHEN DATEDIFF(day, mc.cohort_date, ue.event_date) BETWEEN 0 AND 7
            AND ue.event_type = 'payment_completed' THEN ue.payment_amount ELSE 0 END) AS spend_7d,
        SUM(CASE WHEN DATEDIFF(day, mc.cohort_date, ue.event_date) BETWEEN 0 AND 30
            AND ue.event_type = 'payment_completed' THEN ue.payment_amount ELSE 0 END) AS spend_30d,
        SUM(CASE WHEN DATEDIFF(day, mc.cohort_date, ue.event_date) BETWEEN 0 AND 90
            AND ue.event_type = 'payment_completed' THEN ue.payment_amount ELSE 0 END) AS spend_90d,

        -- On-time payment rate trajectory
        AVG(CASE WHEN DATEDIFF(day, mc.cohort_date, ue.event_date) BETWEEN 0 AND 7
            AND ue.payment_due_date IS NOT NULL
            THEN CASE WHEN ue.payment_completed_date <= ue.payment_due_date THEN 1.0 ELSE 0.0 END END) AS on_time_rate_7d,
        AVG(CASE WHEN DATEDIFF(day, mc.cohort_date, ue.event_date) BETWEEN 0 AND 30
            AND ue.payment_due_date IS NOT NULL
            THEN CASE WHEN ue.payment_completed_date <= ue.payment_due_date THEN 1.0 ELSE 0.0 END END) AS on_time_rate_30d,
        AVG(CASE WHEN DATEDIFF(day, mc.cohort_date, ue.event_date) BETWEEN 0 AND 90
            AND ue.payment_due_date IS NOT NULL
            THEN CASE WHEN ue.payment_completed_date <= ue.payment_due_date THEN 1.0 ELSE 0.0 END END) AS on_time_rate_90d

    FROM messaging_cohorts mc
    LEFT JOIN {{ database }}.{{ schema }}.user_events ue
        ON mc.user_id = ue.user_id
        AND ue.event_date BETWEEN mc.cohort_date
            AND DATEADD(day, 90, mc.cohort_date)
    GROUP BY 1, 2, 3, 4, 5
)

SELECT
    cohort_week,
    holdout_group,
    channel,
    segment,

    COUNT(DISTINCT user_id) AS cohort_size,

    -- LTV metrics by window
    AVG(revenue_7d) AS avg_ltv_7d,
    AVG(revenue_30d) AS avg_ltv_30d,
    AVG(revenue_90d) AS avg_ltv_90d,

    -- Engagement retention
    AVG(active_days_7d) AS avg_active_days_7d,
    AVG(active_days_30d) AS avg_active_days_30d,
    AVG(active_days_90d) AS avg_active_days_90d,

    -- Repeat behavior
    AVG(payment_events_90d) AS avg_payments_90d,
    AVG(CASE WHEN payment_events_90d >= 2 THEN 1.0 ELSE 0 END) AS repeat_payment_rate,

    -- Negative externalities
    AVG(unsubscribed) AS unsubscribe_rate,
    AVG(opted_out) AS opt_out_rate,
    AVG(app_deleted) AS app_delete_rate,

    -- Spend trajectory
    AVG(spend_7d) AS avg_spend_7d,
    AVG(spend_30d) AS avg_spend_30d,
    AVG(spend_90d) AS avg_spend_90d,

    -- On-time rate trajectory
    AVG(on_time_rate_7d) AS avg_on_time_rate_7d,
    AVG(on_time_rate_30d) AS avg_on_time_rate_30d,
    AVG(on_time_rate_90d) AS avg_on_time_rate_90d,

    -- Composite health: (LTV lift) - (fatigue penalty)
    AVG(revenue_90d) - (AVG(unsubscribed) + AVG(opted_out)) * AVG(revenue_90d) * {{ fatigue_penalty_weight }}
        AS health_adjusted_ltv_90d

FROM forward_engagement
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2, 3, 4;
