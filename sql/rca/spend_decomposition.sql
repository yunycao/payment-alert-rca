-- =============================================================================
-- ROOT CAUSE ANALYSIS: Spend Drop Decomposition
-- Decomposes week-over-week spend changes into mix shift vs rate change
-- using a Shapley-value-inspired additive decomposition.
--
-- For each dimension (segment, channel, propensity_decile, etc.),
-- ΔSpend = Σ_d [ (mix_shift_d × baseline_rate_d) + (rate_change_d × current_mix_d) ]
--        = MIX EFFECT + RATE EFFECT
-- =============================================================================

WITH current_period AS (
    SELECT
        po.user_id,
        ea.segment,
        ea.channel,
        ea.campaign_id,
        td.score_source,
        NTILE(10) OVER (ORDER BY ea.propensity_score) AS propensity_decile,
        CASE
            WHEN DATEDIFF(day, po.payment_due_date, ea.eligibility_date) BETWEEN 0 AND 3 THEN '0-3d'
            WHEN DATEDIFF(day, po.payment_due_date, ea.eligibility_date) BETWEEN 4 AND 7 THEN '4-7d'
            WHEN DATEDIFF(day, po.payment_due_date, ea.eligibility_date) BETWEEN 8 AND 14 THEN '8-14d'
            ELSE '15+d'
        END AS payment_due_bucket,
        po.payment_amount,
        po.payment_status,
        CASE WHEN po.payment_completed_date <= po.payment_due_date THEN 1 ELSE 0 END AS is_on_time
    FROM {{ database }}.{{ schema }}.payment_outcomes po
    INNER JOIN {{ database }}.{{ schema }}.messaging_eligibility ea
        ON po.user_id = ea.user_id
        AND ea.intent_name = '{{ intent_name }}'
    LEFT JOIN {{ database }}.{{ schema }}.targeting_decisions td
        ON ea.user_id = td.user_id AND ea.channel = td.channel
        AND td.intent_name = '{{ intent_name }}'
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
        NTILE(10) OVER (ORDER BY ea.propensity_score) AS propensity_decile,
        CASE
            WHEN DATEDIFF(day, po.payment_due_date, ea.eligibility_date) BETWEEN 0 AND 3 THEN '0-3d'
            WHEN DATEDIFF(day, po.payment_due_date, ea.eligibility_date) BETWEEN 4 AND 7 THEN '4-7d'
            WHEN DATEDIFF(day, po.payment_due_date, ea.eligibility_date) BETWEEN 8 AND 14 THEN '8-14d'
            ELSE '15+d'
        END AS payment_due_bucket,
        po.payment_amount,
        po.payment_status,
        CASE WHEN po.payment_completed_date <= po.payment_due_date THEN 1 ELSE 0 END AS is_on_time
    FROM {{ database }}.{{ schema }}.payment_outcomes po
    INNER JOIN {{ database }}.{{ schema }}.messaging_eligibility ea
        ON po.user_id = ea.user_id
        AND ea.intent_name = '{{ intent_name }}'
    LEFT JOIN {{ database }}.{{ schema }}.targeting_decisions td
        ON ea.user_id = td.user_id AND ea.channel = td.channel
        AND td.intent_name = '{{ intent_name }}'
    WHERE po.payment_due_date BETWEEN '{{ baseline_start }}' AND '{{ baseline_end }}'
      AND ea.eligibility_date BETWEEN '{{ baseline_start }}' AND '{{ baseline_end }}'
),

-- Decompose by each dimension
segment_decomposition AS (
    SELECT
        'segment' AS dimension,
        COALESCE(c.segment, b.segment) AS dimension_value,
        COALESCE(b.n_users, 0) AS baseline_users,
        COALESCE(c.n_users, 0) AS current_users,
        COALESCE(b.avg_spend, 0) AS baseline_avg_spend,
        COALESCE(c.avg_spend, 0) AS current_avg_spend,
        COALESCE(b.total_spend, 0) AS baseline_total_spend,
        COALESCE(c.total_spend, 0) AS current_total_spend,
        COALESCE(b.on_time_rate, 0) AS baseline_on_time_rate,
        COALESCE(c.on_time_rate, 0) AS current_on_time_rate
    FROM (
        SELECT segment, COUNT(*) AS n_users, AVG(payment_amount) AS avg_spend,
               SUM(payment_amount) AS total_spend, AVG(is_on_time) AS on_time_rate
        FROM current_period GROUP BY segment
    ) c
    FULL OUTER JOIN (
        SELECT segment, COUNT(*) AS n_users, AVG(payment_amount) AS avg_spend,
               SUM(payment_amount) AS total_spend, AVG(is_on_time) AS on_time_rate
        FROM baseline_period GROUP BY segment
    ) b ON c.segment = b.segment
),

channel_decomposition AS (
    SELECT
        'channel' AS dimension,
        COALESCE(c.channel, b.channel) AS dimension_value,
        COALESCE(b.n_users, 0) AS baseline_users,
        COALESCE(c.n_users, 0) AS current_users,
        COALESCE(b.avg_spend, 0) AS baseline_avg_spend,
        COALESCE(c.avg_spend, 0) AS current_avg_spend,
        COALESCE(b.total_spend, 0) AS baseline_total_spend,
        COALESCE(c.total_spend, 0) AS current_total_spend,
        COALESCE(b.on_time_rate, 0) AS baseline_on_time_rate,
        COALESCE(c.on_time_rate, 0) AS current_on_time_rate
    FROM (
        SELECT channel, COUNT(*) AS n_users, AVG(payment_amount) AS avg_spend,
               SUM(payment_amount) AS total_spend, AVG(is_on_time) AS on_time_rate
        FROM current_period GROUP BY channel
    ) c
    FULL OUTER JOIN (
        SELECT channel, COUNT(*) AS n_users, AVG(payment_amount) AS avg_spend,
               SUM(payment_amount) AS total_spend, AVG(is_on_time) AS on_time_rate
        FROM baseline_period GROUP BY channel
    ) b ON c.channel = b.channel
),

propensity_decomposition AS (
    SELECT
        'propensity_decile' AS dimension,
        COALESCE(c.propensity_decile, b.propensity_decile)::VARCHAR AS dimension_value,
        COALESCE(b.n_users, 0) AS baseline_users,
        COALESCE(c.n_users, 0) AS current_users,
        COALESCE(b.avg_spend, 0) AS baseline_avg_spend,
        COALESCE(c.avg_spend, 0) AS current_avg_spend,
        COALESCE(b.total_spend, 0) AS baseline_total_spend,
        COALESCE(c.total_spend, 0) AS current_total_spend,
        COALESCE(b.on_time_rate, 0) AS baseline_on_time_rate,
        COALESCE(c.on_time_rate, 0) AS current_on_time_rate
    FROM (
        SELECT propensity_decile, COUNT(*) AS n_users, AVG(payment_amount) AS avg_spend,
               SUM(payment_amount) AS total_spend, AVG(is_on_time) AS on_time_rate
        FROM current_period GROUP BY propensity_decile
    ) c
    FULL OUTER JOIN (
        SELECT propensity_decile, COUNT(*) AS n_users, AVG(payment_amount) AS avg_spend,
               SUM(payment_amount) AS total_spend, AVG(is_on_time) AS on_time_rate
        FROM baseline_period GROUP BY propensity_decile
    ) b ON c.propensity_decile = b.propensity_decile
),

due_bucket_decomposition AS (
    SELECT
        'payment_due_bucket' AS dimension,
        COALESCE(c.payment_due_bucket, b.payment_due_bucket) AS dimension_value,
        COALESCE(b.n_users, 0) AS baseline_users,
        COALESCE(c.n_users, 0) AS current_users,
        COALESCE(b.avg_spend, 0) AS baseline_avg_spend,
        COALESCE(c.avg_spend, 0) AS current_avg_spend,
        COALESCE(b.total_spend, 0) AS baseline_total_spend,
        COALESCE(c.total_spend, 0) AS current_total_spend,
        COALESCE(b.on_time_rate, 0) AS baseline_on_time_rate,
        COALESCE(c.on_time_rate, 0) AS current_on_time_rate
    FROM (
        SELECT payment_due_bucket, COUNT(*) AS n_users, AVG(payment_amount) AS avg_spend,
               SUM(payment_amount) AS total_spend, AVG(is_on_time) AS on_time_rate
        FROM current_period GROUP BY payment_due_bucket
    ) c
    FULL OUTER JOIN (
        SELECT payment_due_bucket, COUNT(*) AS n_users, AVG(payment_amount) AS avg_spend,
               SUM(payment_amount) AS total_spend, AVG(is_on_time) AS on_time_rate
        FROM baseline_period GROUP BY payment_due_bucket
    ) b ON c.payment_due_bucket = b.payment_due_bucket
),

score_source_decomposition AS (
    SELECT
        'score_source' AS dimension,
        COALESCE(c.score_source, b.score_source) AS dimension_value,
        COALESCE(b.n_users, 0) AS baseline_users,
        COALESCE(c.n_users, 0) AS current_users,
        COALESCE(b.avg_spend, 0) AS baseline_avg_spend,
        COALESCE(c.avg_spend, 0) AS current_avg_spend,
        COALESCE(b.total_spend, 0) AS baseline_total_spend,
        COALESCE(c.total_spend, 0) AS current_total_spend,
        COALESCE(b.on_time_rate, 0) AS baseline_on_time_rate,
        COALESCE(c.on_time_rate, 0) AS current_on_time_rate
    FROM (
        SELECT score_source, COUNT(*) AS n_users, AVG(payment_amount) AS avg_spend,
               SUM(payment_amount) AS total_spend, AVG(is_on_time) AS on_time_rate
        FROM current_period GROUP BY score_source
    ) c
    FULL OUTER JOIN (
        SELECT score_source, COUNT(*) AS n_users, AVG(payment_amount) AS avg_spend,
               SUM(payment_amount) AS total_spend, AVG(is_on_time) AS on_time_rate
        FROM baseline_period GROUP BY score_source
    ) b ON c.score_source = b.score_source
)

SELECT * FROM segment_decomposition
UNION ALL SELECT * FROM channel_decomposition
UNION ALL SELECT * FROM propensity_decomposition
UNION ALL SELECT * FROM due_bucket_decomposition
UNION ALL SELECT * FROM score_source_decomposition
ORDER BY dimension, dimension_value;
