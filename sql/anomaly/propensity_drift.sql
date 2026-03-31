-- =============================================================================
-- PROPENSITY SCORE DRIFT DETECTION
-- Compares score distributions between reference and detection windows
-- to identify model drift affecting payment alert targeting
-- =============================================================================

WITH reference_scores AS (
    -- Baseline distribution from the reference window
    SELECT
        e.user_id,
        e.propensity_score,
        e.model_version,
        e.channel,
        e.segment,
        NTILE({{ percentile_bins }}) OVER (
            PARTITION BY e.channel
            ORDER BY e.propensity_score
        ) AS score_decile
    FROM {{ database }}.{{ schema }}.messaging_eligibility e
    WHERE e.intent_name = '{{ intent_name }}'
      AND e.eligibility_date BETWEEN
          DATEADD(day, -{{ reference_window_days }}, '{{ detection_start }}')
          AND DATEADD(day, -1, '{{ detection_start }}')
),

detection_scores AS (
    -- Current distribution from the detection window
    SELECT
        e.user_id,
        e.propensity_score,
        e.model_version,
        e.channel,
        e.segment,
        NTILE({{ percentile_bins }}) OVER (
            PARTITION BY e.channel
            ORDER BY e.propensity_score
        ) AS score_decile
    FROM {{ database }}.{{ schema }}.messaging_eligibility e
    WHERE e.intent_name = '{{ intent_name }}'
      AND e.eligibility_date BETWEEN '{{ detection_start }}' AND '{{ detection_end }}'
),

-- Reference distribution summary
ref_distribution AS (
    SELECT
        channel,
        score_decile,
        COUNT(*) AS ref_count,
        COUNT(*) * 1.0 / SUM(COUNT(*)) OVER (PARTITION BY channel) AS ref_pct,
        AVG(propensity_score) AS ref_avg_score,
        MIN(propensity_score) AS ref_min_score,
        MAX(propensity_score) AS ref_max_score,
        STDDEV(propensity_score) AS ref_stddev
    FROM reference_scores
    GROUP BY 1, 2
),

-- Detection distribution summary
det_distribution AS (
    SELECT
        channel,
        score_decile,
        COUNT(*) AS det_count,
        COUNT(*) * 1.0 / SUM(COUNT(*)) OVER (PARTITION BY channel) AS det_pct,
        AVG(propensity_score) AS det_avg_score,
        MIN(propensity_score) AS det_min_score,
        MAX(propensity_score) AS det_max_score,
        STDDEV(propensity_score) AS det_stddev
    FROM detection_scores
    GROUP BY 1, 2
)

-- PSI CALCULATION PER DECILE
SELECT
    COALESCE(r.channel, d.channel) AS channel,
    COALESCE(r.score_decile, d.score_decile) AS score_decile,

    -- Distribution comparison
    r.ref_count,
    d.det_count,
    r.ref_pct,
    d.det_pct,

    -- PSI component: (det_pct - ref_pct) * ln(det_pct / ref_pct)
    CASE
        WHEN r.ref_pct > 0 AND d.det_pct > 0
        THEN (d.det_pct - r.ref_pct) * LN(d.det_pct / r.ref_pct)
        ELSE 0
    END AS psi_component,

    -- Score range shifts
    r.ref_avg_score,
    d.det_avg_score,
    d.det_avg_score - r.ref_avg_score AS avg_score_shift,
    r.ref_stddev,
    d.det_stddev,

    -- Model version tracking
    (SELECT LISTAGG(DISTINCT model_version, ', ') FROM reference_scores WHERE channel = r.channel) AS ref_model_versions,
    (SELECT LISTAGG(DISTINCT model_version, ', ') FROM detection_scores WHERE channel = d.channel) AS det_model_versions

FROM ref_distribution r
FULL OUTER JOIN det_distribution d
    ON r.channel = d.channel
    AND r.score_decile = d.score_decile
ORDER BY channel, score_decile;
