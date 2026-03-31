-- =============================================================================
-- CAMPAIGN IMPRESSION TAKEOVER ANALYSIS
-- Detects when a single large campaign monopolizes messaging impression
-- inventory, crowding out other payment alert campaigns
-- =============================================================================

WITH daily_impressions AS (
    SELECT
        d.send_date AS report_date,
        d.channel,
        d.campaign_id,
        d.campaign_name,
        COUNT(DISTINCT d.user_id) AS users_messaged,
        COUNT(*) AS total_impressions,
        SUM(CASE WHEN e.event_type IN ('open', 'impression') THEN 1 ELSE 0 END) AS actual_impressions,
        SUM(CASE WHEN e.event_type = 'click' THEN 1 ELSE 0 END) AS clicks,
        SUM(COALESCE(cv.revenue_amount, 0)) AS revenue
    FROM {{ database }}.{{ schema }}.message_delivery d
    LEFT JOIN {{ database }}.{{ schema }}.message_engagement e
        ON d.message_id = e.message_id
    LEFT JOIN {{ database }}.{{ schema }}.conversions cv
        ON d.message_id = cv.attribution_message_id
    WHERE d.intent_name = '{{ intent_name }}'
      AND d.send_date BETWEEN '{{ start_date }}' AND '{{ end_date }}'
      AND d.delivery_status IN ('sent', 'delivered')
    GROUP BY 1, 2, 3, 4
),

daily_totals AS (
    SELECT
        report_date,
        channel,
        SUM(users_messaged) AS total_users,
        SUM(total_impressions) AS total_msgs,
        COUNT(DISTINCT campaign_id) AS active_campaigns
    FROM daily_impressions
    GROUP BY 1, 2
),

campaign_share AS (
    SELECT
        di.report_date,
        di.channel,
        di.campaign_id,
        di.campaign_name,
        di.users_messaged,
        di.total_impressions,
        di.actual_impressions,
        di.clicks,
        di.revenue,
        dt.total_users,
        dt.total_msgs,
        dt.active_campaigns,

        -- Impression share
        ROUND(di.total_impressions * 1.0 / NULLIF(dt.total_msgs, 0), 4) AS impression_share,

        -- Performance metrics
        ROUND(di.actual_impressions * 1.0 / NULLIF(di.total_impressions, 0), 4) AS open_rate,
        ROUND(di.clicks * 1.0 / NULLIF(di.actual_impressions, 0), 4) AS ctr,
        ROUND(di.revenue * 1.0 / NULLIF(di.users_messaged, 0), 2) AS revenue_per_user
    FROM daily_impressions di
    JOIN daily_totals dt ON di.report_date = dt.report_date AND di.channel = dt.channel
),

-- Herfindahl-Hirschman Index for concentration
hhi_calculation AS (
    SELECT
        report_date,
        channel,
        active_campaigns,
        SUM(POWER(impression_share, 2)) AS hhi_index,
        MAX(impression_share) AS max_single_campaign_share,
        MAX(CASE WHEN impression_share = (
            SELECT MAX(impression_share)
            FROM campaign_share cs2
            WHERE cs2.report_date = campaign_share.report_date AND cs2.channel = campaign_share.channel
        ) THEN campaign_id END) AS dominant_campaign_id,
        MAX(CASE WHEN impression_share = (
            SELECT MAX(impression_share)
            FROM campaign_share cs2
            WHERE cs2.report_date = campaign_share.report_date AND cs2.channel = campaign_share.channel
        ) THEN campaign_name END) AS dominant_campaign_name
    FROM campaign_share
    GROUP BY 1, 2, 3
)

SELECT
    cs.report_date,
    cs.channel,
    cs.campaign_id,
    cs.campaign_name,
    cs.users_messaged,
    cs.total_impressions,
    cs.impression_share,
    cs.open_rate,
    cs.ctr,
    cs.revenue_per_user,
    cs.revenue,
    cs.active_campaigns,
    h.hhi_index,
    h.max_single_campaign_share,
    h.dominant_campaign_id,
    h.dominant_campaign_name,

    -- Takeover flags
    CASE WHEN cs.impression_share > {{ impression_share_threshold }} THEN TRUE ELSE FALSE END AS is_takeover_campaign,
    CASE WHEN h.hhi_index > {{ hhi_threshold }} THEN TRUE ELSE FALSE END AS is_concentrated_day,
    CASE WHEN h.active_campaigns < {{ min_campaigns }} THEN TRUE ELSE FALSE END AS low_campaign_diversity

FROM campaign_share cs
JOIN hhi_calculation h ON cs.report_date = h.report_date AND cs.channel = h.channel
ORDER BY cs.report_date, cs.channel, cs.impression_share DESC;
