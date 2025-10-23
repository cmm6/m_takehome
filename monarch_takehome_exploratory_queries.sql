-- Exposure Timing
SELECT treatment, 
  client_platform,
  MIN(exposed_at) as first_exposure,
  MAX(exposed_at) as last_exposure
FROM experiment_assisgnment
GROUP BY 1,2
;


-- ID Volume per treatment
SELECT e.TREATMENT,
  COUNT(DISTINCT e.ANONYMOUS_ID) as anon_ct,
  COUNT(DISTINCT u.USER_ID) as user_ct

FROM experiment_assisgnment e
LEFT JOIN user_events u ON e.ANONYMOUS_ID = u.ANONYMOUS_ID
GROUP BY TREATMENT
;


-- Check for many-to-one, etc. relationships
WITH multiple_anon AS (
  SELECT u.USER_ID,
    COUNT(DISTINCT e.ANONYMOUS_ID) as anon_ct
  FROM user_events u
  JOIN experiment_assisgnment e ON u.ANONYMOUS_ID = e.ANONYMOUS_ID 
  GROUP BY 1
)

SELECT COUNT(DISTINCT user_id)
FROM multiple_anon
WHERE anon_ct  > 1
;


-- Variant jumping on many to one: 
WITH variants AS (
        SELECT u.user_id, 
        COUNT(DISTINCT t.treatment) as variant_ct
        FROM user_events u
        JOIN experiment_assisgnment t 
        ON u.ANONYMOUS_ID = t.ANONYMOUS_ID
        GROUP BY 1
    )

SELECT CASE WHEN variant_ct > 1 THEN 'cross_contam' ELSE 'clean' END AS variant_jump,
  COUNT(DISTINCT user_id) as user_ct
FROM variants
GROUP BY 1
;



-- Funnel by Treatment
SELECT treatment,
  COUNT(DISTINCT entity_id) as anonymous_ct,
  COUNT(DISTINCT CASE WHEN ft_converted = 1 THEN entity_id END) as ft_conversion,
  ROUND(1.0 * COUNT(DISTINCT CASE WHEN ft_converted = 1 THEN entity_id END) / COUNT(DISTINCT entity_id), 4) AS ft_rate,
  COUNT(DISTINCT CASE WHEN ver_converted = 1 THEN entity_id END) as ver_conversion,
  ROUND(1.0 * COUNT(DISTINCT CASE WHEN ver_converted = 1 THEN entity_id END) / COUNT(DISTINCT entity_id), 4) AS ver_rate,
  COUNT(DISTINCT CASE WHEN sub_converted = 1 THEN entity_id END) as sub_conversion,
  ROUND(1.0 * COUNT(DISTINCT CASE WHEN sub_converted = 1 THEN entity_id END) / COUNT(DISTINCT entity_id), 4) AS sub_rate,
  COUNT(DISTINCT CASE WHEN ref_converted = 1 THEN entity_id END) as ref_conversion,
  ROUND(1.0 * COUNT(DISTINCT CASE WHEN ref_converted = 1 THEN entity_id END) / COUNT(DISTINCT entity_id), 4) AS ref_rate
FROM deduped_user_or_anon_exposures_deduped_events
GROUP BY 1
ORDER BY 1 DESC
;


-- Funnel by Treatment, Platform
SELECT CLIENT_PLATFORM,
  treatment,
  COUNT(DISTINCT entity_id) as anonymous_ct,
  COUNT(DISTINCT CASE WHEN ft_converted = 1 THEN entity_id END) as ft_conversion,
  ROUND(1.0 * COUNT(DISTINCT CASE WHEN ft_converted = 1 THEN entity_id END) / COUNT(DISTINCT entity_id), 4) AS ft_rate,
  COUNT(DISTINCT CASE WHEN ver_converted = 1 THEN entity_id END) as ver_conversion,
  ROUND(1.0 * COUNT(DISTINCT CASE WHEN ver_converted = 1 THEN entity_id END) / COUNT(DISTINCT entity_id), 4) AS ver_rate,
  COUNT(DISTINCT CASE WHEN sub_converted = 1 THEN entity_id END) as sub_conversion,
  ROUND(1.0 * COUNT(DISTINCT CASE WHEN sub_converted = 1 THEN entity_id END) / COUNT(DISTINCT entity_id), 4) AS sub_rate,
  COUNT(DISTINCT CASE WHEN ref_converted = 1 THEN entity_id END) as ref_conversion,
  ROUND(1.0 * COUNT(DISTINCT CASE WHEN ref_converted = 1 THEN entity_id END) / COUNT(DISTINCT entity_id), 4) AS ref_rate
FROM deduped_user_or_anon_exposures_deduped_events
GROUP BY 1,2
ORDER BY 1,2 DESC
;


-- LTV by treatment for users with LTV  
SELECT treatment,
  COUNT(DISTINCT user_id) AS user_ct,
  AVG(CAST(ltv as REAL)) AS mean_ltv
FROM user_level_deduped
WHERE ltv IS NOT NULL
GROUP BY 1
ORDER BY 1
; 


-- LTV by platform, treatment for users with LTV
SELECT client_platform,
  treatment,
  COUNT(DISTINCT user_id) AS user_ct,
  AVG(CAST(ltv as REAL)) AS mean_ltv
FROM user_level_deduped
WHERE ltv IS NOT NULL
GROUP BY 1, 2
ORDER BY 1, 2
;  


-- Billing period by platform, treatment for users with LTV
SELECT client_platform,
  INITIAL_BILLING_PERIOD,
  COUNT(DISTINCT user_id) AS user_ct,
  AVG(CAST(ltv as REAL)) AS mean_ltv
FROM user_level_deduped
WHERE ltv IS NOT NULL
GROUP BY 1, 2
ORDER BY 1, 2
;  


-- Free trial to subscription conversion by treatment, platform
SELECT treatment,
  client_platform,
  SUM(ft_converted) as ft_users,
  SUM(sub_converted) as sub_users 
FROM user_level_deduped
GROUP BY 1,2
;

-- Free trial to subscription conversion by treatment, platform, Promo Code
SELECT treatment,
  p.HAS_PROMO_CODE,
  client_platform,
  SUM(ft_converted) as ft_users,
  SUM(sub_converted) as sub_users 
FROM user_level_deduped e 
JOIN user_properties p ON e.user_id = p.user_id

GROUP BY 1,2,3
;

-- Free trial to subscription conversion by treatment, platform, MONARCH_ATTRIBUTION_SOURCE
WITH metrics AS (
    SELECT MONARCH_ATTRIBUTION_SOURCE,
      treatment,
      CLIENT_PLATFORM,
      COUNT(DISTINCT e.user_id ) as user_ct,
      COUNT(DISTINCT CASE WHEN ft_converted =1 THEN e.user_id END) AS ft_conversion,
      COUNT(DISTINCT CASE WHEN sub_converted=1 THEN e.user_id END) AS sub_conversion    
    FROM user_level_deduped e 
    GROUP BY 1,2,3
)
SELECT MONARCH_ATTRIBUTION_SOURCE,
  treatment,
  CLIENT_PLATFORM,
  user_ct,
  ft_conversion,
  ROUND(1.0 * ft_conversion / user_ct, 4) AS ft_rate,
  sub_conversion,
  ROUND(1.0 * sub_conversion / user_ct, 4) AS sub_rate
FROM metrics
ORDER BY ft_rate DESC
;


-- Free trial to subscription conversion by treatment, platform, USER_REPORTED_ATTRIBUTION
WITH metrics AS (
    SELECT USER_REPORTED_ATTRIBUTION,
      treatment,
      CLIENT_PLATFORM,
      COUNT(DISTINCT e.user_id ) as user_ct,
      COUNT(DISTINCT CASE WHEN ft_converted =1 THEN user_id END) AS ft_conversion,
      COUNT(DISTINCT CASE WHEN sub_converted=1 THEN user_id END) AS sub_conversion    
    FROM user_level_deduped e 
    GROUP BY 1,2,3
)
SELECT USER_REPORTED_ATTRIBUTION,
  treatment,
  CLIENT_PLATFORM,
  user_ct,
  ft_conversion,
  ROUND(1.0 * ft_conversion / user_ct, 4) AS ft_rate,
  sub_conversion,
  ROUND(1.0 * sub_conversion / user_ct, 4) AS sub_rate
FROM metrics
;


-- Canceled Trial by treatment, platform 
WITH metrics AS (
    SELECT CANCELED_DURING_TRIAL,
      treatment,
      CLIENT_PLATFORM,
      COUNT(DISTINCT e.user_id ) as user_ct,
      COUNT(DISTINCT CASE WHEN ft_converted =1 THEN e.user_id END) AS ft_conversion,
      COUNT(DISTINCT CASE WHEN sub_converted=1 THEN e.user_id END) AS sub_conversion    
    FROM user_level_deduped e 
    GROUP BY 1,2,3
)
SELECT CANCELED_DURING_TRIAL,
  treatment,
  CLIENT_PLATFORM,
  user_ct,
  ft_conversion,
  ROUND(1.0 * ft_conversion / user_ct, 4) AS ft_rate,
  sub_conversion,
  ROUND(1.0 * sub_conversion / user_ct, 4) AS sub_rate
FROM metrics
;


-- Free trial to subscription conversion by treatment, platform, CS Ticket During Trial
WITH metrics AS (
    SELECT HAD_CS_TICKET_DURING_TRIAL,
      treatment,
      CLIENT_PLATFORM,
      COUNT(DISTINCT user_id ) as user_ct,
      COUNT(DISTINCT CASE WHEN ft_converted =1 THEN user_id END) AS ft_conversion,
      COUNT(DISTINCT CASE WHEN sub_converted=1 THEN user_id END) AS sub_conversion    
    FROM user_level_deduped e 
    GROUP BY 1,2
)
SELECT HAD_CS_TICKET_DURING_TRIAL,
  treatment,
  CLIENT_PLATFORM,
  user_ct,
  ft_conversion,
  ROUND(1.0 * ft_conversion / user_ct, 4) AS ft_rate,
  sub_conversion,
  ROUND(1.0 * sub_conversion / user_ct, 4) AS sub_rate
FROM metrics
;


-- Attribution Source Comparison
WITH metrics AS (
    SELECT MONARCH_ATTRIBUTION_SOURCE,
      USER_REPORTED_ATTRIBUTION,
      treatment,
      COUNT(DISTINCT user_id ) as user_ct
    FROM user_level_deduped e 
    GROUP BY 1,2,3
)
SELECT USER_REPORTED_ATTRIBUTION,
  MONARCH_ATTRIBUTION_SOURCE,
  treatment,
  user_ct
FROM metrics
WHERE 1=1
;


-- Platform split among users 
SELECT client_platform,
COUNT(user_id)
FROM user_level_deduped
GROUP BY 1
;


-- Engagement Metrics by treatment and platform
SELECT treatment,
  client_platform,
  COUNT(DISTINCT user_id) AS user_ct,
  AVG(CAST(ltv as REAL)) AS mean_ltv,
  AVG(CAST(count_days_active_in_first_7_days_of_trial as REAL)) as mean_days,
  AVG(CAST(count_events_in_first_1_day_of_trial as REAL)) as mean_1day_events,
  AVG(CAST(count_events_in_first_7_days_of_trial as REAL)) as mean_7day_events
FROM user_level_deduped
WHERE 1=1
--AND ltv IS NOT NULL
GROUP BY 1,2
; 


-- UTM Source by count
SELECT utm_source
,COUNT(DISTINCT anonymous_id)
FROM experiment_assisgnment
GROUP BY 1
;
  


-- Funnel conversion by External Auth Provider
SELECT EXTERNAL_AUTH_PROVIDER,
  treatment,
  COUNT(DISTINCT entity_id) as anonymous_ct,
  COUNT(DISTINCT e.user_id) as user_ct, 
  COUNT(DISTINCT CASE WHEN ft_converted = 1 THEN entity_id END) as ft_conversion,
  ROUND(1.0 * COUNT(DISTINCT CASE WHEN ft_converted = 1 THEN entity_id END) / COUNT(DISTINCT entity_id), 4) AS ft_rate,
  COUNT(DISTINCT CASE WHEN ver_converted = 1 THEN entity_id END) as ver_conversion,
  ROUND(1.0 * COUNT(DISTINCT CASE WHEN ver_converted = 1 THEN entity_id END) / COUNT(DISTINCT entity_id), 4) AS ver_rate,
  COUNT(DISTINCT CASE WHEN sub_converted = 1 THEN entity_id END) as sub_conversion,
  ROUND(1.0 * COUNT(DISTINCT CASE WHEN sub_converted = 1 THEN entity_id END) / COUNT(DISTINCT entity_id), 4) AS sub_rate

FROM deduped_user_or_anon_exposures_deduped_events e
LEFT JOIN user_properties p ON e.user_id = p.user_id
GROUP BY 1,2
ORDER BY 1,2 DESC
;



-- Refund Rate
SELECT treatment,
  COUNT(DISTINCT entity_id) as anonymous_ct,
  COUNT(DISTINCT e.user_id) as user_ct, 
  COUNT(DISTINCT CASE WHEN ref_converted = 1 THEN entity_id END) as ref_conversion,
  ROUND(1.0 * COUNT(DISTINCT CASE WHEN ref_converted = 1 THEN entity_id END) / COUNT(DISTINCT entity_id), 4) AS ref_rate

FROM deduped_user_or_anon_exposures_deduped_events e
LEFT JOIN user_properties p ON e.user_id = p.user_id
GROUP BY 1
ORDER BY 1,2 DESC
;



