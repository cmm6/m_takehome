-- Create assignments table without variant jumpers
CREATE TABLE clean_assignments AS
    WITH user_assignments AS (
      SELECT u.user_id,
        COUNT(DISTINCT e.treatment) AS n_variants
      FROM user_events u
      JOIN experiment_assisgnment e ON u.ANONYMOUS_ID = e.ANONYMOUS_ID
      WHERE e.treatment IN ('ca', 'va')
      GROUP BY 1
    )
    SELECT * 
    FROM user_assignments WHERE n_variants = 1
;

-- Create merged experiment assignment and user event table, without variant jumpers
CREATE TABLE clean_experiment_assignment AS
    WITH user_events_clean AS (
        SELECT u.*
        FROM user_events u 
        JOIN clean_assignments c ON u.user_id = c.user_id
        )
    
    SELECT e.*, 
        u.*
    FROM experiment_assisgnment e
    LEFT JOIN user_events_clean u ON e.anonymous_id = u.anonymous_id
;


-- Create deduped first exposure among clean assignments
CREATE TABLE deduped_first_exposure AS 
    WITH first_exposure AS (
      SELECT COALESCE(user_id, anonymous_id) AS id,
          FIRST_VALUE(treatment) OVER (PARTITION BY COALESCE(user_id, anonymous_id) ORDER BY EXPOSED_AT ASC) AS first_treatment,
          FIRST_VALUE(client_platform) OVER (PARTITION BY COALESCE(user_id, anonymous_id) ORDER BY EXPOSED_AT ASC) AS first_platform,
          FIRST_VALUE(EXPOSED_AT) OVER (PARTITION BY COALESCE(user_id, anonymous_id) ORDER BY EXPOSED_AT ASC) AS first_exposed_at
      FROM clean_experiment_assignment
    )
    SELECT DISTINCT id, first_treatment, first_platform, first_exposed_at FROM first_exposure
  ;


-- Clean user events x user properties, with deduped users and no
CREATE TABLE user_level_deduped AS 
    WITH user_level_events AS (
        SELECT user_id,
            MAX(CASE WHEN free_trial_started_at IS NOT NULL THEN 1 ELSE 0 END) AS ft_converted,
            MAX(CASE WHEN subscription_started_at IS NOT NULL THEN 1 ELSE 0 END) AS sub_converted,
            MAX(CASE WHEN subscription_refunded_at IS NOT NULL THEN 1 ELSE 0 END) AS refunded
        FROM user_events
        WHERE user_id IS NOT NULL
        GROUP BY user_id
    ),

    user_level AS (
        SELECT p.user_id,
            d.first_treatment AS treatment,
            d.first_platform AS client_platform,
            e.ft_converted,
            e.sub_converted,
            e.refunded,
            p.ltv,
            p.INITIAL_BILLING_PERIOD,
            p.HAS_PROMO_CODE,
            p.HAD_CS_TICKET_DURING_TRIAL,
            p.MONARCH_ATTRIBUTION_SOURCE,
            p.USER_REPORTED_ATTRIBUTION,
            p.CANCELED_DURING_TRIAL,
            p.count_days_active_in_first_7_days_of_trial,
            p.count_events_in_first_7_days_of_trial,
            p.count_events_in_first_1_day_of_trial
        FROM user_properties p
        LEFT JOIN user_level_events e ON p.user_id = e.user_id
        LEFT JOIN deduped_first_exposure d ON p.user_id = d.id
    )

    SELECT *
    FROM user_level
;


-- Create a unified identity to resolve many to 1 in user_events
CREATE TABLE deduped_user_or_anon_exposures AS
    WITH resolved_ids AS (
        SELECT COALESCE(user_id, anonymous_id) AS entity_id,
            user_id,
            anonymous_id,
            treatment,
            client_platform,
            exposed_at,
            ROW_NUMBER() OVER ( PARTITION BY COALESCE(user_id, anonymous_id) ORDER BY exposed_at ASC) AS rn
        FROM clean_experiment_assignment
    ),
    deduped AS (
        SELECT entity_id,
            user_id,
            anonymous_id,
            treatment AS first_treatment,
            client_platform AS first_platform,
            exposed_at AS first_exposed_at
        FROM resolved_ids
        WHERE rn = 1
    )
    SELECT entity_id AS id, 
        user_id,
        anonymous_id,
        first_treatment,
        first_platform,
        first_exposed_at
    FROM deduped
;

-- max events for users w/more than one anonymous ID
CREATE TABLE deduped_user_events AS 
WITH event_max AS (
    SELECT
        COALESCE(user_id, anonymous_id) AS entity_id,
        MAX(CASE WHEN USER_VERIFIED_AT IS NOT NULL THEN 1 ELSE 0 END) AS ver_converted,
        MAX(CASE WHEN free_trial_started_at IS NOT NULL THEN 1 ELSE 0 END) AS ft_converted,
        MAX(CASE WHEN subscription_started_at IS NOT NULL THEN 1 ELSE 0 END) AS sub_converted,
        MAX(CASE WHEN subscription_refunded_at IS NOT NULL THEN 1 ELSE 0 END) AS refunded
    FROM user_events
    GROUP BY COALESCE(user_id, anonymous_id)
)
SELECT * FROM event_max;

-- Combine user events with deduped exposures and unified entity key
CREATE TABLE deduped_user_or_anon_exposures_deduped_events AS
    SELECT d.id AS entity_id,
        d.user_id,
        d.anonymous_id,
        d.first_treatment AS treatment,
        d.first_platform AS client_platform,
        d.first_exposed_at,
        CASE WHEN e.ver_converted IS NOT NULL THEN e.ver_converted ELSE 0 END as ver_converted,
        CASE WHEN e.ft_converted IS NOT NULL THEN e.ft_converted ELSE 0 END as ft_converted,
        CASE WHEN e.sub_converted IS NOT NULL THEN e.sub_converted ELSE 0 END as sub_converted,
        CASE WHEN e.refunded IS NOT NULL THEN e.refunded ELSE 0 END as ref_converted
    FROM deduped_user_or_anon_exposures d
    LEFT JOIN deduped_user_events e
        ON d.id = e.entity_id
;
  
  

-- Check for many to 1
SELECT user_id, 
    COUNT(*) 
FROM deduped_user_or_anon_exposures_deduped_events 
GROUP BY 1 
ORDER BY 2 DESC;
  

-- Dashboard Export Table
CREATE TABLE datastudio_export AS 
    SELECT u.*, 
        p.*
    FROM deduped_user_or_anon_exposures_deduped_events u
    LEFT JOIN user_properties p 
        ON u.entity_id = p.user_id
;

