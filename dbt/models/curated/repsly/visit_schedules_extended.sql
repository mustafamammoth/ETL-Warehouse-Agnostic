-- Cleaned and standardized visit schedules extended data
-- dbt/models/curated/repsly/visit_schedules_extended.sql

{{ config(materialized='table') }}

WITH schedule_cleaning AS (
    SELECT 
        -- Primary identifiers
        schedule_code,
        TRIM(COALESCE(user_id, '')) AS user_id,
        
        -- Representative information (handle quoted names with tabs)
        CASE 
            WHEN representative_name LIKE '"%' AND representative_name LIKE '%"' 
            THEN TRIM(REPLACE(REPLACE(representative_name, '"', ''), '	', ' '))
            ELSE TRIM(COALESCE(representative_name, ''))
        END AS representative_name,
        
        -- Client information
        TRIM(COALESCE(client_code, '')) AS client_code,
        TRIM(COALESCE(client_name, '')) AS client_name,
        TRIM(COALESCE(street_address, '')) AS street_address,
        TRIM(COALESCE(zip_code, '')) AS zip_code,
        TRIM(COALESCE(city, '')) AS city,
        TRIM(COALESCE(state, '')) AS state,
        TRIM(COALESCE(country, '')) AS country,
        
        -- Schedule details
        TRIM(COALESCE(visit_note, '')) AS visit_note,
        TRIM(COALESCE(project_name, '')) AS project_name,
        TRIM(COALESCE(external_id, '')) AS external_id,

        -- Simple but robust date parsing
        CASE 
            WHEN scheduled_date_raw IS NOT NULL AND scheduled_date_raw <> '' THEN
                CASE 
                    WHEN LENGTH(scheduled_date_raw) = 10 AND scheduled_date_raw LIKE '____-__-__' THEN 
                        scheduled_date_raw::DATE
                    WHEN scheduled_date_raw LIKE '%/%' THEN 
                        TO_DATE(scheduled_date_raw, 'MM/DD/YY')
                    ELSE NULL
                END
        END AS scheduled_date,

        -- Duration and repeat settings
        CASE WHEN scheduled_duration_raw ~ '^[0-9]+$' 
             THEN scheduled_duration_raw::INTEGER 
             ELSE 0 END AS scheduled_duration_minutes,

        CASE WHEN repeat_every_weeks_raw ~ '^[0-9]+$' 
             THEN repeat_every_weeks_raw::INTEGER 
             ELSE 0 END AS repeat_every_weeks,

        -- Boolean parsing for alerts
        CASE WHEN UPPER(TRIM(COALESCE(alert_users_if_missed_raw, 'FALSE'))) = 'TRUE' THEN TRUE ELSE FALSE END AS alert_users_if_missed,
        CASE WHEN UPPER(TRIM(COALESCE(alert_users_if_late_raw, 'FALSE'))) = 'TRUE' THEN TRUE ELSE FALSE END AS alert_users_if_late,
        CASE WHEN UPPER(TRIM(COALESCE(alert_users_if_done_raw, 'FALSE'))) = 'TRUE' THEN TRUE ELSE FALSE END AS alert_users_if_done,

        -- Metadata
        extracted_at,
        source_system,
        endpoint

    FROM {{ ref('visit_schedules_extended_raw') }}
),

schedule_enhancement AS (
    SELECT
        *,
        
        -- Address and location analysis
        CASE WHEN street_address <> '' AND city <> '' AND state <> '' THEN
                 CONCAT_WS(', ', street_address, city, state, zip_code, country)
        END AS full_address,

        -- State and country standardization
        CASE
            WHEN country IN ('US', 'United States of America', 'United States') AND state = 'CA' THEN 'California'
            WHEN country IN ('US', 'United States of America', 'United States') AND state = 'NY' THEN 'New York'
            WHEN country IN ('US', 'United States of America', 'United States') AND state = 'TX' THEN 'Texas'
            WHEN country IN ('US', 'United States of America', 'United States') AND state = 'FL' THEN 'Florida'
            ELSE state
        END AS state_standardized,

        CASE
            WHEN country IN ('US', 'United States of America') THEN 'United States'
            ELSE country
        END AS country_standardized,

        -- Parse client DBA information
        CASE WHEN client_name LIKE '%DBA:%' 
             THEN TRIM(SUBSTRING(client_name FROM POSITION('DBA:' IN client_name) + 4))
             ELSE client_name
        END AS dba_name,

        CASE WHEN client_name LIKE '%DBA:%' 
             THEN TRIM(SUBSTRING(client_name FROM 1 FOR POSITION('DBA:' IN client_name) - 1))
             ELSE client_name
        END AS legal_name,

        -- Date analysis
        EXTRACT(DOW FROM scheduled_date) AS scheduled_day_of_week,
        EXTRACT(YEAR FROM scheduled_date) AS scheduled_year,
        EXTRACT(MONTH FROM scheduled_date) AS scheduled_month,

        -- Business flags
        (visit_note <> '') AS has_visit_notes,
        (project_name <> '') AS has_project_assignment,
        (repeat_every_weeks > 0) AS is_recurring_schedule,
        (alert_users_if_missed OR alert_users_if_late OR alert_users_if_done) AS has_any_alerts,
        (street_address <> '' AND city <> '' AND state <> '') AS has_complete_address

    FROM schedule_cleaning
),

schedule_business_logic AS (
    SELECT
        se.*,

        -- Day of week analysis
        CASE se.scheduled_day_of_week
            WHEN 0 THEN 'Sunday'
            WHEN 1 THEN 'Monday'
            WHEN 2 THEN 'Tuesday'
            WHEN 3 THEN 'Wednesday'
            WHEN 4 THEN 'Thursday'
            WHEN 5 THEN 'Friday'
            WHEN 6 THEN 'Saturday'
        END AS scheduled_day_of_week_name,

        CASE 
            WHEN se.scheduled_day_of_week IN (0, 6) THEN 'Weekend'
            ELSE 'Weekday'
        END AS work_day_type,

        -- Duration categorization
        CASE
            WHEN se.scheduled_duration_minutes = 0 THEN 'No Duration Set'
            WHEN se.scheduled_duration_minutes <= 15 THEN 'Quick Visit (≤15 min)'
            WHEN se.scheduled_duration_minutes <= 30 THEN 'Short Visit (16-30 min)'
            WHEN se.scheduled_duration_minutes <= 60 THEN 'Standard Visit (31-60 min)'
            ELSE 'Long Visit (60+ min)'
        END AS duration_category,

        -- Schedule type classification
        CASE
            WHEN se.is_recurring_schedule = TRUE THEN 'Recurring Schedule'
            WHEN se.has_project_assignment = TRUE THEN 'Project-Based'
            ELSE 'One-time Visit'
        END AS schedule_type,

        -- Geographic region analysis
        CASE 
            WHEN se.state_standardized = 'California' THEN 'California'
            WHEN se.state_standardized = 'New York' THEN 'New York'
            WHEN se.state_standardized IN ('Texas', 'Florida') THEN se.state_standardized
            ELSE 'Other States'
        END AS geographic_region,

        -- Client business classification
        CASE
            WHEN se.dba_name != se.legal_name THEN 'Corporate Client (DBA)'
            WHEN se.client_name LIKE '%LLC%' OR se.client_name LIKE '%Inc%' OR se.client_name LIKE '%Corp%' THEN 'Business Entity'
            ELSE 'Standard Business'
        END AS client_business_type,

        -- Alert configuration analysis
        CASE
            WHEN se.alert_users_if_missed AND se.alert_users_if_late AND se.alert_users_if_done THEN 'Full Alert Coverage'
            WHEN se.alert_users_if_missed OR se.alert_users_if_late THEN 'Monitoring Alerts'
            WHEN se.alert_users_if_done THEN 'Completion Tracking'
            ELSE 'No Alerts'
        END AS alert_configuration,

        -- Schedule complexity assessment
        CASE
            WHEN se.is_recurring_schedule AND se.has_project_assignment AND se.scheduled_duration_minutes > 0 THEN 'Complex Schedule'
            WHEN (se.is_recurring_schedule AND se.has_project_assignment) OR (se.scheduled_duration_minutes > 0 AND se.has_project_assignment) THEN 'Structured Schedule'
            WHEN se.is_recurring_schedule OR se.has_project_assignment OR se.scheduled_duration_minutes > 0 THEN 'Basic Schedule'
            ELSE 'Simple Schedule'
        END AS schedule_complexity,

        -- Representative workload indicator (based on schedule count)
        CASE
            WHEN se.representative_name IN (
                SELECT representative_name 
                FROM schedule_enhancement 
                GROUP BY representative_name 
                HAVING COUNT(*) >= 15
            ) THEN 'High Volume Rep'
            WHEN se.representative_name IN (
                SELECT representative_name 
                FROM schedule_enhancement 
                GROUP BY representative_name 
                HAVING COUNT(*) BETWEEN 8 AND 14
            ) THEN 'Medium Volume Rep'
            ELSE 'Low Volume Rep'
        END AS rep_workload_category,

        -- Data completeness assessment
        CASE
            WHEN se.has_complete_address AND se.scheduled_duration_minutes > 0 AND se.has_project_assignment THEN 'Complete Data'
            WHEN se.has_complete_address AND (se.scheduled_duration_minutes > 0 OR se.has_project_assignment) THEN 'Good Data'
            WHEN se.has_complete_address OR se.scheduled_duration_minutes > 0 OR se.has_project_assignment THEN 'Partial Data'
            ELSE 'Minimal Data'
        END AS data_completeness_level,

        -- Data quality score (0-100)
        (
            CASE WHEN se.client_code <> '' THEN 20 ELSE 0 END +
            CASE WHEN se.representative_name <> '' THEN 20 ELSE 0 END +
            CASE WHEN se.has_complete_address = TRUE THEN 20 ELSE 0 END +
            CASE WHEN se.scheduled_date IS NOT NULL THEN 20 ELSE 0 END +
            CASE WHEN se.scheduled_duration_minutes > 0 THEN 10 ELSE 0 END +
            CASE WHEN se.has_project_assignment = TRUE THEN 10 ELSE 0 END
        ) AS data_completeness_score

    FROM schedule_enhancement se
)

SELECT *
FROM schedule_business_logic
ORDER BY scheduled_date DESC, schedule_code