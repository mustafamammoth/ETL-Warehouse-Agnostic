-- Cleaned and standardized visit schedule realizations data
-- dbt/models/curated/repsly/visit_schedule_realizations.sql

{{ config(materialized='table') }}

WITH schedule_cleaning AS (
    SELECT 
        -- IDs and codes
        CASE WHEN schedule_id IS NOT NULL AND schedule_id <> '' 
             THEN schedule_id END AS schedule_id,
        CASE WHEN project_id IS NOT NULL AND project_id <> '' 
             THEN project_id END AS project_id,
        employee_id,
        TRIM(COALESCE(employee_code, '')) AS employee_code,
        place_id,
        TRIM(COALESCE(place_code, '')) AS place_code,

        -- Timezone and location
        TRIM(COALESCE(time_zone, '')) AS time_zone,
        TRIM(COALESCE(schedule_note, '')) AS schedule_note,

        -- Status cleaning
        LOWER(TRIM(COALESCE(status_raw, 'unknown'))) AS status,

        -- Parse datetime fields (ISO 8601 format)
        CASE WHEN modified_utc_raw IS NOT NULL AND modified_utc_raw <> '' 
             THEN modified_utc_raw::TIMESTAMP END AS modified_utc,

        CASE WHEN date_time_start_raw IS NOT NULL AND date_time_start_raw <> '' 
             THEN date_time_start_raw::TIMESTAMP END AS actual_start_datetime,
        CASE WHEN date_time_start_utc_raw IS NOT NULL AND date_time_start_utc_raw <> '' 
             THEN date_time_start_utc_raw::TIMESTAMP END AS actual_start_utc,

        CASE WHEN date_time_end_raw IS NOT NULL AND date_time_end_raw <> '' 
             THEN date_time_end_raw::TIMESTAMP END AS actual_end_datetime,
        CASE WHEN date_time_end_utc_raw IS NOT NULL AND date_time_end_utc_raw <> '' 
             THEN date_time_end_utc_raw::TIMESTAMP END AS actual_end_utc,

        CASE WHEN plan_date_time_start_raw IS NOT NULL AND plan_date_time_start_raw <> '' 
             THEN plan_date_time_start_raw::TIMESTAMP END AS planned_start_datetime,
        CASE WHEN plan_date_time_start_utc_raw IS NOT NULL AND plan_date_time_start_utc_raw <> '' 
             THEN plan_date_time_start_utc_raw::TIMESTAMP END AS planned_start_utc,

        CASE WHEN plan_date_time_end_raw IS NOT NULL AND plan_date_time_end_raw <> '' 
             THEN plan_date_time_end_raw::TIMESTAMP END AS planned_end_datetime,
        CASE WHEN plan_date_time_end_utc_raw IS NOT NULL AND plan_date_time_end_utc_raw <> '' 
             THEN plan_date_time_end_utc_raw::TIMESTAMP END AS planned_end_utc,

        -- Tasks (could be JSON or other format)
        CASE WHEN tasks_raw IS NOT NULL AND tasks_raw <> '' 
             THEN tasks_raw END AS tasks,

        -- Metadata
        extracted_at,
        source_system,
        endpoint

    FROM {{ ref('visit_schedule_realizations_raw') }}
),

schedule_enhancement AS (
    SELECT
        *,
        
        -- Extract dates from datetimes
        CASE WHEN actual_start_datetime IS NOT NULL 
             THEN DATE(actual_start_datetime) END AS actual_date,
        CASE WHEN planned_start_datetime IS NOT NULL 
             THEN DATE(planned_start_datetime) END AS planned_date,

        -- Duration calculations
        CASE WHEN actual_start_datetime IS NOT NULL AND actual_end_datetime IS NOT NULL
             THEN EXTRACT(EPOCH FROM (actual_end_datetime - actual_start_datetime)) / 60
        END AS actual_duration_minutes,

        CASE WHEN planned_start_datetime IS NOT NULL AND planned_end_datetime IS NOT NULL
             THEN EXTRACT(EPOCH FROM (planned_end_datetime - planned_start_datetime)) / 60
        END AS planned_duration_minutes,

        -- Variance calculations (actual vs planned)
        CASE WHEN actual_start_datetime IS NOT NULL AND planned_start_datetime IS NOT NULL
             THEN EXTRACT(EPOCH FROM (actual_start_datetime - planned_start_datetime)) / 60
        END AS start_variance_minutes,

        CASE WHEN actual_end_datetime IS NOT NULL AND planned_end_datetime IS NOT NULL
             THEN EXTRACT(EPOCH FROM (actual_end_datetime - planned_end_datetime)) / 60
        END AS end_variance_minutes,

        -- Time zone standardization
        CASE 
            WHEN time_zone = 'America/New_York' THEN 'Eastern'
            WHEN time_zone = 'America/Los_Angeles' THEN 'Pacific'
            WHEN time_zone = 'America/Chicago' THEN 'Central'
            WHEN time_zone = 'America/Denver' THEN 'Mountain'
            ELSE time_zone
        END AS time_zone_standard,

        -- Employee name parsing (handle quoted names)
        CASE 
            WHEN employee_code LIKE '"%' AND employee_code LIKE '%"' 
            THEN TRIM(REPLACE(REPLACE(employee_code, '"', ''), '	', ' '))
            ELSE TRIM(employee_code)
        END AS employee_name_clean,

        -- Data completeness flags
        (schedule_id IS NOT NULL) AS has_schedule_id,
        (project_id IS NOT NULL) AS has_project_id,
        (actual_start_datetime IS NOT NULL) AS has_actual_times,
        (planned_start_datetime IS NOT NULL) AS has_planned_times,
        (schedule_note <> '') AS has_notes

    FROM schedule_cleaning
),

schedule_business_logic AS (
    SELECT
        se.*,

        -- Time analysis
        EXTRACT(HOUR FROM se.actual_start_datetime) AS actual_start_hour,
        EXTRACT(DOW FROM se.actual_date) AS actual_day_of_week,
        EXTRACT(YEAR FROM se.actual_date) AS actual_year,
        EXTRACT(MONTH FROM se.actual_date) AS actual_month,

        -- Time of day categorization
        CASE
            WHEN EXTRACT(HOUR FROM se.actual_start_datetime) BETWEEN 6 AND 11 THEN 'Morning'
            WHEN EXTRACT(HOUR FROM se.actual_start_datetime) BETWEEN 12 AND 17 THEN 'Afternoon' 
            WHEN EXTRACT(HOUR FROM se.actual_start_datetime) BETWEEN 18 AND 21 THEN 'Evening'
            ELSE 'Other'
        END AS actual_time_of_day,

        -- Day of week name
        CASE EXTRACT(DOW FROM se.actual_date)
            WHEN 0 THEN 'Sunday'
            WHEN 1 THEN 'Monday'
            WHEN 2 THEN 'Tuesday'
            WHEN 3 THEN 'Wednesday'
            WHEN 4 THEN 'Thursday'
            WHEN 5 THEN 'Friday'
            WHEN 6 THEN 'Saturday'
        END AS actual_day_of_week_name,

        -- Status standardization
        CASE se.status
            WHEN 'planned' THEN 'Planned'
            WHEN 'unplanned' THEN 'Unplanned'
            WHEN 'missed' THEN 'Missed'
            WHEN 'cancelled' THEN 'Cancelled'
            WHEN 'completed' THEN 'Completed'
            ELSE 'Unknown'
        END AS visit_status,

        -- Visit type classification
        CASE 
            WHEN se.status = 'planned' AND se.has_actual_times = TRUE THEN 'Planned & Executed'
            WHEN se.status = 'planned' AND se.has_actual_times = FALSE THEN 'Planned Only'
            WHEN se.status = 'unplanned' THEN 'Unplanned Visit'
            WHEN se.status = 'missed' THEN 'Missed Visit'
            ELSE 'Other'
        END AS visit_type,

        -- Duration categorization
        CASE
            WHEN se.actual_duration_minutes IS NULL THEN 'No Duration Data'
            WHEN se.actual_duration_minutes < 5 THEN 'Very Short (< 5 min)'
            WHEN se.actual_duration_minutes < 15 THEN 'Short (5-15 min)'
            WHEN se.actual_duration_minutes < 30 THEN 'Medium (15-30 min)'
            WHEN se.actual_duration_minutes < 60 THEN 'Long (30-60 min)'
            ELSE 'Very Long (60+ min)'
        END AS duration_category,

        -- Timing variance analysis
        CASE
            WHEN se.start_variance_minutes IS NULL THEN 'No Comparison'
            WHEN se.start_variance_minutes BETWEEN -15 AND 15 THEN 'On Time'
            WHEN se.start_variance_minutes > 15 THEN 'Late Start'
            WHEN se.start_variance_minutes < -15 THEN 'Early Start'
        END AS timing_performance,

        -- Schedule adherence
        CASE
            WHEN se.status = 'missed' THEN 'Poor Adherence'
            WHEN se.status = 'planned' AND se.has_actual_times = FALSE THEN 'No Execution'
            WHEN se.start_variance_minutes IS NOT NULL AND ABS(se.start_variance_minutes) <= 15 THEN 'Good Adherence'
            WHEN se.start_variance_minutes IS NOT NULL AND ABS(se.start_variance_minutes) <= 60 THEN 'Fair Adherence'
            ELSE 'Poor Adherence'
        END AS schedule_adherence,

        -- Data quality assessment
        CASE
            WHEN se.has_schedule_id AND se.has_project_id AND se.has_actual_times AND se.has_planned_times THEN 'Complete Data'
            WHEN se.has_actual_times AND se.has_planned_times THEN 'Good Data'
            WHEN se.has_actual_times OR se.has_planned_times THEN 'Partial Data'
            ELSE 'Limited Data'
        END AS data_quality,

        -- Work pattern analysis
        CASE 
            WHEN EXTRACT(DOW FROM se.actual_date) IN (0, 6) THEN 'Weekend'
            ELSE 'Weekday'
        END AS work_day_type,

        -- Geographic region (based on timezone)
        CASE 
            WHEN se.time_zone_standard = 'Pacific' THEN 'West Coast'
            WHEN se.time_zone_standard = 'Mountain' THEN 'Mountain Region'
            WHEN se.time_zone_standard = 'Central' THEN 'Central Region'
            WHEN se.time_zone_standard = 'Eastern' THEN 'East Coast'
            ELSE 'Other Region'
        END AS geographic_region,

        -- Data completeness score (0-100)
        (
            CASE WHEN se.has_schedule_id = TRUE THEN 15 ELSE 0 END +
            CASE WHEN se.has_project_id = TRUE THEN 15 ELSE 0 END +
            CASE WHEN se.employee_code <> '' THEN 15 ELSE 0 END +
            CASE WHEN se.place_code <> '' THEN 15 ELSE 0 END +
            CASE WHEN se.has_actual_times = TRUE THEN 20 ELSE 0 END +
            CASE WHEN se.has_planned_times = TRUE THEN 10 ELSE 0 END +
            CASE WHEN se.time_zone <> '' THEN 5 ELSE 0 END +
            CASE WHEN se.has_notes = TRUE THEN 5 ELSE 0 END
        ) AS data_completeness_score

    FROM schedule_enhancement se
)

SELECT *
FROM schedule_business_logic
-- ORDER BY actual_start_datetime DESC, employee_code