-- Cleaned and standardized daily working time data
{{ config(materialized='table') }}

WITH working_time_cleaning AS (
    SELECT 
        daily_working_time_id,
        -- Clean representative information
        TRIM(COALESCE(representative_code, ''))                       AS representative_code,
        TRIM(COALESCE(representative_name, ''))                       AS representative_name,

        -- Parse Microsoft JSON date format for main date
        CASE 
            WHEN date_raw LIKE '/Date(%' 
            THEN DATE(
                    TO_TIMESTAMP(
                        CAST(SUBSTRING(date_raw, 7, LENGTH(date_raw) - 13) AS BIGINT) / 1000
                    )
                 )
            ELSE NULL
        END                                                           AS work_date,

        -- Parse start datetime
        CASE 
            WHEN date_and_time_start_raw LIKE '/Date(%'
            THEN TO_TIMESTAMP(
                    CAST(SUBSTRING(date_and_time_start_raw, 7, LENGTH(date_and_time_start_raw) - 13) AS BIGINT) / 1000
                 )
            ELSE NULL
        END                                                           AS work_start_datetime,

        -- Parse end datetime (handle special case for ongoing work)
        CASE 
            WHEN date_and_time_end_raw LIKE '/Date(1752532203816)/'   THEN NULL        -- “ongoing” marker
            WHEN date_and_time_end_raw LIKE '/Date(%'
            THEN TO_TIMESTAMP(
                    CAST(SUBSTRING(date_and_time_end_raw, 7, LENGTH(date_and_time_end_raw) - 13) AS BIGINT) / 1000
                 )
            ELSE NULL
        END                                                           AS work_end_datetime,

        -- Convert numeric fields
        CASE WHEN length_raw       NOT IN ('', 'NULL') THEN CAST(length_raw       AS INTEGER)       ELSE 0    END AS work_length_minutes,
        CASE WHEN mileage_start_raw NOT IN ('', 'NULL') THEN CAST(mileage_start_raw AS DECIMAL(10,2)) ELSE 0.0 END AS mileage_start,
        CASE WHEN mileage_end_raw   NOT IN ('', 'NULL') THEN CAST(mileage_end_raw   AS DECIMAL(10,2)) ELSE 0.0 END AS mileage_end,
        CASE WHEN mileage_total_raw NOT IN ('', 'NULL') THEN CAST(mileage_total_raw AS DECIMAL(10,2)) ELSE 0.0 END AS mileage_total,

        -- GPS coordinates
        CASE WHEN latitude_start_raw  NOT IN ('', 'NULL', '0') THEN CAST(latitude_start_raw  AS DECIMAL(10,6)) ELSE NULL END AS latitude_start,
        CASE WHEN longitude_start_raw NOT IN ('', 'NULL', '0') THEN CAST(longitude_start_raw AS DECIMAL(10,6)) ELSE NULL END AS longitude_start,
        CASE WHEN latitude_end_raw    NOT IN ('', 'NULL', '0') THEN CAST(latitude_end_raw    AS DECIMAL(10,6)) ELSE NULL END AS latitude_end,
        CASE WHEN longitude_end_raw   NOT IN ('', 'NULL', '0') THEN CAST(longitude_end_raw   AS DECIMAL(10,6)) ELSE NULL END AS longitude_end,

        -- Visit metrics
        CASE WHEN no_of_visits_raw  NOT IN ('', 'NULL') THEN CAST(no_of_visits_raw AS INTEGER) ELSE 0 END AS number_of_visits,

        -- Time breakdown
        CASE WHEN time_at_client_raw NOT IN ('', 'NULL') THEN CAST(time_at_client_raw AS INTEGER) ELSE 0 END AS time_at_client_minutes,
        CASE WHEN time_in_travel_raw NOT IN ('', 'NULL') THEN CAST(time_in_travel_raw AS INTEGER) ELSE 0 END AS time_in_travel_minutes,
        CASE WHEN time_in_pause_raw  NOT IN ('', 'NULL') THEN CAST(time_in_pause_raw  AS INTEGER) ELSE 0 END AS time_in_pause_minutes,

        -- Clean notes and tags
        CASE 
            WHEN note IS NOT NULL AND note != '' 
            THEN REPLACE(REPLACE(TRIM(note), CHR(10), ' '), CHR(13), ' ')
            ELSE NULL
        END                                                           AS notes_cleaned,
        TRIM(COALESCE(tag, ''))                                       AS tag,

        -- Metadata
        extracted_at,
        source_system,
        endpoint,
        testing_mode

    FROM {{ ref('daily_working_time_raw') }}
),

-- 1️⃣  First, calculate actual_work_duration_minutes so later CTEs can reference it safely
working_time_duration AS (
    SELECT
        *,
        CASE 
            WHEN work_start_datetime IS NOT NULL 
             AND work_end_datetime   IS NOT NULL
            THEN EXTRACT(EPOCH FROM (work_end_datetime - work_start_datetime))/60
            ELSE work_length_minutes
        END AS actual_work_duration_minutes
    FROM working_time_cleaning
),

-- 2️⃣  Add time-based analyses and efficiency metrics
working_time_enhancement AS (
    SELECT
        wd.*,

        -- Day / month / year parts
        CASE WHEN work_date IS NOT NULL THEN EXTRACT(DOW   FROM work_date) END AS work_day_of_week,
        CASE WHEN work_date IS NOT NULL THEN EXTRACT(YEAR  FROM work_date) END AS work_year,
        CASE WHEN work_date IS NOT NULL THEN EXTRACT(MONTH FROM work_date) END AS work_month,

        -- Start / end hour
        CASE WHEN work_start_datetime IS NOT NULL THEN EXTRACT(HOUR FROM work_start_datetime) END AS start_hour,
        CASE WHEN work_end_datetime   IS NOT NULL THEN EXTRACT(HOUR FROM work_end_datetime)   END AS end_hour,

        -- Efficiency metrics (now safe because alias exists)
        CASE 
            WHEN actual_work_duration_minutes > 0 
            THEN ROUND((time_at_client_minutes ::DECIMAL / actual_work_duration_minutes) * 100, 2)
            ELSE 0
        END AS client_time_percentage,

        CASE 
            WHEN actual_work_duration_minutes > 0 
            THEN ROUND((time_in_travel_minutes::DECIMAL / actual_work_duration_minutes) * 100, 2)
            ELSE 0
        END AS travel_time_percentage,

        -- GPS data quality
        CASE 
            WHEN latitude_start IS NOT NULL AND longitude_start IS NOT NULL 
              AND latitude_end  IS NOT NULL AND longitude_end  IS NOT NULL THEN 'Complete GPS'
            WHEN latitude_start IS NOT NULL AND longitude_start IS NOT NULL THEN 'Partial GPS (Start Only)'
            WHEN latitude_end   IS NOT NULL AND longitude_end   IS NOT NULL THEN 'Partial GPS (End Only)'
            ELSE 'No GPS'
        END AS gps_data_quality,

        -- Work completion status
        CASE 
            WHEN work_end_datetime IS NULL THEN 'Ongoing/Incomplete'
            ELSE 'Completed'
        END AS work_completion_status
    FROM working_time_duration wd
),

-- 3️⃣  Business-level categorizations & KPIs
working_time_business_logic AS (
    SELECT
        we.*,

        -- Day name
        CASE work_day_of_week
            WHEN 0 THEN 'Sunday'    WHEN 1 THEN 'Monday'  WHEN 2 THEN 'Tuesday'
            WHEN 3 THEN 'Wednesday' WHEN 4 THEN 'Thursday'WHEN 5 THEN 'Friday'
            WHEN 6 THEN 'Saturday'  ELSE 'Unknown'
        END AS day_of_week_name,

        -- Work duration buckets
        CASE 
            WHEN actual_work_duration_minutes = 0   THEN 'No Work Time'
            WHEN actual_work_duration_minutes < 60  THEN 'Very Short (<1 hour)'
            WHEN actual_work_duration_minutes < 240 THEN 'Short (1-4 hours)'
            WHEN actual_work_duration_minutes < 480 THEN 'Standard (4-8 hours)'
            WHEN actual_work_duration_minutes < 600 THEN 'Long (8-10 hours)'
            ELSE 'Very Long (10+ hours)'
        END AS work_duration_category,

        -- Start time buckets
        CASE 
            WHEN start_hour IS NULL          THEN 'Unknown Start'
            WHEN start_hour BETWEEN 5  AND 8 THEN 'Early Start (5-8 AM)'
            WHEN start_hour BETWEEN 9  AND 11 THEN 'Morning Start (9-11 AM)'
            WHEN start_hour BETWEEN 12 AND 14 THEN 'Afternoon Start (12-2 PM)'
            WHEN start_hour BETWEEN 15 AND 17 THEN 'Late Start (3-5 PM)'
            ELSE 'Other Start Time'
        END AS start_time_category,

        -- Visit productivity
        CASE 
            WHEN number_of_visits = 0                    THEN 'No Visits'
            WHEN number_of_visits = 1                    THEN 'Single Visit'
            WHEN number_of_visits BETWEEN 2 AND 3        THEN 'Few Visits (2-3)'
            WHEN number_of_visits BETWEEN 4 AND 6        THEN 'Multiple Visits (4-6)'
            ELSE 'Many Visits (7+)'
        END AS visit_productivity_category,

        -- Efficiency rating
        CASE 
            WHEN client_time_percentage >= 70 THEN 'High Efficiency'
            WHEN client_time_percentage >= 50 THEN 'Good Efficiency'
            WHEN client_time_percentage >= 30 THEN 'Moderate Efficiency'
            WHEN client_time_percentage >  0 THEN 'Low Efficiency'
            ELSE 'No Client Time'
        END AS efficiency_rating,

        -- Mileage buckets
        CASE 
            WHEN mileage_total = 0            THEN 'No Mileage'
            WHEN mileage_total < 50           THEN 'Low Mileage (<50)'
            WHEN mileage_total < 150          THEN 'Medium Mileage (50-150)'
            WHEN mileage_total < 300          THEN 'High Mileage (150-300)'
            ELSE 'Very High Mileage (300+)'
        END AS mileage_category,

        -- Weekday / weekend
        CASE 
            WHEN work_day_of_week IN (1,2,3,4,5) THEN 'Weekday'
            WHEN work_day_of_week IN (0,6)       THEN 'Weekend'
            ELSE 'Unknown'
        END AS work_day_type,

        -- Data-completeness score (0–100)
        (
            CASE WHEN representative_code           != '' THEN 15 ELSE 0 END +
            CASE WHEN work_date                       IS NOT NULL THEN 15 ELSE 0 END +
            CASE WHEN work_start_datetime             IS NOT NULL THEN 10 ELSE 0 END +
            CASE WHEN work_end_datetime               IS NOT NULL THEN 10 ELSE 0 END +
            CASE WHEN number_of_visits               >  0        THEN 15 ELSE 0 END +
            CASE WHEN time_at_client_minutes         >  0        THEN 10 ELSE 0 END +
            CASE WHEN mileage_total                  >  0        THEN 10 ELSE 0 END +
            CASE WHEN gps_data_quality IN ('Complete GPS',
                                           'Partial GPS (Start Only)',
                                           'Partial GPS (End Only)')  THEN 15 ELSE 0 END
        ) AS data_completeness_score,

        -- Performance indicators
        CASE 
            WHEN number_of_visits > 0 AND actual_work_duration_minutes > 0
            THEN ROUND(actual_work_duration_minutes::DECIMAL / number_of_visits, 1)
            ELSE 0
        END AS minutes_per_visit,

        CASE 
            WHEN number_of_visits > 0 AND mileage_total > 0
            THEN ROUND(mileage_total::DECIMAL / number_of_visits, 1)
            ELSE 0
        END AS mileage_per_visit
    FROM working_time_enhancement we
)

SELECT *
FROM working_time_business_logic
ORDER BY work_date DESC,
         work_start_datetime DESC,
         daily_working_time_id
