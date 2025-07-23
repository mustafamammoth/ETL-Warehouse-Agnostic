-- Cleaned and standardized daily working time data (ClickHouse compatible)
{{ config(
    materialized='table',
    schema=var('silver_schema', 'repsly_silver'),
    engine='MergeTree()',
    order_by='(daily_working_time_id, work_date_nn)',
    partition_by='toYYYYMM(work_date_nn)'
) }}

WITH working_time_cleaning AS (
    SELECT 
        daily_working_time_id,

        /* avoid Nullable(Nothing): explicitly cast NULL to Nullable(DateTime) */
        CAST(NULL AS Nullable(DateTime))                      AS timestamp_value,
        
        -- Representative info
        trimBoth(coalesce(representative_code, ''))           AS representative_code,
        trimBoth(coalesce(representative_name, ''))           AS representative_name,

        -- Main work date
        CASE 
            WHEN startsWith(date_raw, '/Date(') THEN
                toDate(
                    toDateTime(
                        toInt64(substring(date_raw, 7, length(date_raw) - 13)) / 1000
                    )
                )
            ELSE NULL
        END                                                   AS work_date,

        -- Start datetime
        CASE 
            WHEN startsWith(date_and_time_start_raw, '/Date(') THEN
                toDateTime(
                    toInt64(substring(date_and_time_start_raw, 7, length(date_and_time_start_raw) - 13)) / 1000
                )
            ELSE NULL
        END                                                   AS work_start_datetime,

        -- End datetime (handle ongoing marker)
        CASE 
            WHEN date_and_time_end_raw = '/Date(1752532203816)/' THEN NULL
            WHEN startsWith(date_and_time_end_raw, '/Date(') THEN
                toDateTime(
                    toInt64(substring(date_and_time_end_raw, 7, length(date_and_time_end_raw) - 13)) / 1000
                )
            ELSE NULL
        END                                                   AS work_end_datetime,

        -- Numeric fields
        CASE WHEN length_raw         NOT IN ('', 'NULL', 'null')        AND length_raw         IS NOT NULL THEN toInt32OrZero(length_raw)         ELSE 0   END AS work_length_minutes,
        CASE WHEN mileage_start_raw  NOT IN ('', 'NULL', 'null')        AND mileage_start_raw  IS NOT NULL THEN toFloat64OrZero(mileage_start_raw) ELSE 0.0 END AS mileage_start,
        CASE WHEN mileage_end_raw    NOT IN ('', 'NULL', 'null')        AND mileage_end_raw    IS NOT NULL THEN toFloat64OrZero(mileage_end_raw)   ELSE 0.0 END AS mileage_end,
        CASE WHEN mileage_total_raw  NOT IN ('', 'NULL', 'null')        AND mileage_total_raw  IS NOT NULL THEN toFloat64OrZero(mileage_total_raw) ELSE 0.0 END AS mileage_total,

        -- GPS
        CASE WHEN latitude_start_raw  NOT IN ('', 'NULL', 'null', '0')  AND latitude_start_raw  IS NOT NULL THEN toFloat64OrZero(latitude_start_raw)  ELSE NULL END AS latitude_start,
        CASE WHEN longitude_start_raw NOT IN ('', 'NULL', 'null', '0')  AND longitude_start_raw IS NOT NULL THEN toFloat64OrZero(longitude_start_raw) ELSE NULL END AS longitude_start,
        CASE WHEN latitude_end_raw    NOT IN ('', 'NULL', 'null', '0')  AND latitude_end_raw    IS NOT NULL THEN toFloat64OrZero(latitude_end_raw)    ELSE NULL END AS latitude_end,
        CASE WHEN longitude_end_raw   NOT IN ('', 'NULL', 'null', '0')  AND longitude_end_raw   IS NOT NULL THEN toFloat64OrZero(longitude_end_raw)   ELSE NULL END AS longitude_end,

        -- Visits
        CASE WHEN no_of_visits_raw   NOT IN ('', 'NULL', 'null')        AND no_of_visits_raw   IS NOT NULL THEN toInt32OrZero(no_of_visits_raw)   ELSE 0 END AS number_of_visits,

        -- Time breakdown
        CASE WHEN time_at_client_raw   NOT IN ('', 'NULL', 'null') AND time_at_client_raw   IS NOT NULL THEN toInt32OrZero(time_at_client_raw)   ELSE 0 END AS time_at_client_minutes,
        CASE WHEN time_in_travel_raw   NOT IN ('', 'NULL', 'null') AND time_in_travel_raw   IS NOT NULL THEN toInt32OrZero(time_in_travel_raw)   ELSE 0 END AS time_in_travel_minutes,
        CASE WHEN time_in_pause_raw    NOT IN ('', 'NULL', 'null') AND time_in_pause_raw    IS NOT NULL THEN toInt32OrZero(time_in_pause_raw)    ELSE 0 END AS time_in_pause_minutes,

        -- Notes / tag
        CASE 
            WHEN note IS NOT NULL AND note != '' 
            THEN replaceRegexpAll(trimBoth(note), '[\\r\\n]+', ' ')
            ELSE NULL
        END                                                   AS notes_cleaned,
        
        trimBoth(coalesce(tag, ''))                           AS tag,

        -- Metadata
        extracted_at,
        source_system,
        dbt_extracted_at,
        dbt_updated_at,
        has_date,
        has_representative,
        has_duration

    FROM {{ ref('daily_working_time_raw') }}
),

working_time_duration AS (
    SELECT
        *,
        CASE 
            WHEN work_start_datetime IS NOT NULL AND work_end_datetime IS NOT NULL
            THEN dateDiff('minute', work_start_datetime, work_end_datetime)
            ELSE work_length_minutes
        END AS actual_work_duration_minutes
    FROM working_time_cleaning
),

working_time_enhancement AS (
    SELECT
        wd.*,

        -- Date pieces
        CASE WHEN work_date IS NOT NULL THEN toDayOfWeek(work_date) ELSE NULL END AS work_day_of_week,
        CASE WHEN work_date IS NOT NULL THEN toYear(work_date)      ELSE NULL END AS work_year,
        CASE WHEN work_date IS NOT NULL THEN toMonth(work_date)     ELSE NULL END AS work_month,

        -- Time pieces
        CASE WHEN work_start_datetime IS NOT NULL THEN toHour(work_start_datetime) ELSE NULL END AS start_hour,
        CASE WHEN work_end_datetime   IS NOT NULL THEN toHour(work_end_datetime)   ELSE NULL END AS end_hour,

        -- Efficiency metrics
        CASE WHEN actual_work_duration_minutes > 0 
             THEN round((time_at_client_minutes * 100.0) / actual_work_duration_minutes, 2)
             ELSE 0 END AS client_time_percentage,

        CASE WHEN actual_work_duration_minutes > 0 
             THEN round((time_in_travel_minutes * 100.0) / actual_work_duration_minutes, 2)
             ELSE 0 END AS travel_time_percentage,

        -- GPS quality
        CASE 
            WHEN latitude_start  IS NOT NULL AND longitude_start  IS NOT NULL 
             AND latitude_end    IS NOT NULL AND longitude_end    IS NOT NULL THEN 'Complete GPS'
            WHEN latitude_start  IS NOT NULL AND longitude_start  IS NOT NULL THEN 'Partial GPS (Start Only)'
            WHEN latitude_end    IS NOT NULL AND longitude_end    IS NOT NULL THEN 'Partial GPS (End Only)'
            ELSE 'No GPS'
        END AS gps_data_quality,

        -- Completion
        CASE WHEN work_end_datetime IS NULL THEN 'Ongoing/Incomplete' ELSE 'Completed' END AS work_completion_status

    FROM working_time_duration wd
),

working_time_business_logic AS (
    SELECT
        we.*,

        -- non-null date for engine keys
        CAST(work_date AS Date) AS work_date_nn,

        -- Day name
        CASE work_day_of_week
            WHEN 1 THEN 'Monday'
            WHEN 2 THEN 'Tuesday'
            WHEN 3 THEN 'Wednesday'
            WHEN 4 THEN 'Thursday'
            WHEN 5 THEN 'Friday'
            WHEN 6 THEN 'Saturday'
            WHEN 7 THEN 'Sunday'
            ELSE 'Unknown'
        END AS day_of_week_name,

        -- Duration cat
        CASE 
            WHEN actual_work_duration_minutes = 0    THEN 'No Work Time'
            WHEN actual_work_duration_minutes < 60   THEN 'Very Short (<1 hour)'
            WHEN actual_work_duration_minutes < 240  THEN 'Short (1-4 hours)'
            WHEN actual_work_duration_minutes < 480  THEN 'Standard (4-8 hours)'
            WHEN actual_work_duration_minutes < 600  THEN 'Long (8-10 hours)'
            ELSE 'Very Long (10+ hours)'
        END AS work_duration_category,

        -- Start time cat
        CASE 
            WHEN start_hour IS NULL             THEN 'Unknown Start'
            WHEN start_hour BETWEEN 5  AND 8    THEN 'Early Start (5-8 AM)'
            WHEN start_hour BETWEEN 9  AND 11   THEN 'Morning Start (9-11 AM)'
            WHEN start_hour BETWEEN 12 AND 14   THEN 'Afternoon Start (12-2 PM)'
            WHEN start_hour BETWEEN 15 AND 17   THEN 'Late Start (3-5 PM)'
            ELSE 'Other Start Time'
        END AS start_time_category,

        -- Visit productivity
        CASE 
            WHEN number_of_visits = 0                 THEN 'No Visits'
            WHEN number_of_visits = 1                 THEN 'Single Visit'
            WHEN number_of_visits BETWEEN 2 AND 3     THEN 'Few Visits (2-3)'
            WHEN number_of_visits BETWEEN 4 AND 6     THEN 'Multiple Visits (4-6)'
            ELSE 'Many Visits (7+)'
        END AS visit_productivity_category,

        -- Efficiency rating
        CASE 
            WHEN client_time_percentage >= 70 THEN 'High Efficiency'
            WHEN client_time_percentage >= 50 THEN 'Good Efficiency'
            WHEN client_time_percentage >= 30 THEN 'Moderate Efficiency'
            WHEN client_time_percentage >  0  THEN 'Low Efficiency'
            ELSE 'No Client Time'
        END AS efficiency_rating,

        -- Mileage categories
        CASE 
            WHEN mileage_total = 0          THEN 'No Mileage'
            WHEN mileage_total < 50         THEN 'Low Mileage (<50)'
            WHEN mileage_total < 150        THEN 'Medium Mileage (50-150)'
            WHEN mileage_total < 300        THEN 'High Mileage (150-300)'
            ELSE 'Very High Mileage (300+)'
        END AS mileage_category,

        -- Weekday / weekend
        CASE 
            WHEN work_day_of_week IN (1,2,3,4,5) THEN 'Weekday'
            WHEN work_day_of_week IN (6,7)       THEN 'Weekend'
            ELSE 'Unknown'
        END AS work_day_type,

        -- Data completeness score (0-100)
        (
            if(representative_code        != '', 15, 0) +
            if(work_date                  IS NOT NULL, 15, 0) +
            if(work_start_datetime        IS NOT NULL, 10, 0) +
            if(work_end_datetime          IS NOT NULL, 10, 0) +
            if(number_of_visits           >  0,        15, 0) +
            if(time_at_client_minutes     >  0,        10, 0) +
            if(mileage_total              >  0,        10, 0) +
            if(gps_data_quality IN ('Complete GPS','Partial GPS (Start Only)','Partial GPS (End Only)'), 15, 0)
        ) AS data_completeness_score,

        -- Performance indicators
        CASE 
            WHEN number_of_visits > 0 AND actual_work_duration_minutes > 0
            THEN round(actual_work_duration_minutes / number_of_visits, 1)
            ELSE 0
        END AS minutes_per_visit,

        CASE 
            WHEN number_of_visits > 0 AND mileage_total > 0
            THEN round(mileage_total / number_of_visits, 1)
            ELSE 0
        END AS mileage_per_visit

    FROM working_time_enhancement we
)

SELECT *
FROM working_time_business_logic
ORDER BY work_date DESC, work_start_datetime DESC, daily_working_time_id
