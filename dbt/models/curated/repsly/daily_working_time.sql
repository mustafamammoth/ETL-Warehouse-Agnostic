{{ config(
    materialized='table',
    schema=var('silver_schema', 'repsly_silver'),
    engine='MergeTree()',
    order_by='(daily_working_time_id, processed_at)',
    partition_by='toYYYYMM(processed_at)',
    meta={
        'description': 'Cleaned & enhanced daily working time data from Repsly',
        'business_key': 'daily_working_time_id',
        'update_strategy': 'latest_wins'
    }
) }}

-- Silver layer: Cleaned, typed, and deduplicated data
WITH latest_records AS (
    SELECT *,
        row_number() OVER (
            PARTITION BY daily_working_time_id
            ORDER BY 
                dbt_loaded_at DESC,
                record_hash DESC
        ) AS row_num
    FROM {{ ref('daily_working_time_raw') }}
    WHERE daily_working_time_id != '' AND daily_working_time_id != 'NULL'
),

with_timestamps AS (
    SELECT *,
        -- Parse extracted_at (stored as string in bronze)
        parseDateTimeBestEffortOrNull(extracted_at) AS extraction_datetime,
        
        -- dbt_loaded_at is already DateTime from now() in bronze
        dbt_loaded_at AS bronze_loaded_datetime,
        
        -- Current processing time
        now() AS processed_at

    FROM latest_records
    WHERE row_num = 1
),

typed_data AS (
    SELECT
        -- Business identifiers
        daily_working_time_id,

        -- Use our reliable timestamps
        extraction_datetime AS data_extracted_at,
        bronze_loaded_datetime AS bronze_processed_at,
        processed_at,

        -- Representative information
        NULLIF(trimBoth(representative_code), '') AS representative_code,
        NULLIF(trimBoth(representative_name), '') AS representative_name,

        -- Parse Microsoft JSON date format for main work date  
        CASE 
            WHEN date_raw LIKE '/Date(%' THEN
                toDate(fromUnixTimestamp(
                    toInt64(toInt64OrNull(substring(date_raw, 7, position(date_raw, ')') - 7)) / 1000)
                ))
            ELSE NULL
        END AS work_date,

        -- Parse start datetime
        CASE 
            WHEN date_and_time_start_raw LIKE '/Date(%' THEN
                fromUnixTimestamp(
                    toInt64(toInt64OrNull(substring(date_and_time_start_raw, 7, position(date_and_time_start_raw, ')') - 7)) / 1000)
                )
            ELSE NULL
        END AS work_start_datetime,

        -- Parse end datetime (handle ongoing work marker)
        CASE 
            WHEN date_and_time_end_raw = '/Date(1752532203816)/' THEN NULL
            WHEN date_and_time_end_raw LIKE '/Date(%' THEN
                fromUnixTimestamp(
                    toInt64(toInt64OrNull(substring(date_and_time_end_raw, 7, position(date_and_time_end_raw, ')') - 7)) / 1000)
                )
            ELSE NULL
        END AS work_end_datetime,

        -- Duration and numeric fields with validation
        CASE 
            WHEN length_raw != '' AND length_raw != 'NULL' AND length_raw IS NOT NULL 
            THEN toInt32OrNull(length_raw)
            ELSE NULL
        END AS work_length_minutes,

        -- Mileage information with validation
        CASE 
            WHEN mileage_start_raw != '' AND mileage_start_raw != 'NULL' AND mileage_start_raw IS NOT NULL
            THEN toFloat64OrNull(mileage_start_raw)
            ELSE NULL
        END AS mileage_start,

        CASE 
            WHEN mileage_end_raw != '' AND mileage_end_raw != 'NULL' AND mileage_end_raw IS NOT NULL
            THEN toFloat64OrNull(mileage_end_raw)
            ELSE NULL
        END AS mileage_end,

        CASE 
            WHEN mileage_total_raw != '' AND mileage_total_raw != 'NULL' AND mileage_total_raw IS NOT NULL
            THEN toFloat64OrNull(mileage_total_raw)
            ELSE NULL
        END AS mileage_total,

        -- GPS coordinates with validation (start location)
        CASE 
            WHEN latitude_start_raw != '' AND latitude_start_raw != 'NULL' AND latitude_start_raw != '0' 
                 AND latitude_start_raw IS NOT NULL
                 AND toFloat64OrNull(latitude_start_raw) BETWEEN -90 AND 90
            THEN toFloat64OrNull(latitude_start_raw)
            ELSE NULL
        END AS latitude_start,

        CASE 
            WHEN longitude_start_raw != '' AND longitude_start_raw != 'NULL' AND longitude_start_raw != '0' 
                 AND longitude_start_raw IS NOT NULL
                 AND toFloat64OrNull(longitude_start_raw) BETWEEN -180 AND 180
            THEN toFloat64OrNull(longitude_start_raw)
            ELSE NULL
        END AS longitude_start,

        -- GPS coordinates with validation (end location)
        CASE 
            WHEN latitude_end_raw != '' AND latitude_end_raw != 'NULL' AND latitude_end_raw != '0' 
                 AND latitude_end_raw IS NOT NULL
                 AND toFloat64OrNull(latitude_end_raw) BETWEEN -90 AND 90
            THEN toFloat64OrNull(latitude_end_raw)
            ELSE NULL
        END AS latitude_end,

        CASE 
            WHEN longitude_end_raw != '' AND longitude_end_raw != 'NULL' AND longitude_end_raw != '0' 
                 AND longitude_end_raw IS NOT NULL
                 AND toFloat64OrNull(longitude_end_raw) BETWEEN -180 AND 180
            THEN toFloat64OrNull(longitude_end_raw)
            ELSE NULL
        END AS longitude_end,

        -- Visit metrics
        CASE 
            WHEN no_of_visits_raw != '' AND no_of_visits_raw != 'NULL' AND no_of_visits_raw IS NOT NULL
            THEN toInt32OrNull(no_of_visits_raw)
            ELSE NULL
        END AS number_of_visits,

        CASE 
            WHEN min_of_visits_raw != '' AND min_of_visits_raw != 'NULL' AND min_of_visits_raw IS NOT NULL
            THEN toInt32OrNull(min_of_visits_raw)
            ELSE NULL
        END AS min_of_visits,

        CASE 
            WHEN max_of_visits_raw != '' AND max_of_visits_raw != 'NULL' AND max_of_visits_raw IS NOT NULL
            THEN toInt32OrNull(max_of_visits_raw)
            ELSE NULL
        END AS max_of_visits,

        NULLIF(trimBoth(min_max_visits_time_raw), '') AS min_max_visits_time,

        -- Time breakdown
        CASE 
            WHEN time_at_client_raw != '' AND time_at_client_raw != 'NULL' AND time_at_client_raw IS NOT NULL
            THEN toInt32OrNull(time_at_client_raw)
            ELSE NULL
        END AS time_at_client_minutes,

        CASE 
            WHEN time_in_travel_raw != '' AND time_in_travel_raw != 'NULL' AND time_in_travel_raw IS NOT NULL
            THEN toInt32OrNull(time_in_travel_raw)
            ELSE NULL
        END AS time_in_travel_minutes,

        CASE 
            WHEN time_in_pause_raw != '' AND time_in_pause_raw != 'NULL' AND time_in_pause_raw IS NOT NULL
            THEN toInt32OrNull(time_in_pause_raw)
            ELSE NULL
        END AS time_in_pause_minutes,

        -- Notes and tags (clean)
        CASE 
            WHEN note IS NOT NULL AND note != '' AND note != 'NULL'
            THEN replaceRegexpAll(trimBoth(note), '[\\r\\n]+', ' ')
            ELSE NULL
        END AS notes_cleaned,

        NULLIF(trimBoth(tag), '') AS tag_clean,

        -- System metadata
        source_system,
        endpoint,
        record_hash

    FROM with_timestamps
),

enhanced AS (
    SELECT
        *,
        
        -- Calculate actual work duration
        CASE 
            WHEN work_start_datetime IS NOT NULL AND work_end_datetime IS NOT NULL
            THEN dateDiff('minute', work_start_datetime, work_end_datetime)
            ELSE work_length_minutes
        END AS actual_work_duration_minutes,

        -- Date components
        CASE WHEN work_date IS NOT NULL THEN toDayOfWeek(work_date) ELSE NULL END AS work_day_of_week,
        CASE WHEN work_date IS NOT NULL THEN toYear(work_date) ELSE NULL END AS work_year,
        CASE WHEN work_date IS NOT NULL THEN toMonth(work_date) ELSE NULL END AS work_month,

        -- Time components
        CASE WHEN work_start_datetime IS NOT NULL THEN toHour(work_start_datetime) ELSE NULL END AS start_hour,
        CASE WHEN work_end_datetime IS NOT NULL THEN toHour(work_end_datetime) ELSE NULL END AS end_hour,

        -- GPS data quality assessment
        CASE 
            WHEN latitude_start IS NOT NULL AND longitude_start IS NOT NULL 
                 AND latitude_end IS NOT NULL AND longitude_end IS NOT NULL 
            THEN 'Complete GPS'
            WHEN latitude_start IS NOT NULL AND longitude_start IS NOT NULL 
            THEN 'Start GPS Only'
            WHEN latitude_end IS NOT NULL AND longitude_end IS NOT NULL 
            THEN 'End GPS Only'
            ELSE 'No GPS'
        END AS gps_data_quality,

        -- Work completion status
        CASE 
            WHEN work_end_datetime IS NULL 
            THEN 'Ongoing/Incomplete' 
            ELSE 'Completed' 
        END AS work_completion_status,

        -- Data quality flags
        CASE WHEN work_date IS NOT NULL THEN 1 ELSE 0 END AS has_work_date,
        CASE WHEN representative_code IS NOT NULL THEN 1 ELSE 0 END AS has_representative,
        CASE WHEN work_length_minutes IS NOT NULL AND work_length_minutes > 0 THEN 1 ELSE 0 END AS has_duration,
        CASE WHEN number_of_visits IS NOT NULL AND number_of_visits > 0 THEN 1 ELSE 0 END AS has_visits,
        CASE WHEN mileage_total IS NOT NULL AND mileage_total > 0 THEN 1 ELSE 0 END AS has_mileage

    FROM typed_data
),

business_logic AS (
    SELECT
        *,
        
        -- Efficiency metrics
        CASE 
            WHEN actual_work_duration_minutes IS NOT NULL AND actual_work_duration_minutes > 0 
                 AND time_at_client_minutes IS NOT NULL
            THEN round((time_at_client_minutes * 100.0) / actual_work_duration_minutes, 2)
            ELSE NULL
        END AS client_time_percentage,

        CASE 
            WHEN actual_work_duration_minutes IS NOT NULL AND actual_work_duration_minutes > 0 
                 AND time_in_travel_minutes IS NOT NULL
            THEN round((time_in_travel_minutes * 100.0) / actual_work_duration_minutes, 2)
            ELSE NULL
        END AS travel_time_percentage,

        -- Day of week name
        CASE work_day_of_week
            WHEN 1 THEN 'Monday'
            WHEN 2 THEN 'Tuesday'
            WHEN 3 THEN 'Wednesday'
            WHEN 4 THEN 'Thursday'
            WHEN 5 THEN 'Friday'
            WHEN 6 THEN 'Saturday'
            WHEN 7 THEN 'Sunday'
            ELSE NULL
        END AS day_of_week_name,

        -- Work day type
        CASE 
            WHEN work_day_of_week IN (1,2,3,4,5) THEN 'Weekday'
            WHEN work_day_of_week IN (6,7) THEN 'Weekend'
            ELSE NULL
        END AS work_day_type,

        -- Duration categories
        CASE 
            WHEN actual_work_duration_minutes IS NULL THEN 'No Duration Data'
            WHEN actual_work_duration_minutes = 0 THEN 'No Work Time'
            WHEN actual_work_duration_minutes < 60 THEN 'Very Short (<1 hour)'
            WHEN actual_work_duration_minutes < 240 THEN 'Short (1-4 hours)'
            WHEN actual_work_duration_minutes < 480 THEN 'Standard (4-8 hours)'
            WHEN actual_work_duration_minutes < 600 THEN 'Long (8-10 hours)'
            ELSE 'Very Long (10+ hours)'
        END AS work_duration_category,

        -- Start time categories
        CASE 
            WHEN start_hour IS NULL THEN 'Unknown Start'
            WHEN start_hour BETWEEN 5 AND 8 THEN 'Early Start (5-8 AM)'
            WHEN start_hour BETWEEN 9 AND 11 THEN 'Morning Start (9-11 AM)'
            WHEN start_hour BETWEEN 12 AND 14 THEN 'Afternoon Start (12-2 PM)'
            WHEN start_hour BETWEEN 15 AND 17 THEN 'Late Start (3-5 PM)'
            ELSE 'Other Start Time'
        END AS start_time_category,

        -- Visit productivity categories
        CASE 
            WHEN number_of_visits IS NULL THEN 'No Visit Data'
            WHEN number_of_visits = 0 THEN 'No Visits'
            WHEN number_of_visits = 1 THEN 'Single Visit'
            WHEN number_of_visits BETWEEN 2 AND 3 THEN 'Few Visits (2-3)'
            WHEN number_of_visits BETWEEN 4 AND 6 THEN 'Multiple Visits (4-6)'
            ELSE 'Many Visits (7+)'
        END AS visit_productivity_category,

        -- Efficiency rating
        CASE 
            WHEN client_time_percentage IS NULL THEN 'No Data'
            WHEN client_time_percentage >= 70 THEN 'High Efficiency'
            WHEN client_time_percentage >= 50 THEN 'Good Efficiency'
            WHEN client_time_percentage >= 30 THEN 'Moderate Efficiency'
            WHEN client_time_percentage > 0 THEN 'Low Efficiency'
            ELSE 'No Client Time'
        END AS efficiency_rating,

        -- Mileage categories
        CASE 
            WHEN mileage_total IS NULL THEN 'No Mileage Data'
            WHEN mileage_total = 0 THEN 'No Mileage'
            WHEN mileage_total < 50 THEN 'Low Mileage (<50)'
            WHEN mileage_total < 150 THEN 'Medium Mileage (50-150)'
            WHEN mileage_total < 300 THEN 'High Mileage (150-300)'
            ELSE 'Very High Mileage (300+)'
        END AS mileage_category,

        -- Performance indicators
        CASE 
            WHEN number_of_visits IS NOT NULL AND number_of_visits > 0 
                 AND actual_work_duration_minutes IS NOT NULL AND actual_work_duration_minutes > 0
            THEN round(actual_work_duration_minutes / number_of_visits, 1)
            ELSE NULL
        END AS minutes_per_visit,

        CASE 
            WHEN number_of_visits IS NOT NULL AND number_of_visits > 0 
                 AND mileage_total IS NOT NULL AND mileage_total > 0
            THEN round(mileage_total / number_of_visits, 1)
            ELSE NULL
        END AS mileage_per_visit,

        -- Data completeness score (0-100)
        (
            CASE WHEN representative_code IS NOT NULL THEN 15 ELSE 0 END +
            CASE WHEN work_date IS NOT NULL THEN 15 ELSE 0 END +
            CASE WHEN work_start_datetime IS NOT NULL THEN 10 ELSE 0 END +
            CASE WHEN work_end_datetime IS NOT NULL THEN 10 ELSE 0 END +
            CASE WHEN number_of_visits IS NOT NULL AND number_of_visits > 0 THEN 15 ELSE 0 END +
            CASE WHEN time_at_client_minutes IS NOT NULL AND time_at_client_minutes > 0 THEN 10 ELSE 0 END +
            CASE WHEN mileage_total IS NOT NULL AND mileage_total > 0 THEN 10 ELSE 0 END +
            CASE WHEN gps_data_quality IN ('Complete GPS', 'Start GPS Only', 'End GPS Only') THEN 15 ELSE 0 END
        ) AS data_completeness_score

    FROM enhanced
)

SELECT *
FROM business_logic