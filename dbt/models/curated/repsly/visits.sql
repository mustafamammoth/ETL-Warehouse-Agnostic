{{ config(
    materialized='table',
    schema=var('silver_schema', 'repsly_silver'),
    engine='MergeTree()',
    order_by='(visit_id, processed_at)',
    partition_by='toYYYYMM(processed_at)',
    meta={
        'description': 'Cleaned & enhanced visits data from Repsly',
        'business_key': 'visit_id',
        'update_strategy': 'latest_wins'
    }
) }}

-- Silver layer: Cleaned, typed, and deduplicated visits data
WITH latest_records AS (
    SELECT *,
        row_number() OVER (
            PARTITION BY visit_id
            ORDER BY 
                dbt_loaded_at DESC,
                record_hash DESC
        ) AS row_num
    FROM {{ ref('visits_raw') }}
    WHERE visit_id != '' AND visit_id != 'NULL'
),

with_timestamps AS (
    SELECT *,
        -- Parse extracted_at (stored as string in bronze)
        parseDateTimeBestEffortOrNull(extracted_at) AS extraction_datetime,
        
        -- dbt_loaded_at is already DateTime from now() in bronze
        dbt_loaded_at AS bronze_loaded_datetime,
        
        -- Keep Repsly timestamps as reference (don't parse proprietary format)
        timestamp_raw as repsly_timestamp_raw,
        date_raw as repsly_date_raw,
        
        -- Current processing time
        now() AS processed_at

    FROM latest_records
    WHERE row_num = 1
),

typed_data AS (
    SELECT
        -- Business identifiers
        visit_id,
        NULLIF(trimBoth(client_code), '') AS client_code,
        NULLIF(trimBoth(client_name), '') AS client_name,
        NULLIF(trimBoth(representative_code), '') AS representative_code,
        NULLIF(trimBoth(representative_name), '') AS representative_name,

        -- Use our reliable timestamps instead of parsing Repsly's proprietary format
        extraction_datetime AS visit_data_extracted_at,
        bronze_loaded_datetime AS bronze_processed_at,
        processed_at,
        repsly_timestamp_raw,
        repsly_date_raw,

        -- Boolean fields
        multiIf(
            lower(explicit_check_in_raw) IN ('true','1','t','yes'), 1,
            lower(explicit_check_in_raw) IN ('false','0','f','no'), 0,
            NULL
        ) AS explicit_check_in,

        multiIf(
            lower(visit_ended_raw) IN ('true','1','t','yes'), 1,
            lower(visit_ended_raw) IN ('false','0','f','no'), 0,
            NULL
        ) AS visit_ended,

        -- Numeric fields with validation
        CASE 
            WHEN visit_status_by_schedule_raw != '' AND visit_status_by_schedule_raw != 'NULL'
                 AND match(visit_status_by_schedule_raw, '^-?\\d+$')
            THEN toInt32OrNull(visit_status_by_schedule_raw)
            ELSE NULL
        END AS visit_status_by_schedule,

        -- GPS coordinates with validation
        CASE 
            WHEN latitude_start_raw != '' AND latitude_start_raw != 'NULL' 
                 AND match(latitude_start_raw, '^-?\\d*\\.?\\d+$')
                 AND toFloat64OrNull(latitude_start_raw) BETWEEN -90 AND 90
            THEN toFloat64OrNull(latitude_start_raw)
            ELSE NULL
        END AS latitude_start,

        CASE 
            WHEN longitude_start_raw != '' AND longitude_start_raw != 'NULL' 
                 AND match(longitude_start_raw, '^-?\\d*\\.?\\d+$')
                 AND toFloat64OrNull(longitude_start_raw) BETWEEN -180 AND 180
            THEN toFloat64OrNull(longitude_start_raw)
            ELSE NULL
        END AS longitude_start,

        CASE 
            WHEN latitude_end_raw != '' AND latitude_end_raw != 'NULL' 
                 AND match(latitude_end_raw, '^-?\\d*\\.?\\d+$')
                 AND toFloat64OrNull(latitude_end_raw) BETWEEN -90 AND 90
            THEN toFloat64OrNull(latitude_end_raw)
            ELSE NULL
        END AS latitude_end,

        CASE 
            WHEN longitude_end_raw != '' AND longitude_end_raw != 'NULL' 
                 AND match(longitude_end_raw, '^-?\\d*\\.?\\d+$')
                 AND toFloat64OrNull(longitude_end_raw) BETWEEN -180 AND 180
            THEN toFloat64OrNull(longitude_end_raw)
            ELSE NULL
        END AS longitude_end,

        -- Precision fields
        CASE 
            WHEN precision_start_raw != '' AND precision_start_raw != 'NULL'
                 AND match(precision_start_raw, '^\\d+$')
            THEN toInt32OrNull(precision_start_raw)
            ELSE NULL
        END AS precision_start,

        CASE 
            WHEN precision_end_raw != '' AND precision_end_raw != 'NULL'
                 AND match(precision_end_raw, '^\\d+$')
            THEN toInt32OrNull(precision_end_raw)
            ELSE NULL
        END AS precision_end,

        -- Address fields
        NULLIF(trimBoth(street_address), '') AS street_address,
        NULLIF(trimBoth(zip_code), '') AS zip_code,
        NULLIF(trimBoth(zip_ext), '') AS zip_ext,
        NULLIF(trimBoth(city), '') AS city,
        NULLIF(trimBoth(state), '') AS state,
        NULLIF(trimBoth(country), '') AS country,
        NULLIF(trimBoth(territory), '') AS territory,

        -- Parse Microsoft JSON dates with fallback (but prefer our timestamps)
        CASE 
            WHEN date_and_time_start_raw LIKE '/Date(%'
            THEN 
                toDateTime(
                    toInt64(
                        substring(
                            date_and_time_start_raw, 
                            position(date_and_time_start_raw, '(') + 1,
                            position(date_and_time_start_raw, '+') - position(date_and_time_start_raw, '(') - 1
                        )
                    ) / 1000
                )
            WHEN date_and_time_start_raw != '' AND date_and_time_start_raw != 'NULL'
            THEN parseDateTimeBestEffortOrNull(date_and_time_start_raw)
            ELSE NULL
        END AS visit_start_datetime,

        CASE 
            WHEN date_and_time_end_raw LIKE '/Date(%'
            THEN 
                toDateTime(
                    toInt64(
                        substring(
                            date_and_time_end_raw, 
                            position(date_and_time_end_raw, '(') + 1,
                            position(date_and_time_end_raw, '+') - position(date_and_time_end_raw, '(') - 1
                        )
                    ) / 1000
                )
            WHEN date_and_time_end_raw != '' AND date_and_time_end_raw != 'NULL'
            THEN parseDateTimeBestEffortOrNull(date_and_time_end_raw)
            ELSE NULL
        END AS visit_end_datetime,

        -- System metadata
        source_system,
        endpoint,
        record_hash

    FROM with_timestamps
),

enhanced AS (
    SELECT
        *,
        
        -- Derived date fields
        toDate(visit_start_datetime) AS visit_date,
        COALESCE(toDate(visit_start_datetime), toDate(processed_at)) AS visit_date_nn, -- Non-null for partitioning
        
        -- Duration calculation
        CASE 
            WHEN visit_start_datetime IS NOT NULL AND visit_end_datetime IS NOT NULL
            THEN dateDiff('minute', visit_start_datetime, visit_end_datetime)
            ELSE NULL
        END AS visit_duration_minutes,

        -- Time analysis
        CASE 
            WHEN visit_start_datetime IS NOT NULL
            THEN toHour(visit_start_datetime)
            ELSE NULL
        END AS visit_start_hour,

        CASE 
            WHEN visit_start_datetime IS NOT NULL
            THEN toDayOfWeek(visit_start_datetime)
            ELSE NULL
        END AS visit_day_of_week,

        -- Time of day classification
        CASE
            WHEN toHour(visit_start_datetime) BETWEEN 6 AND 11  THEN 'Morning'
            WHEN toHour(visit_start_datetime) BETWEEN 12 AND 17 THEN 'Afternoon'
            WHEN toHour(visit_start_datetime) BETWEEN 18 AND 21 THEN 'Evening'
            WHEN visit_start_datetime IS NOT NULL              THEN 'Other'
            ELSE 'Unknown'
        END AS visit_time_of_day,

        -- Derived address field
        CASE 
            WHEN street_address IS NOT NULL OR city IS NOT NULL OR state IS NOT NULL OR country IS NOT NULL THEN
                arrayStringConcat(
                    arrayFilter(x -> x != '', [
                        COALESCE(street_address, ''),
                        COALESCE(city, ''),
                        CASE 
                            WHEN state IS NOT NULL AND zip_code IS NOT NULL 
                            THEN concat(state, ' ', zip_code, COALESCE(concat('-', zip_ext), ''))
                            WHEN state IS NOT NULL 
                            THEN state
                            WHEN zip_code IS NOT NULL 
                            THEN concat(zip_code, COALESCE(concat('-', zip_ext), ''))
                            ELSE ''
                        END,
                        COALESCE(country, '')
                    ]),
                    ', '
                )
            ELSE NULL
        END AS full_address,

        -- Territory hierarchy
        CASE 
            WHEN territory IS NOT NULL AND position(territory, '>') > 0 
            THEN trimBoth(substring(territory, 1, position(territory, '>') - 1))
            ELSE territory
        END AS territory_level_1,

        CASE 
            WHEN territory IS NOT NULL AND position(territory, '>') > 0 
            THEN trimBoth(substring(territory, position(territory, '>') + 1))
            ELSE NULL
        END AS territory_level_2,

        -- GPS data quality flags
        CASE WHEN latitude_start IS NOT NULL AND longitude_start IS NOT NULL THEN 1 ELSE 0 END AS has_start_gps,
        CASE WHEN latitude_end IS NOT NULL AND longitude_end IS NOT NULL THEN 1 ELSE 0 END AS has_end_gps,

        -- Movement calculation (approximate distance in meters)
        CASE
            WHEN latitude_start IS NOT NULL AND longitude_start IS NOT NULL
             AND latitude_end IS NOT NULL AND longitude_end IS NOT NULL
            THEN 111320 * sqrt(
                    pow(latitude_end - latitude_start, 2) +
                    pow((longitude_end - longitude_start) * cos(radians((latitude_start + latitude_end)/2.0)), 2)
                 )
            ELSE NULL
        END AS movement_distance_meters

    FROM typed_data
),

business_logic AS (
    SELECT
        *,

        -- Visit status classification
        CASE visit_status_by_schedule
            WHEN 1 THEN 'On Schedule'
            WHEN 0 THEN 'Off Schedule'
            ELSE 'Unknown Status'
        END AS visit_schedule_status,

        -- Movement classification
        CASE
            WHEN movement_distance_meters IS NULL          THEN 'No GPS Data'
            WHEN movement_distance_meters < 10             THEN 'Stationary (< 10m)'
            WHEN movement_distance_meters < 50             THEN 'Minimal Movement (10-50m)'
            WHEN movement_distance_meters < 100            THEN 'Some Movement (50-100m)'
            ELSE 'Significant Movement (100m+)'
        END AS movement_category,

        -- Visit quality assessment
        CASE
            WHEN visit_ended = 1 AND visit_duration_minutes >= 15 AND has_start_gps = 1 THEN 'High Quality'
            WHEN visit_ended = 1 AND visit_duration_minutes >= 5                           THEN 'Good Quality'
            WHEN visit_ended = 1                                                           THEN 'Basic Quality'
            WHEN visit_ended = 0                                                           THEN 'Incomplete Visit'
            ELSE 'Poor Quality'
        END AS visit_quality,

        -- GPS data quality
        CASE
            WHEN has_start_gps = 1 AND has_end_gps = 1 THEN 'Complete GPS'
            WHEN has_start_gps = 1 OR has_end_gps = 1 THEN 'Partial GPS'
            ELSE 'No GPS'
        END AS gps_data_quality,

        -- Day of week name
        CASE visit_day_of_week
            WHEN 1 THEN 'Monday'
            WHEN 2 THEN 'Tuesday'
            WHEN 3 THEN 'Wednesday'
            WHEN 4 THEN 'Thursday'
            WHEN 5 THEN 'Friday'
            WHEN 6 THEN 'Saturday'
            WHEN 7 THEN 'Sunday'
            ELSE 'Unknown'
        END AS day_of_week_name

    FROM enhanced
),

scored AS (
    SELECT
        *,
        -- Data completeness score (0-100)
        (
            CASE WHEN visit_id IS NOT NULL THEN 10 ELSE 0 END +
            CASE WHEN client_code IS NOT NULL THEN 15 ELSE 0 END +
            CASE WHEN representative_code IS NOT NULL THEN 10 ELSE 0 END +
            CASE WHEN visit_start_datetime IS NOT NULL THEN 15 ELSE 0 END +
            CASE WHEN visit_end_datetime IS NOT NULL THEN 10 ELSE 0 END +
            CASE WHEN visit_ended = 1 THEN 10 ELSE 0 END +
            CASE WHEN city IS NOT NULL THEN 5 ELSE 0 END +
            CASE WHEN state IS NOT NULL THEN 5 ELSE 0 END +
            CASE WHEN has_start_gps = 1 THEN 10 ELSE 0 END +
            CASE WHEN has_end_gps = 1 THEN 10 ELSE 0 END
        ) AS data_completeness_score

    FROM business_logic
)

SELECT *
FROM scored
ORDER BY visit_id, processed_at