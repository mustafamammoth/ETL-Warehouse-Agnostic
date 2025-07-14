-- Cleaned and standardized visits data
{{ config(materialized='table') }}

WITH visit_cleaning AS (
    SELECT 
        visit_id,
        CAST(timestamp_raw AS BIGINT) AS timestamp_value,
        client_code,

        -- Clean representative information
        TRIM(COALESCE(representative_code, ''))  AS representative_code,
        TRIM(COALESCE(representative_name, '')) AS representative_name,

        -- Clean client information
        TRIM(COALESCE(client_name, ''))   AS client_name,
        TRIM(COALESCE(street_address, '')) AS street_address,
        TRIM(COALESCE(zip_code, ''))      AS zip_code,
        TRIM(COALESCE(zip_ext, ''))       AS zip_ext,
        TRIM(COALESCE(city, ''))          AS city,
        TRIM(COALESCE(state, ''))         AS state,
        TRIM(COALESCE(country, ''))       AS country,
        TRIM(COALESCE(territory, ''))     AS territory,

        -- Boolean conversions
        CASE WHEN UPPER(TRIM(COALESCE(explicit_check_in_raw, 'FALSE'))) = 'TRUE' THEN TRUE ELSE FALSE END AS explicit_check_in,
        CASE WHEN UPPER(TRIM(COALESCE(visit_ended_raw, 'FALSE')))       = 'TRUE' THEN TRUE ELSE FALSE END AS visit_ended,

        -- GPS coordinates (start location)
        CASE WHEN latitude_start_raw IS NOT NULL AND latitude_start_raw NOT IN ('', 'NULL')
             THEN CAST(latitude_start_raw AS DECIMAL(10,6)) END AS latitude_start,
        CASE WHEN longitude_start_raw IS NOT NULL AND longitude_start_raw NOT IN ('', 'NULL')
             THEN CAST(longitude_start_raw AS DECIMAL(10,6)) END AS longitude_start,

        -- GPS coordinates (end location)
        CASE WHEN latitude_end_raw IS NOT NULL AND latitude_end_raw NOT IN ('', 'NULL')
             THEN CAST(latitude_end_raw AS DECIMAL(10,6)) END AS latitude_end,
        CASE WHEN longitude_end_raw IS NOT NULL AND longitude_end_raw NOT IN ('', 'NULL')
             THEN CAST(longitude_end_raw AS DECIMAL(10,6)) END AS longitude_end,

        -- GPS precision
        CASE WHEN precision_start_raw IS NOT NULL AND precision_start_raw NOT IN ('', 'NULL')
             THEN CAST(precision_start_raw AS INTEGER) END AS precision_start,
        CASE WHEN precision_end_raw IS NOT NULL AND precision_end_raw NOT IN ('', 'NULL')
             THEN CAST(precision_end_raw AS INTEGER) END AS precision_end,

        -- Visit status
        CASE WHEN visit_status_by_schedule_raw IS NOT NULL AND visit_status_by_schedule_raw NOT IN ('', 'NULL')
             THEN CAST(visit_status_by_schedule_raw AS INTEGER) END AS visit_status_by_schedule,

        -- Parse Microsoft JSON date formats
        CASE WHEN date_raw LIKE '/Date(%' THEN
                 TO_TIMESTAMP(CAST(SUBSTRING(date_raw, 7, LENGTH(date_raw) - 13) AS BIGINT) / 1000)
             END AS visit_date_planned,

        CASE WHEN date_and_time_start_raw LIKE '/Date(%' THEN
                 TO_TIMESTAMP(CAST(SUBSTRING(date_and_time_start_raw, 7, LENGTH(date_and_time_start_raw) - 13) AS BIGINT) / 1000)
             END AS visit_start_datetime,

        CASE WHEN date_and_time_end_raw LIKE '/Date(%' THEN
                 TO_TIMESTAMP(CAST(SUBSTRING(date_and_time_end_raw, 7, LENGTH(date_and_time_end_raw) - 13) AS BIGINT) / 1000)
             END AS visit_end_datetime,

        -- Metadata
        extracted_at,
        source_system,
        endpoint

    FROM {{ ref('visits_raw') }}
),

visit_enhancement AS (
    SELECT
        *,
        -- Extract dates from datetimes
        CASE WHEN visit_date_planned IS NOT NULL THEN DATE(visit_date_planned)  END AS visit_date,
        CASE WHEN visit_start_datetime IS NOT NULL THEN DATE(visit_start_datetime) END AS visit_actual_date,

        -- Visit duration calculation
        CASE WHEN visit_start_datetime IS NOT NULL AND visit_end_datetime IS NOT NULL
             THEN EXTRACT(EPOCH FROM (visit_end_datetime - visit_start_datetime)) / 60
        END AS visit_duration_minutes,

        -- Full address concatenation
        CASE WHEN street_address <> '' OR city <> '' OR state <> '' THEN
                 CONCAT_WS(', ',
                     NULLIF(street_address, ''),
                     NULLIF(city, ''),
                     NULLIF(state, ''),
                     NULLIF(CONCAT_WS('-', NULLIF(zip_code, ''), NULLIF(zip_ext, '')), ''),
                     NULLIF(country, '')
                 )
        END AS full_address,

        -- Territory parsing (split by '>')
        CASE WHEN territory LIKE '%>%' THEN TRIM(SPLIT_PART(territory, '>', 1))
             WHEN territory <> '' THEN territory
        END AS territory_level_1,

        CASE WHEN territory LIKE '%>%' THEN TRIM(SPLIT_PART(territory, '>', 2)) END AS territory_level_2,

        -- State standardization for US
        CASE
            WHEN country = 'United States' AND state IN ('CA', 'California') THEN 'California'
            WHEN country = 'United States' AND state = 'TX' THEN 'Texas'
            WHEN country = 'United States' AND state = 'NY' THEN 'New York'
            WHEN country = 'United States' AND state = 'FL' THEN 'Florida'
            WHEN country = 'United States' AND state = 'MA' THEN 'Massachusetts'
            ELSE state
        END AS state_standardized,

        -- GPS quality flags
        (latitude_start IS NOT NULL AND longitude_start IS NOT NULL) AS has_start_gps,
        (latitude_end   IS NOT NULL AND longitude_end   IS NOT NULL) AS has_end_gps,

        -- Movement calculation (distance between start and end)
        CASE WHEN latitude_start IS NOT NULL AND longitude_start IS NOT NULL
                  AND latitude_end IS NOT NULL   AND longitude_end   IS NOT NULL THEN
                 111320 * SQRT(
                     POWER(latitude_end - latitude_start, 2) +
                     POWER((longitude_end - longitude_start) * COS(RADIANS((latitude_start + latitude_end) / 2)), 2)
                 )
        END AS movement_distance_meters

    FROM visit_cleaning
),

visit_business_logic AS (
    SELECT
        ve.*,

        -- Calendar breakdown (now safe to reference aliases from previous CTE)
        EXTRACT(HOUR  FROM ve.visit_start_datetime) AS visit_start_hour,
        EXTRACT(DOW   FROM ve.visit_start_datetime) AS visit_day_of_week,
        EXTRACT(YEAR  FROM ve.visit_actual_date)    AS visit_year,
        EXTRACT(MONTH FROM ve.visit_actual_date)    AS visit_month,

        -- Time analysis labels
        CASE
            WHEN EXTRACT(HOUR FROM ve.visit_start_datetime) BETWEEN 6  AND 11 THEN 'Morning'
            WHEN EXTRACT(HOUR FROM ve.visit_start_datetime) BETWEEN 12 AND 17 THEN 'Afternoon'
            WHEN EXTRACT(HOUR FROM ve.visit_start_datetime) BETWEEN 18 AND 21 THEN 'Evening'
            ELSE 'Other'
        END AS visit_time_of_day,

        -- Day-of-week name
        CASE EXTRACT(DOW FROM ve.visit_start_datetime)
            WHEN 0 THEN 'Sunday'
            WHEN 1 THEN 'Monday'
            WHEN 2 THEN 'Tuesday'
            WHEN 3 THEN 'Wednesday'
            WHEN 4 THEN 'Thursday'
            WHEN 5 THEN 'Friday'
            WHEN 6 THEN 'Saturday'
        END AS day_of_week_name,

        -- Visit status interpretation
        CASE ve.visit_status_by_schedule
            WHEN 1 THEN 'On Schedule'
            WHEN 0 THEN 'Off Schedule'
            ELSE 'Unknown Status'
        END AS visit_schedule_status,

        -- Movement category
        CASE
            WHEN ve.movement_distance_meters IS NULL                   THEN 'No GPS Data'
            WHEN ve.movement_distance_meters < 10                      THEN 'Stationary (< 10m)'
            WHEN ve.movement_distance_meters < 50                      THEN 'Minimal Movement (10-50m)'
            WHEN ve.movement_distance_meters < 100                     THEN 'Some Movement (50-100m)'
            ELSE 'Significant Movement (100m+)'
        END AS movement_category,

        -- Visit quality
        CASE
            WHEN ve.visit_ended = TRUE AND ve.visit_duration_minutes >= 15 AND ve.has_start_gps = TRUE THEN 'High Quality'
            WHEN ve.visit_ended = TRUE AND ve.visit_duration_minutes >=  5                              THEN 'Good Quality'
            WHEN ve.visit_ended = TRUE                                                                   THEN 'Basic Quality'
            WHEN ve.visit_ended = FALSE                                                                  THEN 'Incomplete Visit'
            ELSE 'Poor Quality'
        END AS visit_quality,

        -- GPS data quality
        CASE
            WHEN ve.has_start_gps = TRUE AND ve.has_end_gps = TRUE THEN 'Complete GPS'
            WHEN ve.has_start_gps = TRUE OR  ve.has_end_gps = TRUE THEN 'Partial GPS'
            ELSE 'No GPS'
        END AS gps_data_quality,

        -- Planned vs actual analysis
        CASE
            WHEN ve.visit_date IS NOT NULL AND ve.visit_actual_date IS NOT NULL THEN
                CASE
                    WHEN ve.visit_date = ve.visit_actual_date THEN 'On Planned Date'
                    WHEN ve.visit_actual_date > ve.visit_date THEN 'Late Visit'
                    WHEN ve.visit_actual_date < ve.visit_date THEN 'Early Visit'
                    ELSE 'Date Mismatch'
                END
            ELSE 'No Date Comparison'
        END AS visit_timing_vs_plan,

        -- Data completeness score (0–100)
        (
            CASE WHEN ve.client_code          <> '' THEN 10 ELSE 0 END +
            CASE WHEN ve.representative_code  <> '' THEN 10 ELSE 0 END +
            CASE WHEN ve.visit_start_datetime IS NOT NULL THEN 15 ELSE 0 END +
            CASE WHEN ve.visit_end_datetime   IS NOT NULL THEN 15 ELSE 0 END +
            CASE WHEN ve.visit_ended          =  TRUE    THEN 10 ELSE 0 END +
            CASE WHEN ve.city                 <> '' THEN 10 ELSE 0 END +
            CASE WHEN ve.state                <> '' THEN 10 ELSE 0 END +
            CASE WHEN ve.has_start_gps        =  TRUE    THEN 10 ELSE 0 END +
            CASE WHEN ve.has_end_gps          =  TRUE    THEN 10 ELSE 0 END
        ) AS data_completeness_score

    FROM visit_enhancement ve
)

SELECT *
FROM visit_business_logic
-- ORDER BY visit_start_datetime DESC, visit_id
