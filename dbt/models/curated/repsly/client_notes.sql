{{ config(
    materialized='table',
    schema=var('silver_schema', 'repsly_silver'),
    engine='MergeTree()',
    order_by='(client_note_id, note_date_nn)',
    partition_by='toYYYYMM(note_date_nn)',
    meta={'description': 'Cleaned and enhanced client notes data with robust date parsing'}
) }}

WITH date_parsing AS (
    SELECT 
        *,
        CASE 
            WHEN startsWith(date_and_time_raw, '/Date(') AND endsWith(date_and_time_raw, ')/') THEN
                CASE
                    WHEN length(extractAll(date_and_time_raw, '/Date\\((\\d+)')[1]) > 0
                    THEN toDateTime(toInt64(extractAll(date_and_time_raw, '/Date\\((\\d+)')[1]) / 1000)
                    ELSE NULL
                END
            WHEN endsWith(date_and_time_raw, 'Z') AND position(date_and_time_raw, 'T') > 0
            THEN parseDateTimeBestEffortOrNull(date_and_time_raw)
            WHEN match(date_and_time_raw, '\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}')
            THEN parseDateTimeBestEffortOrNull(date_and_time_raw)
            WHEN match(date_and_time_raw, '\\d{4}-\\d{2}-\\d{2}')
            THEN parseDateTimeBestEffortOrNull(date_and_time_raw)
            WHEN date_and_time_raw != '' AND date_and_time_raw != 'NULL'
            THEN parseDateTimeBestEffortOrNull(date_and_time_raw)
            ELSE NULL
        END AS parsed_datetime,
        CASE
            WHEN timestamp_raw != '' AND timestamp_raw != 'NULL' AND match(timestamp_raw, '^\\d+$')
            THEN toDateTime(toInt64(timestamp_raw) / 1000)
            ELSE NULL
        END AS parsed_timestamp
    FROM {{ ref('client_notes_raw') }}
),

cleaned_data AS (
    SELECT 
        client_note_id,
        COALESCE(parsed_datetime, parsed_timestamp)                AS note_datetime,
        COALESCE(toDate(parsed_datetime), toDate(parsed_timestamp)) AS note_date,
        trim(representative_code)                                  AS representative_code,
        trim(representative_name)                                  AS representative_name,
        trim(client_code)                                          AS client_code,
        trim(client_name)                                          AS client_name,
        CASE WHEN trim(street_address) != '' THEN trim(street_address) ELSE NULL END AS street_address,
        CASE WHEN trim(zip_code) != '' THEN trim(zip_code) ELSE NULL END AS zip_code,
        CASE WHEN trim(zip_ext) != '' THEN trim(zip_ext) ELSE NULL END AS zip_ext,
        CASE WHEN trim(city) != '' THEN trim(city) ELSE NULL END AS city,
        CASE WHEN trim(state) != '' THEN trim(state) ELSE NULL END AS state,
        CASE WHEN trim(country) != '' THEN trim(country) ELSE NULL END AS country,
        CASE WHEN trim(email) != '' THEN trim(email) ELSE NULL END AS email,
        CASE WHEN trim(phone) != '' THEN trim(phone) ELSE NULL END AS phone,
        CASE WHEN trim(mobile) != '' THEN trim(mobile) ELSE NULL END AS mobile,
        CASE WHEN trim(territory) != '' THEN trim(territory) ELSE NULL END AS territory,
        CASE 
            WHEN longitude_raw != '' AND longitude_raw != 'NULL' AND longitude_raw != '0'
                 AND match(longitude_raw, '^-?\\d+\\.?\\d*$')
            THEN toFloat64OrNull(longitude_raw)
            ELSE NULL
        END AS longitude,
        CASE 
            WHEN latitude_raw != '' AND latitude_raw != 'NULL' AND latitude_raw != '0'
                 AND match(latitude_raw, '^-?\\d+\\.?\\d*$')
            THEN toFloat64OrNull(latitude_raw)
            ELSE NULL
        END AS latitude,
        CASE 
            WHEN note_raw IS NOT NULL AND note_raw != '' AND note_raw != 'NULL' THEN 
                replaceRegexpAll(
                    replaceRegexpAll(trim(note_raw), '\\s+', ' '),
                    '[\\r\\n]+', ' '
                )
            ELSE NULL
        END AS note_cleaned,
        CASE WHEN visit_id != '' AND visit_id != 'NULL' THEN visit_id ELSE NULL END AS visit_id,
        extracted_at,
        source_system,
        endpoint,
        dbt_extracted_at,
        record_hash
    FROM date_parsing
    WHERE client_note_id IS NOT NULL
      AND client_code IS NOT NULL 
      AND trim(client_code) != ''
),

enhanced_data AS (
    SELECT
        *,
        CASE 
            WHEN street_address IS NOT NULL OR city IS NOT NULL OR state IS NOT NULL OR country IS NOT NULL THEN
                arrayStringConcat(
                    arrayFilter(x -> x != '', [
                        COALESCE(street_address, ''),
                        COALESCE(city, ''),
                        COALESCE(
                            CASE 
                                WHEN state IS NOT NULL AND zip_code IS NOT NULL 
                                THEN concat(state, ' ', zip_code, COALESCE(concat('-', zip_ext), ''))
                                WHEN state IS NOT NULL 
                                THEN state
                                WHEN zip_code IS NOT NULL 
                                THEN concat(zip_code, COALESCE(concat('-', zip_ext), ''))
                                ELSE ''
                            END, 
                            ''
                        ),
                        COALESCE(country, '')
                    ]), 
                    ', '
                )
            ELSE NULL
        END AS full_address,
        CASE WHEN note_date IS NOT NULL THEN toYear(note_date)      ELSE NULL END AS note_year,
        CASE WHEN note_date IS NOT NULL THEN toMonth(note_date)     ELSE NULL END AS note_month,
        CASE WHEN note_date IS NOT NULL THEN toDayOfWeek(note_date) ELSE NULL END AS note_day_of_week,
        CASE WHEN note_datetime IS NOT NULL THEN toHour(note_datetime) ELSE NULL END AS note_hour,
        CASE WHEN note_cleaned IS NOT NULL THEN length(note_cleaned) ELSE 0 END AS note_length,
        CASE 
            WHEN territory IS NOT NULL AND position(territory, '>') > 0 
            THEN trim(substring(territory, 1, position(territory, '>') - 1))
            WHEN territory IS NOT NULL AND territory != ''
            THEN territory
            ELSE NULL
        END AS territory_level_1,
        CASE 
            WHEN territory IS NOT NULL AND position(territory, '>') > 0 
            THEN trim(substring(territory, position(territory, '>') + 1))
            ELSE NULL
        END AS territory_level_2,
        CASE 
            WHEN country IN ('United States', 'USA', 'US') THEN
                CASE
                    WHEN state IN ('CA', 'California') THEN 'California'
                    WHEN state IN ('TX', 'Texas')      THEN 'Texas'
                    WHEN state IN ('NY', 'New York')   THEN 'New York'
                    WHEN state IN ('FL', 'Florida')    THEN 'Florida'
                    WHEN state IN ('MA', 'Massachusetts') THEN 'Massachusetts'
                    WHEN state IN ('IL', 'Illinois')   THEN 'Illinois'
                    WHEN state IN ('PA', 'Pennsylvania') THEN 'Pennsylvania'
                    WHEN state IN ('OH', 'Ohio')       THEN 'Ohio'
                    WHEN state IN ('WA', 'Washington') THEN 'Washington'
                    WHEN state IN ('OR', 'Oregon')     THEN 'Oregon'
                    ELSE state
                END
            ELSE state
        END AS state_standardized,
        CASE 
            WHEN positionCaseInsensitive(COALESCE(note_cleaned, ''), 'out of stock') > 0 
              OR positionCaseInsensitive(COALESCE(note_cleaned, ''), 'no stock') > 0 
              OR positionCaseInsensitive(COALESCE(note_cleaned, ''), 'inventory') > 0
            THEN 'Inventory Issue'
            WHEN positionCaseInsensitive(COALESCE(note_cleaned, ''), 'reorder') > 0 
              OR positionCaseInsensitive(COALESCE(note_cleaned, ''), 'restock') > 0 
              OR positionCaseInsensitive(COALESCE(note_cleaned, ''), 'order') > 0
              OR positionCaseInsensitive(COALESCE(note_cleaned, ''), 'purchase') > 0
            THEN 'Ordering'
            WHEN positionCaseInsensitive(COALESCE(note_cleaned, ''), 'promo') > 0 
              OR positionCaseInsensitive(COALESCE(note_cleaned, ''), 'deal') > 0 
              OR positionCaseInsensitive(COALESCE(note_cleaned, ''), 'sale') > 0
              OR positionCaseInsensitive(COALESCE(note_cleaned, ''), 'discount') > 0
            THEN 'Promotion'
            WHEN positionCaseInsensitive(COALESCE(note_cleaned, ''), 'display') > 0 
              OR positionCaseInsensitive(COALESCE(note_cleaned, ''), 'demo') > 0
              OR positionCaseInsensitive(COALESCE(note_cleaned, ''), 'merchandising') > 0
            THEN 'Display/Demo'
            WHEN positionCaseInsensitive(COALESCE(note_cleaned, ''), 'manager') > 0 
              OR positionCaseInsensitive(COALESCE(note_cleaned, ''), 'owner') > 0 
              OR positionCaseInsensitive(COALESCE(note_cleaned, ''), 'staff') > 0
              OR positionCaseInsensitive(COALESCE(note_cleaned, ''), 'employee') > 0
            THEN 'Staff Interaction'
            WHEN positionCaseInsensitive(COALESCE(note_cleaned, ''), 'vacation') > 0 
              OR positionCaseInsensitive(COALESCE(note_cleaned, ''), 'closed') > 0 
              OR positionCaseInsensitive(COALESCE(note_cleaned, ''), 'busy') > 0
              OR positionCaseInsensitive(COALESCE(note_cleaned, ''), 'unavailable') > 0
            THEN 'Availability'
            WHEN positionCaseInsensitive(COALESCE(note_cleaned, ''), 'training') > 0 
              OR positionCaseInsensitive(COALESCE(note_cleaned, ''), 'education') > 0
              OR positionCaseInsensitive(COALESCE(note_cleaned, ''), 'learn') > 0
            THEN 'Training'
            WHEN positionCaseInsensitive(COALESCE(note_cleaned, ''), 'product') > 0 
              OR positionCaseInsensitive(COALESCE(note_cleaned, ''), 'cart') > 0 
              OR positionCaseInsensitive(COALESCE(note_cleaned, ''), 'gummies') > 0
              OR positionCaseInsensitive(COALESCE(note_cleaned, ''), 'item') > 0
            THEN 'Product Discussion'
            WHEN note_cleaned IS NULL OR note_cleaned = '' THEN 'No Content'
            ELSE 'General'
        END AS note_category,
        CASE 
            WHEN positionCaseInsensitive(COALESCE(note_cleaned, ''), 'excited') > 0 
              OR positionCaseInsensitive(COALESCE(note_cleaned, ''), 'great') > 0 
              OR positionCaseInsensitive(COALESCE(note_cleaned, ''), 'excellent') > 0
              OR positionCaseInsensitive(COALESCE(note_cleaned, ''), 'fantastic') > 0
              OR positionCaseInsensitive(COALESCE(note_cleaned, ''), 'love') > 0
              OR positionCaseInsensitive(COALESCE(note_cleaned, ''), 'happy') > 0
            THEN 'Positive'
            WHEN positionCaseInsensitive(COALESCE(note_cleaned, ''), 'problem') > 0 
              OR positionCaseInsensitive(COALESCE(note_cleaned, ''), 'issue') > 0 
              OR positionCaseInsensitive(COALESCE(note_cleaned, ''), 'concern') > 0
              OR positionCaseInsensitive(COALESCE(note_cleaned, ''), 'complaint') > 0
              OR positionCaseInsensitive(COALESCE(note_cleaned, ''), 'unhappy') > 0
              OR positionCaseInsensitive(COALESCE(note_cleaned, ''), 'disappointed') > 0
            THEN 'Negative'
            ELSE 'Neutral'
        END AS note_sentiment,
        CASE 
            WHEN positionCaseInsensitive(COALESCE(note_cleaned, ''), 'urgent') > 0 
              OR positionCaseInsensitive(COALESCE(note_cleaned, ''), 'asap') > 0 
              OR positionCaseInsensitive(COALESCE(note_cleaned, ''), 'immediately') > 0
              OR positionCaseInsensitive(COALESCE(note_cleaned, ''), 'emergency') > 0
            THEN 'High'
            WHEN positionCaseInsensitive(COALESCE(note_cleaned, ''), 'follow up') > 0 
              OR positionCaseInsensitive(COALESCE(note_cleaned, ''), 'reorder') > 0 
              OR positionCaseInsensitive(COALESCE(note_cleaned, ''), 'need') > 0
              OR positionCaseInsensitive(COALESCE(note_cleaned, ''), 'important') > 0
            THEN 'Medium'
            ELSE 'Low'
        END AS business_priority,
        CASE 
            WHEN latitude IS NOT NULL AND longitude IS NOT NULL 
                 AND latitude BETWEEN -90 AND 90 
                 AND longitude BETWEEN -180 AND 180
            THEN 1
            ELSE 0
        END AS has_valid_gps_coordinates
    FROM cleaned_data
),

final_data AS (
    SELECT
        *,
        /* make a NOT NULL version for engine keys */
        CAST(note_date AS Date) AS note_date_nn,
        CASE note_day_of_week
            WHEN 1 THEN 'Monday'
            WHEN 2 THEN 'Tuesday'
            WHEN 3 THEN 'Wednesday'
            WHEN 4 THEN 'Thursday'
            WHEN 5 THEN 'Friday'
            WHEN 6 THEN 'Saturday'
            WHEN 7 THEN 'Sunday'
            ELSE 'Unknown'
        END AS day_of_week_name,
        CASE 
            WHEN note_hour IS NOT NULL AND note_hour BETWEEN 6 AND 11 THEN 'Morning'
            WHEN note_hour IS NOT NULL AND note_hour BETWEEN 12 AND 17 THEN 'Afternoon'
            WHEN note_hour IS NOT NULL AND note_hour BETWEEN 18 AND 21 THEN 'Evening'
            WHEN note_hour IS NOT NULL THEN 'Other'
            ELSE 'Unknown'
        END AS time_of_day,
        CASE 
            WHEN note_length >= 100 AND note_category NOT IN ('General', 'No Content') THEN 'Detailed'
            WHEN note_length >= 50  THEN 'Adequate'
            WHEN note_length >= 20  THEN 'Brief'
            WHEN note_length > 0    THEN 'Minimal'
            ELSE 'Empty'
        END AS note_quality,
        (
            CASE WHEN client_code IS NOT NULL               AND client_code != ''               THEN 10 ELSE 0 END +
            CASE WHEN representative_code IS NOT NULL       AND representative_code != ''       THEN 10 ELSE 0 END +
            CASE WHEN note_cleaned IS NOT NULL              AND length(note_cleaned) > 10       THEN 20 ELSE 0 END +
            CASE WHEN city IS NOT NULL                      AND city != ''                      THEN 10 ELSE 0 END +
            CASE WHEN state IS NOT NULL                     AND state != ''                     THEN 10 ELSE 0 END +
            CASE WHEN has_valid_gps_coordinates = 1                                          THEN 15 ELSE 0 END +
            CASE WHEN visit_id IS NOT NULL                                                   THEN 10 ELSE 0 END +
            CASE WHEN note_datetime IS NOT NULL                                              THEN 15 ELSE 0 END
        ) AS data_completeness_score,
        CASE 
            WHEN note_date IS NULL                         THEN 'Missing Date'
            WHEN note_date < toDate('2020-01-01')          THEN 'Date Too Old'
            WHEN note_date > toDate(now() + toIntervalDay(1)) THEN 'Future Date'
            ELSE 'Valid'
        END AS date_validation_status,
        now() AS processed_at
    FROM enhanced_data
)

SELECT *
FROM final_data
WHERE date_validation_status != 'Future Date'
  AND note_date IS NOT NULL
