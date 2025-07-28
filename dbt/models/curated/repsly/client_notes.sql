{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(client_note_id, processed_at)',
    schema=var('silver_schema', 'repsly_silver'),
    partition_by='toYYYYMM(COALESCE(note_date, toDate(''1970-01-01'')))',
    meta={
        'description': 'Cleaned and enhanced client notes data with robust date parsing',
        'business_key': 'client_note_id',
        'update_strategy': 'latest_wins'
    }
) }}

-- Silver layer: Cleaned, typed, and deduplicated data
WITH latest_records AS (
    SELECT *,
        row_number() OVER (
            PARTITION BY client_note_id
            ORDER BY 
                dbt_loaded_at DESC,
                record_hash DESC
        ) AS row_num
    FROM {{ ref('client_notes_raw') }}
    WHERE client_note_id != '' AND client_note_id != 'NULL'
),

with_timestamps AS (
    SELECT *,
        -- Parse extracted_at (stored as string in bronze)
        parseDateTimeBestEffortOrNull(extracted_at) AS extraction_datetime,
        
        -- dbt_loaded_at is already DateTime from now() in bronze
        dbt_loaded_at AS bronze_loaded_datetime,
        
        -- Parse DateAndTime field (main business timestamp)
        CASE 
            WHEN date_and_time_raw LIKE '/Date(%' AND date_and_time_raw LIKE '%)'
            THEN 
                CASE
                    WHEN position(date_and_time_raw, '/Date(') > 0 AND position(date_and_time_raw, ')') > 0
                    THEN 
                        toDateTime(
                            toInt64(
                                substring(
                                    date_and_time_raw,
                                    position(date_and_time_raw, '/Date(') + 6,
                                    position(date_and_time_raw, ')') - position(date_and_time_raw, '/Date(') - 6
                                )
                            ) / 1000
                        )
                    ELSE NULL
                END
            WHEN date_and_time_raw != '' AND date_and_time_raw != 'NULL'
            THEN parseDateTimeBestEffortOrNull(date_and_time_raw)
            ELSE NULL
        END AS note_datetime,
        
        -- Parse TimeStamp field (Repsly proprietary)
        CASE
            WHEN timestamp_raw != '' AND timestamp_raw != 'NULL' AND match(timestamp_raw, '^\\d+$')
            THEN toDateTime(toInt64(timestamp_raw) / 1000)
            ELSE NULL
        END AS repsly_timestamp,
        
        -- Current processing time
        now() AS processed_at

    FROM latest_records
    WHERE row_num = 1
),

typed_data AS (
    SELECT
        -- Business identifiers
        client_note_id,
        NULLIF(trimBoth(client_code), '') AS client_code,
        NULLIF(trimBoth(client_name), '') AS client_name,
        NULLIF(trimBoth(representative_code), '') AS representative_code,
        NULLIF(trimBoth(representative_name), '') AS representative_name,
        NULLIF(trimBoth(visit_id), '') AS visit_id,

        -- Timestamps
        COALESCE(note_datetime, repsly_timestamp) AS note_datetime_final,
        COALESCE(toDate(note_datetime), toDate(repsly_timestamp)) AS note_date,
        extraction_datetime AS data_extracted_at,
        bronze_loaded_datetime AS bronze_processed_at,
        processed_at,

        -- Note content
        CASE 
            WHEN note_raw IS NOT NULL AND note_raw != '' AND note_raw != 'NULL' THEN 
                replaceRegexpAll(
                    replaceRegexpAll(trimBoth(note_raw), '\\s+', ' '),
                    '[\\r\\n]+', ' '
                )
            ELSE NULL
        END AS note_cleaned,

        -- Geographic coordinates with validation
        CASE 
            WHEN longitude_raw != '' AND longitude_raw != 'NULL' AND longitude_raw != '0'
                 AND match(longitude_raw, '^-?\\d*\\.?\\d+$')
                 AND toFloat64OrNull(longitude_raw) BETWEEN -180 AND 180
            THEN toFloat64OrNull(longitude_raw)
            ELSE NULL
        END AS longitude,

        CASE 
            WHEN latitude_raw != '' AND latitude_raw != 'NULL' AND latitude_raw != '0'
                 AND match(latitude_raw, '^-?\\d*\\.?\\d+$')
                 AND toFloat64OrNull(latitude_raw) BETWEEN -90 AND 90
            THEN toFloat64OrNull(latitude_raw)
            ELSE NULL
        END AS latitude,

        -- Address fields
        NULLIF(trimBoth(street_address), '') AS street_address,
        NULLIF(trimBoth(zip_code), '') AS zip_code,
        NULLIF(trimBoth(zip_ext), '') AS zip_ext,
        NULLIF(trimBoth(city), '') AS city,
        NULLIF(trimBoth(state), '') AS state,
        NULLIF(trimBoth(country), '') AS country,

        -- Contact information
        CASE 
            WHEN email != '' AND email != 'NULL' 
                 AND match(email, '^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$')
            THEN trimBoth(email)
            ELSE NULL
        END AS email_clean,

        NULLIF(trimBoth(phone), '') AS phone_clean,
        NULLIF(trimBoth(mobile), '') AS mobile_clean,

        -- Territory
        NULLIF(trimBoth(territory), '') AS territory,

        -- System metadata
        source_system,
        endpoint,
        record_hash

    FROM with_timestamps
),

enhanced_data AS (
    SELECT
        *,
        
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

        -- Date components
        CASE WHEN note_date IS NOT NULL THEN toYear(note_date) ELSE NULL END AS note_year,
        CASE WHEN note_date IS NOT NULL THEN toMonth(note_date) ELSE NULL END AS note_month,
        CASE WHEN note_date IS NOT NULL THEN toDayOfWeek(note_date) ELSE NULL END AS note_day_of_week,
        CASE WHEN note_datetime_final IS NOT NULL THEN toHour(note_datetime_final) ELSE NULL END AS note_hour,

        -- Note analysis
        CASE WHEN note_cleaned IS NOT NULL THEN length(note_cleaned) ELSE 0 END AS note_length,

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

        -- Business categorization
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
            WHEN note_cleaned IS NULL OR note_cleaned = '' THEN 'No Content'
            ELSE 'General'
        END AS note_category,

        -- Sentiment analysis
        CASE 
            WHEN positionCaseInsensitive(COALESCE(note_cleaned, ''), 'excited') > 0 
              OR positionCaseInsensitive(COALESCE(note_cleaned, ''), 'great') > 0 
              OR positionCaseInsensitive(COALESCE(note_cleaned, ''), 'excellent') > 0
              OR positionCaseInsensitive(COALESCE(note_cleaned, ''), 'love') > 0
              OR positionCaseInsensitive(COALESCE(note_cleaned, ''), 'happy') > 0
            THEN 'Positive'
            WHEN positionCaseInsensitive(COALESCE(note_cleaned, ''), 'problem') > 0 
              OR positionCaseInsensitive(COALESCE(note_cleaned, ''), 'issue') > 0 
              OR positionCaseInsensitive(COALESCE(note_cleaned, ''), 'concern') > 0
              OR positionCaseInsensitive(COALESCE(note_cleaned, ''), 'complaint') > 0
            THEN 'Negative'
            ELSE 'Neutral'
        END AS note_sentiment,

        -- Data quality flags
        CASE 
            WHEN latitude IS NOT NULL AND longitude IS NOT NULL 
                 AND latitude BETWEEN -90 AND 90 
                 AND longitude BETWEEN -180 AND 180
            THEN 1
            ELSE 0
        END AS has_valid_gps_coordinates

    FROM typed_data
),

final_data AS (
    SELECT
        *,
        
        -- Day of week name
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

        -- Time of day
        CASE 
            WHEN note_hour IS NOT NULL AND note_hour BETWEEN 6 AND 11 THEN 'Morning'
            WHEN note_hour IS NOT NULL AND note_hour BETWEEN 12 AND 17 THEN 'Afternoon'
            WHEN note_hour IS NOT NULL AND note_hour BETWEEN 18 AND 21 THEN 'Evening'
            WHEN note_hour IS NOT NULL THEN 'Other'
            ELSE 'Unknown'
        END AS time_of_day,

        -- Note quality assessment
        CASE 
            WHEN note_length >= 100 AND note_category NOT IN ('General', 'No Content') THEN 'Detailed'
            WHEN note_length >= 50 THEN 'Adequate'
            WHEN note_length >= 20 THEN 'Brief'
            WHEN note_length > 0 THEN 'Minimal'
            ELSE 'Empty'
        END AS note_quality,

        -- Data completeness score
        (
            CASE WHEN client_code IS NOT NULL AND client_code != '' THEN 10 ELSE 0 END +
            CASE WHEN representative_code IS NOT NULL AND representative_code != '' THEN 10 ELSE 0 END +
            CASE WHEN note_cleaned IS NOT NULL AND length(note_cleaned) > 10 THEN 20 ELSE 0 END +
            CASE WHEN city IS NOT NULL AND city != '' THEN 10 ELSE 0 END +
            CASE WHEN state IS NOT NULL AND state != '' THEN 10 ELSE 0 END +
            CASE WHEN has_valid_gps_coordinates = 1 THEN 15 ELSE 0 END +
            CASE WHEN visit_id IS NOT NULL THEN 10 ELSE 0 END +
            CASE WHEN note_datetime_final IS NOT NULL THEN 15 ELSE 0 END
        ) AS data_completeness_score,

        -- Date validation
        CASE 
            WHEN note_date IS NULL THEN 'Missing Date'
            WHEN note_date < toDate('2020-01-01') THEN 'Date Too Old'
            WHEN note_date > toDate(now() + toIntervalDay(1)) THEN 'Future Date'
            ELSE 'Valid'
        END AS date_validation_status

    FROM enhanced_data
)

SELECT *
FROM final_data
WHERE date_validation_status != 'Future Date'
ORDER BY client_note_id, processed_at