{{ config(
    materialized='table',
    schema=var('silver_schema', 'repsly_silver'),
    engine='MergeTree()',
    order_by='(client_id, processed_at)',
    partition_by='toYYYYMM(processed_at)',
    meta={
        'description': 'Cleaned & enhanced client data from Repsly',
        'business_key': 'client_id',
        'update_strategy': 'latest_wins'
    }
) }}

-- Silver layer: Cleaned, typed, and deduplicated data
WITH latest_records AS (
    SELECT *,
        row_number() OVER (
            PARTITION BY client_id
            ORDER BY 
                dbt_loaded_at DESC,
                record_hash DESC
        ) AS row_num
    FROM {{ ref('clients_raw') }}
    WHERE client_id != '' AND client_id != 'NULL'
),

with_timestamps AS (
    SELECT *,
        -- Parse extracted_at (stored as string in bronze)
        parseDateTimeBestEffortOrNull(extracted_at) AS extraction_datetime,
        
        -- dbt_loaded_at is already DateTime from now() in bronze
        dbt_loaded_at AS bronze_loaded_datetime,
        
        -- Keep Repsly timestamp as reference
        timestamp_raw as repsly_timestamp_raw,
        
        -- Current processing time
        now() AS processed_at

    FROM latest_records
    WHERE row_num = 1
),

typed_data AS (
    SELECT
        -- Business identifiers
        client_id,
        NULLIF(trimBoth(client_code), '') AS client_code,
        NULLIF(trimBoth(client_name), '') AS client_name,

        -- Use our reliable timestamps
        extraction_datetime AS data_extracted_at,
        bronze_loaded_datetime AS bronze_processed_at,
        processed_at,
        repsly_timestamp_raw,

        -- Boolean fields
        multiIf(
            lower(active_raw) IN ('true','1','t','yes'), 1,
            lower(active_raw) IN ('false','0','f','no'), 0,
            NULL
        ) AS is_active,

        -- Geographic coordinates with validation
        CASE 
            WHEN longitude_raw != '' AND longitude_raw != 'NULL' 
                 AND match(longitude_raw, '^-?\\d*\\.?\\d+$')
                 AND toFloat64OrNull(longitude_raw) BETWEEN -180 AND 180
            THEN toFloat64OrNull(longitude_raw)
            ELSE NULL
        END AS longitude,

        CASE 
            WHEN latitude_raw != '' AND latitude_raw != 'NULL'
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
        NULLIF(trimBoth(website), '') AS website,
        NULLIF(trimBoth(contact_person), '') AS contact_person,
        NULLIF(trimBoth(contact_title), '') AS contact_title,

        -- Business classification  
        NULLIF(trimBoth(territory), '') AS territory,
        NULLIF(trimBoth(representative_code), '') AS representative_code,
        NULLIF(trimBoth(representative_name), '') AS representative_name,
        NULLIF(trimBoth(client_status), '') AS client_status,

        -- Other business data
        NULLIF(trimBoth(price_lists), '') AS price_lists,
        NULLIF(trimBoth(account_code), '') AS account_code,

        -- Raw text fields
        NULLIF(trimBoth(notes_raw), '') AS notes_raw,
        NULLIF(trimBoth(tags_raw), '') AS tags_raw,
        NULLIF(trimBoth(custom_fields_raw), '') AS custom_fields_raw,

        -- System metadata
        source_system,
        endpoint,
        record_hash

    FROM with_timestamps
),

enhanced AS (
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

        -- Data quality flags
        CASE WHEN email_clean IS NOT NULL THEN 1 ELSE 0 END AS has_valid_email,
        CASE WHEN longitude IS NOT NULL AND latitude IS NOT NULL THEN 1 ELSE 0 END AS has_valid_coordinates,
        CASE WHEN phone_clean IS NOT NULL OR mobile_clean IS NOT NULL THEN 1 ELSE 0 END AS has_contact_number

    FROM typed_data
),

scored AS (
    SELECT
        *,
        -- Data completeness score
        (
            CASE WHEN client_code IS NOT NULL THEN 15 ELSE 0 END +
            CASE WHEN client_name IS NOT NULL THEN 15 ELSE 0 END +
            CASE WHEN is_active IS NOT NULL THEN 10 ELSE 0 END +
            CASE WHEN has_valid_email = 1 THEN 10 ELSE 0 END +
            CASE WHEN has_contact_number = 1 THEN 10 ELSE 0 END +
            CASE WHEN full_address IS NOT NULL THEN 15 ELSE 0 END +
            CASE WHEN has_valid_coordinates = 1 THEN 15 ELSE 0 END +
            CASE WHEN data_extracted_at IS NOT NULL THEN 5 ELSE 0 END +
            CASE WHEN representative_code IS NOT NULL THEN 3 ELSE 0 END +
            CASE WHEN territory IS NOT NULL THEN 2 ELSE 0 END
        ) AS data_completeness_score

    FROM enhanced
)

SELECT *
FROM scored
ORDER BY client_id, processed_at