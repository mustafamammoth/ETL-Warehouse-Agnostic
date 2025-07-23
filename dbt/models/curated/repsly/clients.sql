-- Cleaned & enhanced client data from Repsly (ClickHouse)
-- NOTE: Comments cannot be inside the {{ config() }} block.

{{ config(
    materialized='table',
    schema=var('silver_schema', 'repsly_silver'),
    engine='MergeTree()',
    order_by='(client_id, created_date_nn)',
    partition_by='toYYYYMM(created_date_nn)',
    meta={'description': 'Cleaned & enhanced client data from Repsly'}
) }}

WITH parsed AS (
    SELECT
        client_id,
        trimBoth(client_code) AS client_code,
        trimBoth(client_name) AS client_name,

        /* booleans / numerics */
        multiIf(lower(active_raw) IN ('true','1','t','yes'), 1, 0)                AS is_active,
        toFloat64OrNull(NULLIF(discount_raw,   ''))                               AS discount_percentage,
        toFloat64OrNull(NULLIF(credit_limit_raw,''))                              AS credit_limit,

        /* dates */
        parseDateTimeBestEffortOrNull(NULLIF(modified_date_raw,''))               AS modified_date,
        parseDateTimeBestEffortOrNull(NULLIF(created_date_raw,''))                AS created_date_dt,
        parseDateTimeBestEffortOrNull(NULLIF(last_visit_date_raw,''))             AS last_visit_date_dt,
        parseDateTimeBestEffortOrNull(NULLIF(next_visit_date_raw,''))             AS next_visit_date_dt,

        /* NON-NULL version for MergeTree keys */
        CAST(
            COALESCE(
                toDate(parseDateTimeBestEffortOrNull(NULLIF(created_date_raw,''))),
                toDate('1970-01-01')
            ) AS Date
        ) AS created_date_nn,

        /* address / contact */
        nullIf(trimBoth(street_address),   '') AS street_address,
        nullIf(trimBoth(zip_code),         '') AS zip_code,
        nullIf(trimBoth(zip_ext),          '') AS zip_ext,
        nullIf(trimBoth(city),             '') AS city,
        nullIf(trimBoth(state),            '') AS state,
        nullIf(trimBoth(country),          '') AS country,
        nullIf(trimBoth(email),            '') AS email_clean,
        nullIf(trimBoth(phone),            '') AS phone_clean,
        nullIf(trimBoth(mobile),           '') AS mobile_clean,

        nullIf(trimBoth(territory),        '') AS territory,

        nullIf(trimBoth(representative_code), '') AS representative_code,
        nullIf(trimBoth(representative_name), '') AS representative_name,

        nullIf(trimBoth(notes_raw),        '') AS notes_raw,
        nullIf(trimBoth(tags_raw),         '') AS tags_raw,
        nullIf(trimBoth(custom_fields_raw), '') AS custom_fields_raw,

        nullIf(trimBoth(client_type),      '') AS client_type,
        nullIf(trimBoth(client_sub_type),  '') AS client_sub_type,
        nullIf(trimBoth(client_status),    '') AS client_status,

        nullIf(trimBoth(payment_terms),    '') AS payment_terms,
        nullIf(trimBoth(currency),         '') AS currency,
        nullIf(trimBoth(price_list),       '') AS price_list,

        nullIf(trimBoth(company_number),   '') AS company_number,
        nullIf(trimBoth(tax_number),       '') AS tax_number,
        nullIf(trimBoth(fax),              '') AS fax,
        nullIf(trimBoth(website),          '') AS website,
        nullIf(trimBoth(contact_person),   '') AS contact_person,

        /* geo */
        CASE 
            WHEN longitude_raw != '' AND longitude_raw != 'NULL' 
                 AND match(longitude_raw, '^-?\\d+\\.?\\d*$')
            THEN toFloat64OrNull(longitude_raw)
            ELSE NULL
        END AS longitude,
        CASE 
            WHEN latitude_raw  != '' AND latitude_raw  != 'NULL'
                 AND match(latitude_raw,  '^-?\\d+\\.?\\d*$')
            THEN toFloat64OrNull(latitude_raw)
            ELSE NULL
        END AS latitude,

        /* metadata */
        extracted_at,
        source_system,
        endpoint,
        dbt_extracted_at,
        record_hash
    FROM {{ ref('clients_raw') }}
),

enhanced AS (
    SELECT
        *,
        /* full address */
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

        /* territory split */
        CASE 
            WHEN territory IS NOT NULL AND position(territory, '>') > 0 
            THEN trimBoth(substring(territory, 1, position(territory, '>') - 1))
            WHEN territory IS NOT NULL AND territory != '' THEN territory
            ELSE NULL
        END AS territory_level_1,
        CASE 
            WHEN territory IS NOT NULL AND position(territory, '>') > 0 
            THEN trimBoth(substring(territory, position(territory, '>') + 1))
            ELSE NULL
        END AS territory_level_2,

        /* state standardization */
        CASE 
            WHEN country IN ('United States','USA','US') THEN
                CASE
                    WHEN state IN ('CA','California')      THEN 'California'
                    WHEN state IN ('TX','Texas')           THEN 'Texas'
                    WHEN state IN ('NY','New York')        THEN 'New York'
                    WHEN state IN ('FL','Florida')         THEN 'Florida'
                    WHEN state IN ('MA','Massachusetts')   THEN 'Massachusetts'
                    WHEN state IN ('IL','Illinois')        THEN 'Illinois'
                    WHEN state IN ('PA','Pennsylvania')    THEN 'Pennsylvania'
                    WHEN state IN ('OH','Ohio')            THEN 'Ohio'
                    WHEN state IN ('WA','Washington')      THEN 'Washington'
                    WHEN state IN ('OR','Oregon')          THEN 'Oregon'
                    ELSE state
                END
            ELSE state
        END AS state_standardized,

        /* email quality */
        CASE 
            WHEN email_clean IS NOT NULL AND match(email_clean, '^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$') THEN 1
            ELSE 0
        END AS email_valid_flag,

        /* gps quality */
        CASE 
            WHEN longitude IS NOT NULL AND latitude IS NOT NULL 
                 AND latitude BETWEEN -90 AND 90 AND longitude BETWEEN -180 AND 180
            THEN 1 ELSE 0
        END AS has_valid_gps_coordinates
    FROM parsed
),

scored AS (
    SELECT
        *,
        (
            CASE WHEN client_code   IS NOT NULL AND client_code   != '' THEN 10 ELSE 0 END +
            CASE WHEN client_name   IS NOT NULL AND client_name   != '' THEN 10 ELSE 0 END +
            CASE WHEN is_active     IS NOT NULL                       THEN 5  ELSE 0 END +
            CASE WHEN email_clean   IS NOT NULL AND email_clean   != '' THEN 5  ELSE 0 END +
            CASE WHEN phone_clean   IS NOT NULL AND phone_clean   != '' THEN 5  ELSE 0 END +
            CASE WHEN full_address  IS NOT NULL AND full_address  != '' THEN 10 ELSE 0 END +
            CASE WHEN has_valid_gps_coordinates = 1                    THEN 10 ELSE 0 END +
            CASE WHEN created_date_dt IS NOT NULL                      THEN 10 ELSE 0 END +
            CASE WHEN modified_date   IS NOT NULL                      THEN 10 ELSE 0 END
        ) AS data_completeness_score,
        now() AS processed_at
    FROM enhanced
)

SELECT *
FROM scored
ORDER BY client_id
