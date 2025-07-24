{{ config(
    materialized      = 'table',
    schema            = var('silver_schema', 'repsly_silver'),
    engine            = 'MergeTree()',
    order_by          = '(representative_id, processed_at)',
    partition_by      = 'toYYYYMM(processed_at)',
    meta = {
        'description'    : 'Cleaned & enhanced representatives data from Repsly',
        'business_key'   : 'representative_id',
        'update_strategy': 'latest_wins'
    }
) }}

-- Silver layer: cleaned, typed, enriched
WITH latest_records AS (
    SELECT *
         , row_number() OVER (
               PARTITION BY representative_id
               ORDER BY dbt_loaded_at DESC, record_hash DESC
           ) AS row_num
    FROM {{ ref('representatives_raw') }}
    WHERE representative_id NOT IN ('', 'NULL')
),

with_timestamps AS (
    SELECT *
         , parseDateTimeBestEffortOrNull(extracted_at) AS extraction_datetime
         , dbt_loaded_at                               AS bronze_loaded_datetime
         , now()                                       AS processed_at
    FROM latest_records
    WHERE row_num = 1
),

representative_cleaning AS (
    SELECT
        -- identifiers
        representative_id,
        NULLIF(trimBoth(code), '')  AS representative_code,
        NULLIF(trimBoth(name), '')  AS representative_name,
        NULLIF(trimBoth(note), '')  AS note,

        -- contact info
        NULLIF(trimBoth(lower(email)), '') AS email_raw,
        NULLIF(trimBoth(phone), '')        AS phone_raw,
        NULLIF(trimBoth(mobile), '')       AS mobile_raw,

        -- boolean
        multiIf(
            lower(active_raw) IN ('true','1','t','yes'), 1,
            lower(active_raw) IN ('false','0','f','no'), 0,
            NULL
        ) AS is_active,

        -- address
        NULLIF(trimBoth(address1), '')     AS address1,
        NULLIF(trimBoth(address2), '')     AS address2,
        NULLIF(trimBoth(city), '')         AS city,
        NULLIF(trimBoth(state), '')        AS state,
        NULLIF(trimBoth(zip_code), '')     AS zip_code,
        NULLIF(trimBoth(zip_code_ext), '') AS zip_code_ext,
        NULLIF(trimBoth(country), '')      AS country,
        NULLIF(trimBoth(country_code), '') AS country_code,

        -- json blobs
        NULLIF(trimBoth(territories_json), '') AS territories_json,
        NULLIF(trimBoth(attributes_json),  '') AS attributes_json,

        -- timestamps
        extraction_datetime,
        bronze_loaded_datetime,
        processed_at,

        -- system metadata
        source_system,
        endpoint,
        record_hash
    FROM with_timestamps
),

representative_enhancement AS (
    SELECT
        *,
        -- email validation - avoid splitByChar in multiIf
        CASE
            WHEN email_raw != '' AND match(email_raw, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$')
            THEN email_raw 
            ELSE NULL 
        END AS email_clean,

        -- phone clean‑up
        CASE
            WHEN phone_raw != ''
            THEN CASE
                     WHEN length(replaceRegexpAll(phone_raw, '[^0-9]', '')) = 10
                     THEN replaceRegexpAll(phone_raw, '[^0-9]', '')
                     WHEN length(replaceRegexpAll(phone_raw, '[^0-9]', '')) = 11
                          AND startsWith(replaceRegexpAll(phone_raw, '[^0-9]', ''), '1')
                     THEN substring(replaceRegexpAll(phone_raw, '[^0-9]', ''), 2)
                     ELSE NULL
                 END
            ELSE NULL
        END AS phone_clean,

        -- mobile clean‑up
        CASE
            WHEN mobile_raw != ''
            THEN CASE
                     WHEN length(replaceRegexpAll(mobile_raw, '[^0-9]', '')) = 10
                     THEN replaceRegexpAll(mobile_raw, '[^0-9]', '')
                     WHEN length(replaceRegexpAll(mobile_raw, '[^0-9]', '')) = 11
                          AND startsWith(replaceRegexpAll(mobile_raw, '[^0-9]', ''), '1')
                     THEN substring(replaceRegexpAll(mobile_raw, '[^0-9]', ''), 2)
                     ELSE NULL
                 END
            ELSE NULL
        END AS mobile_clean,

        -- email domain - extract domain without splitByChar in multiIf
        CASE
            WHEN email_raw != '' AND match(email_raw, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$')
            THEN lower(replaceRegexpOne(email_raw, '^.*@', ''))
            ELSE NULL
        END AS email_domain,

        -- name split - avoid splitByChar in multiIf by using regex
        CASE
            WHEN position(representative_name, ' ') > 0
            THEN trimBoth(replaceRegexpOne(representative_name, ' .*$', ''))
            WHEN representative_name IS NOT NULL
            THEN representative_name
            ELSE NULL
        END AS first_name,

        CASE
            WHEN position(representative_name, ' ') > 0
            THEN trimBoth(replaceRegexpOne(representative_name, '^[^ ]+ ', ''))
            ELSE NULL
        END AS last_name,

        -- territory parsing - avoid splitByChar in multiIf
        CASE
            WHEN territories_json LIKE '%"%' 
            THEN trimBoth(replaceRegexpAll(
                     replaceRegexpOne(replaceRegexpAll(territories_json, '[\\[\\]"]', ''), ',.*$', ''),
                     '^\\s+|\\s+$', ''
                 ))
            WHEN territories_json IS NOT NULL AND territories_json != ''
            THEN territories_json
            ELSE NULL
        END AS primary_territory,

        -- territory count - avoid splitByChar by using regex
        CASE
            WHEN territories_json LIKE '%,%'
            THEN length(territories_json) - length(replaceRegexpAll(territories_json, ',', '')) + 1
            WHEN territories_json NOT IN ('', '[]') AND territories_json IS NOT NULL
            THEN 1
            ELSE 0
        END AS territory_count
    FROM representative_cleaning
),

representative_business_logic AS (
    SELECT
        re.*,

        -- company mapping
        CASE 
            WHEN re.email_domain = 'mammoth.org'                                    THEN 'Mammoth Distribution'
            WHEN re.email_domain = '710labs.com'                                    THEN '710 Labs'
            WHEN re.email_domain = 'purebeautypurebeauty.co'                        THEN 'Pure Beauty'
            WHEN re.email_domain = 'heavyhitters.co'                                THEN 'Heavy Hitters'
            WHEN re.email_domain = 'repsly.com'                                     THEN 'Repsly (External)'
            WHEN re.email_domain LIKE '%test%' OR re.email_domain LIKE '%demo%'     THEN 'Test Account'
            WHEN re.email_domain IS NOT NULL                                        THEN 'External'
            ELSE 'Unknown'
        END AS company_affiliation,

        -- representative type
        CASE 
            WHEN re.primary_territory LIKE '%710 Labs%'                             THEN '710 Labs Representative'
            WHEN re.primary_territory LIKE '%California%'    AND re.email_domain = 'mammoth.org'
                                                                                     THEN 'Mammoth CA Representative'
            WHEN re.primary_territory LIKE '%California%'                            THEN 'California Representative'
            WHEN re.primary_territory LIKE '%Colorado%'                              THEN 'Colorado Representative'
            WHEN re.primary_territory LIKE '%Michigan%'                              THEN 'Michigan Representative'
            WHEN re.primary_territory LIKE '%New York%'                              THEN 'New York Representative'
            WHEN re.territory_count > 1                                              THEN 'Multi-Territory Representative'
            WHEN re.territory_count = 1                                              THEN 'Single Territory Representative'
            ELSE 'No Territory Assignment'
        END AS representative_type,

        -- status / quality / tiers
        CASE WHEN re.is_active = 1 AND re.email_clean IS NOT NULL                   THEN 'Active'
             WHEN re.is_active = 1 AND re.email_clean IS NULL                       THEN 'Active - No Email'
             WHEN re.is_active = 0                                                  THEN 'Inactive'
             ELSE 'Unknown Status' END                                              AS representative_status,

        CASE WHEN re.email_clean IS NOT NULL AND (re.phone_clean IS NOT NULL OR re.mobile_clean IS NOT NULL)
                                                                                     THEN 'Excellent'
             WHEN re.email_clean IS NOT NULL                                        THEN 'Good'
             WHEN re.phone_clean IS NOT NULL OR re.mobile_clean IS NOT NULL         THEN 'Phone Only'
             ELSE 'Poor' END                                                        AS contact_quality,

        CASE WHEN re.address1 IS NOT NULL AND re.city IS NOT NULL AND re.state IS NOT NULL AND re.zip_code IS NOT NULL
                                                                                     THEN 'Complete'
             WHEN re.city IS NOT NULL AND re.state IS NOT NULL                       THEN 'Partial'
             WHEN re.state IS NOT NULL OR re.city IS NOT NULL                        THEN 'Minimal'
             ELSE 'Missing' END                                                     AS address_quality,

        CASE WHEN re.territory_count >= 3                                           THEN 'Multi-Regional'
             WHEN re.territory_count = 2                                            THEN 'Dual Territory'
             WHEN re.territory_count = 1                                            THEN 'Single Territory'
             ELSE 'No Territory' END                                                AS territory_scope,

        CASE WHEN re.email_domain = 'mammoth.org'  AND re.territory_count >= 1      THEN 'Internal - Primary'
             WHEN re.email_domain IN ('710labs.com','purebeautypurebeauty.co','heavyhitters.co')
                  AND re.territory_count >= 1                                       THEN 'Partner - Primary'
             WHEN re.territory_count >= 1                                           THEN 'External - Active'
             WHEN re.is_active = 1                                                 THEN 'Active - Unassigned'
             ELSE 'Inactive/Unknown' END                                           AS representative_tier,

        CASE WHEN re.primary_territory LIKE '%CA%' OR re.primary_territory LIKE '%California%'
                                                                                     THEN 'West Coast'
             WHEN re.primary_territory LIKE '%CO%' OR re.primary_territory LIKE '%Colorado%'
                                                                                     THEN 'Mountain West'
             WHEN re.primary_territory LIKE '%MI%' OR re.primary_territory LIKE '%Michigan%'
                                                                                     THEN 'Midwest'
             WHEN re.primary_territory LIKE '%NY%' OR re.primary_territory LIKE '%New York%'
                                                                                     THEN 'Northeast'
             ELSE 'Other/Unknown' END                                              AS geographic_region,

        -- data completeness score
        ( CASE WHEN re.representative_code IS NOT NULL                               THEN 15 ELSE 0 END +
          CASE WHEN re.representative_name IS NOT NULL                               THEN 15 ELSE 0 END +
          CASE WHEN re.email_clean IS NOT NULL                                       THEN 20 ELSE 0 END +
          CASE WHEN re.phone_clean IS NOT NULL OR re.mobile_clean IS NOT NULL        THEN 15 ELSE 0 END +
          CASE WHEN re.territory_count > 0                                           THEN 15 ELSE 0 END +
          CASE WHEN re.city IS NOT NULL AND re.state IS NOT NULL                     THEN 10 ELSE 0 END +
          CASE WHEN re.is_active = 1                                                 THEN 10 ELSE 0 END
        ) AS data_completeness_score,

        -- preferred contact
        CASE WHEN re.email_clean IS NOT NULL                 THEN 'Email'
             WHEN re.mobile_clean IS NOT NULL                THEN 'Mobile'
             WHEN re.phone_clean  IS NOT NULL                THEN 'Phone'
             ELSE 'No Contact Method' END                    AS primary_contact_method,

        -- contact flag
        CASE WHEN re.email_clean IS NOT NULL
                  OR re.phone_clean IS NOT NULL
                  OR re.mobile_clean IS NOT NULL
        THEN 1 ELSE 0 END                                   AS has_contact_info,

        -- specialization
        CASE
            WHEN re.primary_territory LIKE '%710 Labs%'      THEN '710 Labs Specialist'
            WHEN re.email_domain = 'mammoth.org'             THEN 'Mammoth Internal'
            WHEN re.territory_count > 2                      THEN 'Multi-Regional Specialist'
            WHEN re.territory_count = 1                      THEN 'Territory Specialist'
            ELSE 'General Representative'
        END AS specialization
    FROM representative_enhancement re
)

SELECT *
FROM representative_business_logic