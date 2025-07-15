-- Cleaned and standardized representatives data (dbt/models/curated/repsly/representatives.sql)
{{ config(materialized='table') }}

WITH representative_cleaning AS (
    SELECT 
        -- Clean basic identifiers
        TRIM(COALESCE(code, '')) AS representative_code,
        TRIM(COALESCE(name, '')) AS representative_name,
        TRIM(COALESCE(note, '')) AS note,

        -- Clean contact information
        TRIM(LOWER(COALESCE(email, ''))) AS email_raw,
        TRIM(COALESCE(phone, '')) AS phone_raw,
        TRIM(COALESCE(mobile, '')) AS mobile_raw,

        -- Boolean conversions
        CASE WHEN UPPER(TRIM(COALESCE(active_raw, 'FALSE'))) = 'TRUE' THEN TRUE ELSE FALSE END AS is_active,

        -- Address information
        TRIM(COALESCE(address1, '')) AS address1,
        TRIM(COALESCE(address2, '')) AS address2,
        TRIM(COALESCE(city, '')) AS city,
        TRIM(COALESCE(state, '')) AS state,
        TRIM(COALESCE(zip_code, '')) AS zip_code,
        TRIM(COALESCE(zip_code_ext, '')) AS zip_code_ext,
        TRIM(COALESCE(country, '')) AS country,
        TRIM(COALESCE(country_code, '')) AS country_code,

        -- JSON fields (territories and attributes)
        TRIM(COALESCE(territories_json, '')) AS territories_json,
        TRIM(COALESCE(attributes_json, '')) AS attributes_json,

        -- Metadata
        extracted_at,
        source_system,
        endpoint

    FROM {{ ref('representatives_raw') }}
),

representative_enhancement AS (
    SELECT
        *,
        -- Email validation and cleaning
        CASE 
            WHEN email_raw ~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$' THEN email_raw
            WHEN email_raw <> '' THEN NULL  -- Invalid email format
            ELSE NULL
        END AS email_clean,

        -- Phone number cleaning (remove all non-digits, then format)
        CASE 
            WHEN phone_raw <> '' THEN 
                CASE 
                    WHEN LENGTH(REGEXP_REPLACE(phone_raw, '[^0-9]', '', 'g')) = 10 THEN 
                        REGEXP_REPLACE(phone_raw, '[^0-9]', '', 'g')
                    WHEN LENGTH(REGEXP_REPLACE(phone_raw, '[^0-9]', '', 'g')) = 11 AND 
                         REGEXP_REPLACE(phone_raw, '[^0-9]', '', 'g') LIKE '1%' THEN 
                        SUBSTRING(REGEXP_REPLACE(phone_raw, '[^0-9]', '', 'g'), 2)
                    ELSE NULL
                END
            ELSE NULL
        END AS phone_clean,

        -- Mobile number cleaning
        CASE 
            WHEN mobile_raw <> '' THEN 
                CASE 
                    WHEN LENGTH(REGEXP_REPLACE(mobile_raw, '[^0-9]', '', 'g')) = 10 THEN 
                        REGEXP_REPLACE(mobile_raw, '[^0-9]', '', 'g')
                    WHEN LENGTH(REGEXP_REPLACE(mobile_raw, '[^0-9]', '', 'g')) = 11 AND 
                         REGEXP_REPLACE(mobile_raw, '[^0-9]', '', 'g') LIKE '1%' THEN 
                        SUBSTRING(REGEXP_REPLACE(mobile_raw, '[^0-9]', '', 'g'), 2)
                    ELSE NULL
                END
            ELSE NULL
        END AS mobile_clean,

        -- Full address concatenation
        CASE WHEN address1 <> '' OR city <> '' OR state <> '' THEN
                 CONCAT_WS(', ',
                     NULLIF(address1, ''),
                     NULLIF(address2, ''),
                     NULLIF(city, ''),
                     NULLIF(state, ''),
                     NULLIF(CONCAT_WS('-', NULLIF(zip_code, ''), NULLIF(zip_code_ext, '')), ''),
                     NULLIF(country, '')
                 )
        END AS full_address,

        -- Extract email domain for company analysis
        CASE 
            WHEN email_raw ~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$' THEN 
                LOWER(SPLIT_PART(email_raw, '@', 2))
            ELSE NULL
        END AS email_domain,

        -- Parse name into first/last (basic splitting)
        CASE 
            WHEN representative_name LIKE '% %' THEN TRIM(SPLIT_PART(representative_name, ' ', 1))
            WHEN representative_name <> '' THEN representative_name
            ELSE NULL
        END AS first_name,

        CASE 
            WHEN representative_name LIKE '% %' THEN 
                TRIM(SUBSTRING(representative_name FROM POSITION(' ' IN representative_name) + 1))
            ELSE NULL
        END AS last_name,

        -- Territory parsing (extract first territory if multiple)
        CASE 
            WHEN territories_json LIKE '%"%' THEN 
                TRIM(REGEXP_REPLACE(
                    SPLIT_PART(
                        REGEXP_REPLACE(territories_json, '[\[\]"]', '', 'g'), 
                        ',', 1
                    ), 
                    '^[[:space:]]*|[[:space:]]*$', '', 'g'
                ))
            WHEN territories_json <> '' THEN territories_json
            ELSE NULL
        END AS primary_territory,

        -- Count territories
        CASE 
            WHEN territories_json LIKE '%,%' THEN 
                ARRAY_LENGTH(STRING_TO_ARRAY(
                    REGEXP_REPLACE(territories_json, '[\[\]"]', '', 'g'), ','
                ), 1)
            WHEN territories_json <> '' AND territories_json <> '[]' THEN 1
            ELSE 0
        END AS territory_count

    FROM representative_cleaning
),

representative_business_logic AS (
    SELECT
        re.*,

        -- Company affiliation based on email domain
        CASE 
            WHEN re.email_domain = 'mammoth.org' THEN 'Mammoth Distribution'
            WHEN re.email_domain = '710labs.com' THEN '710 Labs'
            WHEN re.email_domain = 'purebeautypurebeauty.co' THEN 'Pure Beauty'
            WHEN re.email_domain = 'heavyhitters.co' THEN 'Heavy Hitters'
            WHEN re.email_domain = 'repsly.com' THEN 'Repsly (External)'
            WHEN re.email_domain LIKE '%test%' OR re.email_domain LIKE '%demo%' THEN 'Test Account'
            WHEN re.email_domain IS NOT NULL THEN 'External'
            ELSE 'Unknown'
        END AS company_affiliation,

        -- Representative type based on territories and company
        CASE 
            WHEN re.primary_territory LIKE '%710 Labs%' THEN '710 Labs Representative'
            WHEN re.primary_territory LIKE '%California%' AND re.email_domain = 'mammoth.org' THEN 'Mammoth CA Representative'
            WHEN re.primary_territory LIKE '%California%' THEN 'California Representative'
            WHEN re.primary_territory LIKE '%Colorado%' THEN 'Colorado Representative'
            WHEN re.primary_territory LIKE '%Michigan%' THEN 'Michigan Representative'
            WHEN re.primary_territory LIKE '%New York%' THEN 'New York Representative'
            WHEN re.territory_count > 1 THEN 'Multi-Territory Representative'
            WHEN re.territory_count = 1 THEN 'Single Territory Representative'
            ELSE 'No Territory Assignment'
        END AS representative_type,

        -- Activity status
        CASE 
            WHEN re.is_active = TRUE AND re.email_clean IS NOT NULL THEN 'Active'
            WHEN re.is_active = TRUE AND re.email_clean IS NULL THEN 'Active - No Email'
            WHEN re.is_active = FALSE THEN 'Inactive'
            ELSE 'Unknown Status'
        END AS representative_status,

        -- Contact quality assessment
        CASE 
            WHEN re.email_clean IS NOT NULL AND (re.phone_clean IS NOT NULL OR re.mobile_clean IS NOT NULL) THEN 'Excellent'
            WHEN re.email_clean IS NOT NULL THEN 'Good'
            WHEN re.phone_clean IS NOT NULL OR re.mobile_clean IS NOT NULL THEN 'Phone Only'
            ELSE 'Poor'
        END AS contact_quality,

        -- Address quality
        CASE 
            WHEN re.address1 <> '' AND re.city <> '' AND re.state <> '' AND re.zip_code <> '' THEN 'Complete'
            WHEN re.city <> '' AND re.state <> '' THEN 'Partial'
            WHEN re.state <> '' OR re.city <> '' THEN 'Minimal'
            ELSE 'Missing'
        END AS address_quality,

        -- Territory scope assessment
        CASE 
            WHEN re.territory_count >= 3 THEN 'Multi-Regional'
            WHEN re.territory_count = 2 THEN 'Dual Territory'
            WHEN re.territory_count = 1 THEN 'Single Territory'
            ELSE 'No Territory'
        END AS territory_scope,

        -- Representative tier (based on company and territory count)
        CASE 
            WHEN re.email_domain = 'mammoth.org' AND re.territory_count >= 1 THEN 'Internal - Primary'
            WHEN re.email_domain IN ('710labs.com', 'purebeautypurebeauty.co', 'heavyhitters.co') 
                 AND re.territory_count >= 1 THEN 'Partner - Primary'
            WHEN re.territory_count >= 1 THEN 'External - Active'
            WHEN re.is_active = TRUE THEN 'Active - Unassigned'
            ELSE 'Inactive/Unknown'
        END AS representative_tier,

        -- Geographic region (derived from primary territory)
        CASE 
            WHEN re.primary_territory LIKE '%CA%' OR re.primary_territory LIKE '%California%' THEN 'West Coast'
            WHEN re.primary_territory LIKE '%CO%' OR re.primary_territory LIKE '%Colorado%' THEN 'Mountain West'
            WHEN re.primary_territory LIKE '%MI%' OR re.primary_territory LIKE '%Michigan%' THEN 'Midwest'
            WHEN re.primary_territory LIKE '%NY%' OR re.primary_territory LIKE '%New York%' THEN 'Northeast'
            ELSE 'Other/Unknown'
        END AS geographic_region,

        -- Data completeness score (0-100)
        (
            CASE WHEN re.representative_code <> '' THEN 15 ELSE 0 END +
            CASE WHEN re.representative_name <> '' THEN 15 ELSE 0 END +
            CASE WHEN re.email_clean IS NOT NULL THEN 20 ELSE 0 END +
            CASE WHEN re.phone_clean IS NOT NULL OR re.mobile_clean IS NOT NULL THEN 15 ELSE 0 END +
            CASE WHEN re.territory_count > 0 THEN 15 ELSE 0 END +
            CASE WHEN re.city <> '' AND re.state <> '' THEN 10 ELSE 0 END +
            CASE WHEN re.is_active = TRUE THEN 10 ELSE 0 END
        ) AS data_completeness_score,

        -- Primary contact method
        CASE 
            WHEN re.email_clean IS NOT NULL THEN 'Email'
            WHEN re.mobile_clean IS NOT NULL THEN 'Mobile'
            WHEN re.phone_clean IS NOT NULL THEN 'Phone'
            ELSE 'No Contact Method'
        END AS primary_contact_method,

        -- Has any contact info flag
        (re.email_clean IS NOT NULL OR re.phone_clean IS NOT NULL OR re.mobile_clean IS NOT NULL) AS has_contact_info,

        -- Territory specialization
        CASE 
            WHEN re.primary_territory LIKE '%710 Labs%' THEN '710 Labs Specialist'
            WHEN re.email_domain = 'mammoth.org' THEN 'Mammoth Internal'
            WHEN re.territory_count > 2 THEN 'Multi-Regional Specialist'
            WHEN re.territory_count = 1 THEN 'Territory Specialist'
            ELSE 'General Representative'
        END AS specialization

    FROM representative_enhancement re
)

SELECT *
FROM representative_business_logic
ORDER BY representative_name, representative_code