{{ config(
    materialized='table',
    schema=var('silver_schema', 'repsly_silver'),
    engine='MergeTree()',
    order_by='(user_id, processed_at)',
    partition_by='toYYYYMM(processed_at)',
    meta={
        'description': 'Cleaned & enhanced users data from Repsly',
        'business_key': 'user_id',
        'update_strategy': 'latest_wins'
    }
) }}

-- Silver layer: Cleaned, typed, and deduplicated data
WITH latest_records AS (
    SELECT *,
        row_number() OVER (
            PARTITION BY user_id
            ORDER BY 
                dbt_loaded_at DESC,
                record_hash DESC
        ) AS row_num
    FROM {{ ref('users_raw') }}
    WHERE user_id != '' AND user_id != 'NULL'
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

user_cleaning AS (
    SELECT 
        user_id,
        code,
        
        -- Clean user information
        trimBoth(COALESCE(name, '')) AS name,
        lower(trimBoth(COALESCE(email, ''))) AS email_raw,
        trimBoth(COALESCE(phone, '')) AS phone_raw,
        
        -- Convert boolean fields
        multiIf(
            lower(active_raw) IN ('true','1','t','yes'), 1,
            lower(active_raw) IN ('false','0','f','no'), 0,
            NULL
        ) AS is_active,
        
        multiIf(
            lower(send_email_enabled_raw) IN ('true','1','t','yes'), 1,
            lower(send_email_enabled_raw) IN ('false','0','f','no'), 0,
            NULL
        ) AS send_email_enabled,
        
        multiIf(
            lower(show_activities_without_team_raw) IN ('true','1','t','yes'), 1,
            lower(show_activities_without_team_raw) IN ('false','0','f','no'), 0,
            NULL
        ) AS show_activities_without_team,
        
        -- Clean role information
        trimBoth(COALESCE(role_raw, '')) AS role_raw_clean,
        
        -- Address information
        trimBoth(COALESCE(address1, '')) AS address1,
        trimBoth(COALESCE(address2, '')) AS address2,
        trimBoth(COALESCE(city, '')) AS city,
        trimBoth(COALESCE(state, '')) AS state,
        trimBoth(COALESCE(zip_code, '')) AS zip_code,
        trimBoth(COALESCE(zip_code_ext, '')) AS zip_code_ext,
        trimBoth(COALESCE(country, '')) AS country,
        trimBoth(COALESCE(country_code, '')) AS country_code,
        
        -- Clean notes
        CASE 
            WHEN note IS NOT NULL AND note != '' 
            THEN replaceRegexpAll(replaceRegexpAll(trimBoth(note), '\n', ' '), '\r', ' ')
            ELSE NULL
        END AS notes_cleaned,
        
        -- JSON fields (kept as-is)
        territories_json,
        attributes_json,
        permissions_json,
        teams_json,
        
        -- Timestamps
        extraction_datetime,
        bronze_loaded_datetime,
        processed_at,
        
        -- Metadata
        source_system,
        endpoint,
        record_hash

    FROM with_timestamps
),

user_enhancement AS (
    SELECT
        uc.*,
        
        -- Parse roles (can be multiple, separated by |)
        CASE 
            WHEN position(role_raw_clean, '|') > 0
            THEN arrayStringConcat(splitByString('|', role_raw_clean), ', ')
            ELSE role_raw_clean
        END AS roles_combined,
        
        -- Primary role (first in list)
        CASE 
            WHEN position(role_raw_clean, '|') > 0
            THEN trimBoth(splitByString('|', role_raw_clean)[1])
            ELSE role_raw_clean
        END AS primary_role,
        
        -- Role flags
        CASE WHEN lower(role_raw_clean) LIKE '%admin%' THEN 1 ELSE 0 END AS is_admin,
        CASE WHEN lower(role_raw_clean) LIKE '%representative%' THEN 1 ELSE 0 END AS is_representative,
        
        -- Email validation
        CASE 
            WHEN email_raw = '' THEN NULL
            WHEN email_raw LIKE '%@%.%' AND length(email_raw) > 5 THEN email_raw
            ELSE NULL
        END AS email_clean,
        
        -- Phone cleaning
        CASE 
            WHEN phone_raw = '' THEN NULL
            ELSE replaceRegexpAll(phone_raw, '[^0-9+]', '')
        END AS phone_clean,
        
        -- Full address concatenation
        CASE 
            WHEN address1 != '' OR city != '' OR state != '' THEN
                arrayStringConcat(
                    arrayFilter(x -> x != '', [
                        COALESCE(NULLIF(address1, ''), ''),
                        COALESCE(NULLIF(address2, ''), ''),
                        COALESCE(NULLIF(city, ''), ''),
                        COALESCE(NULLIF(state, ''), ''),
                        COALESCE(
                            CASE 
                                WHEN zip_code != '' AND zip_code_ext != ''
                                THEN concat(zip_code, '-', zip_code_ext)
                                WHEN zip_code != ''
                                THEN zip_code
                                ELSE ''
                            END, 
                            ''
                        ),
                        COALESCE(NULLIF(country, ''), '')
                    ]),
                    ', '
                )
            ELSE NULL
        END AS full_address,
        
        -- Domain extraction from email
        CASE 
            WHEN email_raw LIKE '%@%.%' 
            THEN substring(email_raw, position(email_raw, '@') + 1)
            ELSE NULL
        END AS email_domain,
        
        -- Name parsing
        CASE 
            WHEN position(name, ' ') > 0 
            THEN trimBoth(substring(name, 1, position(name, ' ') - 1))
            ELSE name
        END AS first_name,
        
        CASE 
            WHEN position(name, ' ') > 0 
            THEN trimBoth(substring(name, position(name, ' ') + 1))
            ELSE NULL
        END AS last_name

    FROM user_cleaning uc
),

user_business_logic_base AS (
    SELECT
        ue.*,
        
        -- User type classification
        multiIf(
            ue.is_admin = 1 AND ue.is_representative = 1, 'Admin Representative',
            ue.is_admin = 1, 'Admin Only',
            ue.is_representative = 1, 'Representative Only',
            'Other'
        ) AS user_type,
        
        -- Status classification
        multiIf(
            ue.is_active = 1 AND ue.email_clean IS NOT NULL, 'Active',
            ue.is_active = 0 AND ue.email_clean IS NOT NULL, 'Inactive',
            ue.is_active = 1 AND ue.email_clean IS NULL, 'Active - No Email',
            ue.name = 'Inactive' OR lower(ue.name) LIKE '%inactive%', 'Deactivated Account',
            'Unknown Status'
        ) AS user_status,
        
        -- Address quality assessment
        multiIf(
            ue.address1 != '' AND ue.city != '' AND ue.state != '' AND ue.zip_code != '', 'Complete',
            (ue.address1 != '' OR ue.city != '') AND (ue.state != '' OR ue.zip_code != ''), 'Partial',
            ue.city != '' OR ue.state != '' OR ue.zip_code != '', 'Minimal',
            'Missing'
        ) AS address_quality,
        
        -- Contact quality assessment
        multiIf(
            ue.email_clean IS NOT NULL AND ue.phone_clean IS NOT NULL, 'Excellent',
            ue.email_clean IS NOT NULL OR ue.phone_clean IS NOT NULL, 'Good',
            ue.email_clean IS NOT NULL, 'Email Only',
            ue.phone_clean IS NOT NULL, 'Phone Only',
            'Poor'
        ) AS contact_quality,
        
        -- Company affiliation (based on email domain)  
        multiIf(
            ue.email_domain = 'mammoth.org', 'Mammoth Distribution',
            ue.email_domain = 'repsly.com', 'Repsly (External)',
            ue.email_domain = 'example.com', 'Test Account',
            ue.email_domain IS NOT NULL, 'External',
            'Unknown'
        ) AS company_affiliation,
        
        -- Account flags
        CASE 
            WHEN lower(ue.name) LIKE '%test%' OR ue.email_clean LIKE '%test%' THEN 1
            WHEN lower(ue.name) = 'inactive' OR ue.code LIKE '%435907%' THEN 1
            ELSE 0
        END AS is_test_account,
        
        -- Permission level (rough estimate based on JSON length)
        multiIf(
            ue.permissions_json IS NULL OR ue.permissions_json = '', 'No Permissions',
            length(ue.permissions_json) > 500, 'Full Permissions',
            length(ue.permissions_json) > 100, 'Limited Permissions',
            'Basic Permissions'
        ) AS permission_level,
        
        -- Territory scope
        multiIf(
            ue.territories_json LIKE '%"All"%', 'All Territories',
            ue.territories_json IS NOT NULL AND ue.territories_json != '', 'Specific Territories',
            'No Territory Assignment'
        ) AS territory_scope,
        
        -- Security flags
        multiIf(
            ue.is_admin = 1 AND ue.send_email_enabled = 1, 'High Access',
            ue.is_admin = 1, 'Admin Access',
            ue.is_representative = 1, 'Field Access',
            'Limited Access'
        ) AS access_level

    FROM user_enhancement ue
),

user_business_logic AS (
    SELECT
        b.*,
        
        -- Data completeness score (0-100)
        (
            CASE WHEN b.name != '' THEN 15 ELSE 0 END +
            CASE WHEN b.email_clean IS NOT NULL THEN 20 ELSE 0 END +
            CASE WHEN b.primary_role != '' THEN 15 ELSE 0 END +
            CASE WHEN b.phone_clean IS NOT NULL THEN 10 ELSE 0 END +
            CASE WHEN b.address_quality IN ('Complete','Partial') THEN 10 ELSE 0 END +
            CASE WHEN b.is_active IS NOT NULL THEN 10 ELSE 0 END +
            CASE WHEN b.territory_scope != 'No Territory Assignment' THEN 10 ELSE 0 END +
            CASE WHEN b.permission_level != 'No Permissions' THEN 10 ELSE 0 END
        ) AS data_completeness_score
        
    FROM user_business_logic_base b
)

SELECT *
FROM user_business_logic
ORDER BY company_affiliation, user_type, name