{{ config(materialized='table') }}

WITH user_cleaning AS (
    SELECT 
        user_id,
        code,
        
        -- Clean user information
        TRIM(COALESCE(name, '')) as name,
        LOWER(TRIM(COALESCE(email, ''))) as email_raw,
        TRIM(COALESCE(phone, '')) as phone_raw,
        
        -- Convert boolean fields
        CASE 
            WHEN UPPER(TRIM(COALESCE(active_raw, 'FALSE'))) = 'TRUE' THEN TRUE
            WHEN UPPER(TRIM(COALESCE(active_raw, 'FALSE'))) = 'FALSE' THEN FALSE
            ELSE NULL
        END AS is_active,
        
        CASE 
            WHEN UPPER(TRIM(COALESCE(send_email_enabled_raw, 'FALSE'))) = 'TRUE' THEN TRUE
            WHEN UPPER(TRIM(COALESCE(send_email_enabled_raw, 'FALSE'))) = 'FALSE' THEN FALSE
            ELSE NULL
        END AS send_email_enabled,
        
        CASE 
            WHEN UPPER(TRIM(COALESCE(show_activities_without_team_raw, 'FALSE'))) = 'TRUE' THEN TRUE
            WHEN UPPER(TRIM(COALESCE(show_activities_without_team_raw, 'FALSE'))) = 'FALSE' THEN FALSE
            ELSE NULL
        END AS show_activities_without_team,
        
        -- Clean role information
        TRIM(COALESCE(role_raw, '')) as role_raw_clean,
        
        -- Address information
        TRIM(COALESCE(address1, '')) as address1,
        TRIM(COALESCE(address2, '')) as address2,
        TRIM(COALESCE(city, '')) as city,
        TRIM(COALESCE(state, '')) as state,
        TRIM(COALESCE(zip_code, '')) as zip_code,
        TRIM(COALESCE(zip_code_ext, '')) as zip_code_ext,
        TRIM(COALESCE(country, '')) as country,
        TRIM(COALESCE(country_code, '')) as country_code,
        
        -- Clean notes
        CASE 
            WHEN note IS NOT NULL AND note != '' THEN 
                REPLACE(REPLACE(TRIM(note), CHR(10), ' '), CHR(13), ' ')
            ELSE NULL
        END as notes_cleaned,
        
        -- JSON fields (keep as-is for now)
        territories_json,
        attributes_json,
        permissions_json,
        teams_json,
        
        -- Metadata
        extracted_at,
        source_system,
        endpoint,
        testing_mode

    FROM {{ ref('users_raw') }}
),

user_enhancement AS (
    SELECT
        *,
        
        -- Parse roles (can be multiple, separated by |)
        CASE 
            WHEN role_raw_clean LIKE '%|%' THEN 
                ARRAY_TO_STRING(STRING_TO_ARRAY(role_raw_clean, '|'), ', ')
            ELSE role_raw_clean
        END AS roles_combined,
        
        -- Primary role (first in list)
        CASE 
            WHEN role_raw_clean LIKE '%|%' THEN 
                TRIM(SPLIT_PART(role_raw_clean, '|', 1))
            ELSE role_raw_clean
        END AS primary_role,
        
        -- Role flags
        CASE 
            WHEN LOWER(role_raw_clean) LIKE '%admin%' THEN TRUE
            ELSE FALSE
        END AS is_admin,
        
        CASE 
            WHEN LOWER(role_raw_clean) LIKE '%representative%' THEN TRUE
            ELSE FALSE
        END AS is_representative,
        
        -- Email validation
        CASE 
            WHEN email_raw = '' THEN NULL
            WHEN email_raw LIKE '%@%.%' AND LENGTH(email_raw) > 5 THEN email_raw
            ELSE NULL
        END AS email_clean,
        
        -- Phone cleaning
        CASE 
            WHEN phone_raw = '' THEN NULL
            ELSE REGEXP_REPLACE(phone_raw, '[^0-9+]', '', 'g')
        END AS phone_clean,
        
        -- Full address concatenation
        CASE 
            WHEN address1 != '' OR city != '' OR state != '' THEN
                CONCAT_WS(', ',
                    NULLIF(address1, ''),
                    NULLIF(address2, ''),
                    NULLIF(city, ''),
                    NULLIF(state, ''),
                    NULLIF(CONCAT_WS('-', NULLIF(zip_code, ''), NULLIF(zip_code_ext, '')), ''),
                    NULLIF(country, '')
                )
            ELSE NULL
        END AS full_address,
        
        -- Domain extraction from email
        CASE 
            WHEN email_raw LIKE '%@%.%' THEN 
                SUBSTRING(email_raw FROM '@(.*)$')
            ELSE NULL
        END AS email_domain,
        
        -- Name parsing
        CASE 
            WHEN name LIKE '% %' THEN TRIM(SPLIT_PART(name, ' ', 1))
            ELSE name
        END AS first_name,
        
        CASE 
            WHEN name LIKE '% %' THEN TRIM(SUBSTRING(name FROM POSITION(' ' IN name) + 1))
            ELSE NULL
        END AS last_name

    FROM user_cleaning
),

user_business_logic AS (
    SELECT
        *,
        
        -- User type classification
        CASE 
            WHEN is_admin = TRUE AND is_representative = TRUE THEN 'Admin Representative'
            WHEN is_admin = TRUE THEN 'Admin Only'
            WHEN is_representative = TRUE THEN 'Representative Only'
            ELSE 'Other'
        END AS user_type,
        
        -- Status classification
        CASE 
            WHEN is_active = TRUE AND email_clean IS NOT NULL THEN 'Active'
            WHEN is_active = FALSE AND email_clean IS NOT NULL THEN 'Inactive'
            WHEN is_active = TRUE AND email_clean IS NULL THEN 'Active - No Email'
            WHEN name = 'Inactive' OR LOWER(name) LIKE '%inactive%' THEN 'Deactivated Account'
            ELSE 'Unknown Status'
        END AS user_status,
        
        -- Address quality assessment
        CASE 
            WHEN address1 != '' AND city != '' AND state != '' AND zip_code != '' THEN 'Complete'
            WHEN (address1 != '' OR city != '') AND (state != '' OR zip_code != '') THEN 'Partial'
            WHEN city != '' OR state != '' OR zip_code != '' THEN 'Minimal'
            ELSE 'Missing'
        END AS address_quality,
        
        -- Contact quality assessment
        CASE 
            WHEN email_clean IS NOT NULL AND phone_clean IS NOT NULL THEN 'Excellent'
            WHEN email_clean IS NOT NULL OR phone_clean IS NOT NULL THEN 'Good'
            WHEN email_clean IS NOT NULL THEN 'Email Only'
            WHEN phone_clean IS NOT NULL THEN 'Phone Only'
            ELSE 'Poor'
        END AS contact_quality,
        
        -- Company affiliation (based on email domain)
        CASE 
            WHEN email_domain = 'mammoth.org' THEN 'Mammoth Distribution'
            WHEN email_domain = 'repsly.com' THEN 'Repsly (External)'
            WHEN email_domain = 'example.com' THEN 'Test Account'
            WHEN email_domain IS NOT NULL THEN 'External'
            ELSE 'Unknown'
        END AS company_affiliation,
        
        -- Account flags
        CASE 
            WHEN LOWER(name) LIKE '%test%' OR email_clean LIKE '%test%' THEN TRUE
            WHEN LOWER(name) = 'inactive' OR code LIKE '%435907%' THEN TRUE
            ELSE FALSE
        END AS is_test_account,
        
        -- Permission level (rough estimate based on JSON length)
        CASE 
            WHEN permissions_json IS NULL OR permissions_json = '' THEN 'No Permissions'
            WHEN LENGTH(permissions_json) > 500 THEN 'Full Permissions'
            WHEN LENGTH(permissions_json) > 100 THEN 'Limited Permissions'
            ELSE 'Basic Permissions'
        END AS permission_level,
        
        -- Territory scope
        CASE 
            WHEN territories_json LIKE '%"All"%' THEN 'All Territories'
            WHEN territories_json IS NOT NULL AND territories_json != '' THEN 'Specific Territories'
            ELSE 'No Territory Assignment'
        END AS territory_scope,
        
        -- Data completeness score (0-100)
        (
            CASE WHEN name != '' THEN 15 ELSE 0 END +
            CASE WHEN email_clean IS NOT NULL THEN 20 ELSE 0 END +
            CASE WHEN primary_role != '' THEN 15 ELSE 0 END +
            CASE WHEN phone_clean IS NOT NULL THEN 10 ELSE 0 END +
            CASE WHEN address_quality IN ('Complete', 'Partial') THEN 10 ELSE 0 END +
            CASE WHEN is_active IS NOT NULL THEN 10 ELSE 0 END +
            CASE WHEN territory_scope != 'No Territory Assignment' THEN 10 ELSE 0 END +
            CASE WHEN permission_level != 'No Permissions' THEN 10 ELSE 0 END
        ) AS data_completeness_score,
        
        -- Security flags
        CASE 
            WHEN is_admin = TRUE AND send_email_enabled = TRUE THEN 'High Access'
            WHEN is_admin = TRUE THEN 'Admin Access'
            WHEN is_representative = TRUE THEN 'Field Access'
            ELSE 'Limited Access'
        END AS access_level

    FROM user_enhancement
)

SELECT * 
FROM user_business_logic
ORDER BY company_affiliation, user_type, name