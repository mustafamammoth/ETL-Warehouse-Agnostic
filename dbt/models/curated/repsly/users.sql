{{ config(materialized='table') }}

-- ============================================================
-- 1️⃣  Raw → Clean
-- ------------------------------------------------------------
WITH user_cleaning AS (

    SELECT 
        user_id,
        code,
        
        -- Clean user information
        TRIM(COALESCE(name, ''))                              AS name,
        LOWER(TRIM(COALESCE(email, '')))                      AS email_raw,
        TRIM(COALESCE(phone, ''))                             AS phone_raw,
        
        -- Convert boolean fields
        CASE 
            WHEN UPPER(TRIM(COALESCE(active_raw, 'FALSE')))              = 'TRUE'  THEN TRUE
            WHEN UPPER(TRIM(COALESCE(active_raw, 'FALSE')))              = 'FALSE' THEN FALSE
            ELSE NULL
        END                                                  AS is_active,
        
        CASE 
            WHEN UPPER(TRIM(COALESCE(send_email_enabled_raw, 'FALSE')))   = 'TRUE'  THEN TRUE
            WHEN UPPER(TRIM(COALESCE(send_email_enabled_raw, 'FALSE')))   = 'FALSE' THEN FALSE
            ELSE NULL
        END                                                  AS send_email_enabled,
        
        CASE 
            WHEN UPPER(TRIM(COALESCE(show_activities_without_team_raw,
                                     'FALSE')))                          = 'TRUE'  THEN TRUE
            WHEN UPPER(TRIM(COALESCE(show_activities_without_team_raw,
                                     'FALSE')))                          = 'FALSE' THEN FALSE
            ELSE NULL
        END                                                  AS show_activities_without_team,
        
        -- Clean role information
        TRIM(COALESCE(role_raw, ''))                         AS role_raw_clean,
        
        -- Address information
        TRIM(COALESCE(address1,       ''))                   AS address1,
        TRIM(COALESCE(address2,       ''))                   AS address2,
        TRIM(COALESCE(city,           ''))                   AS city,
        TRIM(COALESCE(state,          ''))                   AS state,
        TRIM(COALESCE(zip_code,       ''))                   AS zip_code,
        TRIM(COALESCE(zip_code_ext,   ''))                   AS zip_code_ext,
        TRIM(COALESCE(country,        ''))                   AS country,
        TRIM(COALESCE(country_code,   ''))                   AS country_code,
        
        -- Clean notes
        CASE 
            WHEN note IS NOT NULL
             AND note <> '' THEN REPLACE(REPLACE(TRIM(note), CHR(10), ' '), CHR(13), ' ')
            ELSE NULL
        END                                                  AS notes_cleaned,
        
        -- JSON fields (kept as-is)
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

-- ============================================================
-- 2️⃣  Clean → Enhanced (parsing roles, email, phone, etc.)
-- ------------------------------------------------------------
user_enhancement AS (

    SELECT
        uc.*,
        
        -- Parse roles (can be multiple, separated by |)
        CASE 
            WHEN role_raw_clean LIKE '%|%' 
                 THEN ARRAY_TO_STRING(STRING_TO_ARRAY(role_raw_clean, '|'), ', ')
            ELSE role_raw_clean
        END                                                  AS roles_combined,
        
        -- Primary role (first in list)
        CASE 
            WHEN role_raw_clean LIKE '%|%' 
                 THEN TRIM(SPLIT_PART(role_raw_clean, '|', 1))
            ELSE role_raw_clean
        END                                                  AS primary_role,
        
        -- Role flags
        CASE WHEN LOWER(role_raw_clean) LIKE '%admin%'         THEN TRUE ELSE FALSE END AS is_admin,
        CASE WHEN LOWER(role_raw_clean) LIKE '%representative%' THEN TRUE ELSE FALSE END AS is_representative,
        
        -- Email validation
        CASE 
            WHEN email_raw = ''                                          THEN NULL
            WHEN email_raw LIKE '%@%.%' AND LENGTH(email_raw) > 5        THEN email_raw
            ELSE NULL
        END                                                  AS email_clean,
        
        -- Phone cleaning
        CASE 
            WHEN phone_raw = ''                               THEN NULL
            ELSE REGEXP_REPLACE(phone_raw, '[^0-9+]', '', 'g')
        END                                                  AS phone_clean,
        
        -- Full address concatenation
        CASE 
            WHEN address1 <> '' OR city <> '' OR state <> '' THEN
                CONCAT_WS(
                    ', ',
                    NULLIF(address1, ''),
                    NULLIF(address2, ''),
                    NULLIF(city,     ''),
                    NULLIF(state,    ''),
                    NULLIF(CONCAT_WS('-', NULLIF(zip_code, ''),
                                             NULLIF(zip_code_ext, '')), ''),
                    NULLIF(country,  '')
                )
            ELSE NULL
        END                                                  AS full_address,
        
        -- Domain extraction from email
        CASE 
            WHEN email_raw LIKE '%@%.%' 
                 THEN SUBSTRING(email_raw FROM '@(.*)$')
            ELSE NULL
        END                                                  AS email_domain,
        
        -- Name parsing
        CASE 
            WHEN name LIKE '% %' THEN TRIM(SPLIT_PART(name, ' ', 1))
            ELSE name
        END                                                  AS first_name,
        
        CASE 
            WHEN name LIKE '% %' 
                 THEN TRIM(SUBSTRING(name FROM POSITION(' ' IN name) + 1))
            ELSE NULL
        END                                                  AS last_name

    FROM user_cleaning uc
),

-- ============================================================
-- 3️⃣  Enhanced → Business-logic (step A)
--     Everything *except* the score, so address_quality
--     can be referenced later.
-- ------------------------------------------------------------
user_business_logic_base AS (

    SELECT
        ue.*,
        
        -- User type classification
        CASE 
            WHEN ue.is_admin AND ue.is_representative THEN 'Admin Representative'
            WHEN ue.is_admin                         THEN 'Admin Only'
            WHEN ue.is_representative                THEN 'Representative Only'
            ELSE 'Other'
        END                                                  AS user_type,
        
        -- Status classification
        CASE 
            WHEN ue.is_active AND ue.email_clean IS NOT NULL THEN 'Active'
            WHEN NOT ue.is_active AND ue.email_clean IS NOT NULL
                                                           THEN 'Inactive'
            WHEN ue.is_active AND ue.email_clean IS NULL    THEN 'Active - No Email'
            WHEN ue.name = 'Inactive'
              OR LOWER(ue.name) LIKE '%inactive%'           THEN 'Deactivated Account'
            ELSE 'Unknown Status'
        END                                                  AS user_status,
        
        -- Address quality assessment  ⬅️ *needed for score later*
        CASE 
            WHEN ue.address1 <> '' AND ue.city  <> '' 
             AND ue.state   <> '' AND ue.zip_code <> ''      THEN 'Complete'
            WHEN (ue.address1 <> '' OR ue.city <> '')
             AND (ue.state   <> '' OR ue.zip_code <> '')     THEN 'Partial'
            WHEN ue.city <> '' OR ue.state <> '' 
             OR ue.zip_code <> ''                            THEN 'Minimal'
            ELSE 'Missing'
        END                                                  AS address_quality,
        
        -- Contact quality assessment
        CASE 
            WHEN ue.email_clean IS NOT NULL
             AND ue.phone_clean IS NOT NULL                  THEN 'Excellent'
            WHEN ue.email_clean IS NOT NULL
              OR ue.phone_clean IS NOT NULL                  THEN 'Good'
            WHEN ue.email_clean IS NOT NULL                  THEN 'Email Only'
            WHEN ue.phone_clean IS NOT NULL                  THEN 'Phone Only'
            ELSE 'Poor'
        END                                                  AS contact_quality,
        
        -- Company affiliation (based on email domain)
        CASE 
            WHEN ue.email_domain = 'mammoth.org'             THEN 'Mammoth Distribution'
            WHEN ue.email_domain = 'repsly.com'              THEN 'Repsly (External)'
            WHEN ue.email_domain = 'example.com'             THEN 'Test Account'
            WHEN ue.email_domain IS NOT NULL                 THEN 'External'
            ELSE 'Unknown'
        END                                                  AS company_affiliation,
        
        -- Account flags
        CASE 
            WHEN LOWER(ue.name) LIKE '%test%' 
              OR ue.email_clean LIKE '%test%'                THEN TRUE
            WHEN LOWER(ue.name) = 'inactive' 
              OR ue.code LIKE '%435907%'                     THEN TRUE
            ELSE FALSE
        END                                                  AS is_test_account,
        
        -- Permission level (rough estimate based on JSON length)
        CASE 
            WHEN ue.permissions_json IS NULL 
              OR ue.permissions_json = ''                    THEN 'No Permissions'
            WHEN LENGTH(ue.permissions_json) > 500           THEN 'Full Permissions'
            WHEN LENGTH(ue.permissions_json) > 100           THEN 'Limited Permissions'
            ELSE 'Basic Permissions'
        END                                                  AS permission_level,
        
        -- Territory scope
        CASE 
            WHEN ue.territories_json LIKE '%"All"%'          THEN 'All Territories'
            WHEN ue.territories_json IS NOT NULL 
              AND ue.territories_json <> ''                  THEN 'Specific Territories'
            ELSE 'No Territory Assignment'
        END                                                  AS territory_scope,
        
        -- Security flags
        CASE 
            WHEN ue.is_admin AND ue.send_email_enabled       THEN 'High Access'
            WHEN ue.is_admin                                 THEN 'Admin Access'
            WHEN ue.is_representative                        THEN 'Field Access'
            ELSE 'Limited Access'
        END                                                  AS access_level

    FROM user_enhancement ue
),

-- ============================================================
-- 4️⃣  Business-logic (step B)
--     Compute the score after address_quality exists.
-- ------------------------------------------------------------
user_business_logic AS (

    SELECT
        b.*,
        
        -- Data completeness score (0-100)
        (   CASE WHEN b.name <> ''                                 THEN 15 ELSE 0 END +
            CASE WHEN b.email_clean IS NOT NULL                     THEN 20 ELSE 0 END +
            CASE WHEN b.primary_role <> ''                          THEN 15 ELSE 0 END +
            CASE WHEN b.phone_clean IS NOT NULL                     THEN 10 ELSE 0 END +
            CASE WHEN b.address_quality IN ('Complete','Partial')   THEN 10 ELSE 0 END +
            CASE WHEN b.is_active IS NOT NULL                       THEN 10 ELSE 0 END +
            CASE WHEN b.territory_scope <> 'No Territory Assignment'THEN 10 ELSE 0 END +
            CASE WHEN b.permission_level <> 'No Permissions'        THEN 10 ELSE 0 END
        )                                                          AS data_completeness_score
        
    FROM user_business_logic_base b
)

-- ============================================================
-- 5️⃣  Final result
-- ------------------------------------------------------------
SELECT *
FROM   user_business_logic
ORDER  BY company_affiliation, user_type, name
