-- Cleaned and standardized client data (PostgreSQL compatible)
{{ config(materialized='table') }}

WITH client_cleaning AS (
    SELECT 
        client_id,
        CAST(timestamp_raw AS BIGINT) as timestamp_value,
        code,
        
        -- Clean client name
        TRIM(name) AS name,
        
        -- Convert active flag to boolean
        CASE 
            WHEN UPPER(TRIM(active_raw)) = 'TRUE' THEN TRUE
            WHEN UPPER(TRIM(active_raw)) = 'FALSE' THEN FALSE
            ELSE NULL
        END AS is_active,
        
        -- Clean tags and territory
        TRIM(COALESCE(tag, '')) AS tags,
        TRIM(COALESCE(territory, '')) AS territory_raw,
        
        -- Representative information
        TRIM(COALESCE(representative_code, '')) AS representative_code,
        TRIM(COALESCE(representative_name, '')) AS representative_name,
        
        -- Address fields
        TRIM(COALESCE(street_address, '')) AS street_address,
        TRIM(COALESCE(zip_code, '')) AS zip_code,
        TRIM(COALESCE(zip_ext, '')) AS zip_ext,
        TRIM(COALESCE(city, '')) AS city,
        TRIM(COALESCE(state, '')) AS state,
        TRIM(COALESCE(country, '')) AS country,
        
        -- Contact information
        LOWER(TRIM(COALESCE(email, ''))) AS email_raw,
        TRIM(COALESCE(phone, '')) AS phone_raw,
        TRIM(COALESCE(mobile, '')) AS mobile_raw,
        TRIM(COALESCE(website, '')) AS website,
        TRIM(COALESCE(contact_name, '')) AS contact_name,
        TRIM(COALESCE(contact_title, '')) AS contact_title,
        
        -- Notes and status
        CASE 
            WHEN note IS NOT NULL AND note != '' THEN 
                REPLACE(REPLACE(TRIM(note), CHR(10), ' '), CHR(13), ' ')
            ELSE NULL
        END as notes_cleaned,
        
        TRIM(COALESCE(status, '')) AS status,
        TRIM(COALESCE(account_code, '')) AS account_code,
        
        -- GPS coordinates
        CASE 
            WHEN gps_longitude_raw IS NOT NULL AND gps_longitude_raw != '' AND gps_longitude_raw != 'NULL'
            THEN CAST(gps_longitude_raw AS DECIMAL(10,6))
            ELSE NULL
        END AS gps_longitude,
        
        CASE 
            WHEN gps_latitude_raw IS NOT NULL AND gps_latitude_raw != '' AND gps_latitude_raw != 'NULL'
            THEN CAST(gps_latitude_raw AS DECIMAL(10,6))
            ELSE NULL
        END AS gps_latitude,
        
        -- Custom fields
        TRIM(COALESCE(custom_coa_emails, '')) AS custom_coa_emails,
        TRIM(COALESCE(custom_dba, '')) AS custom_dba,
        TRIM(COALESCE(custom_delivery_instructions, '')) AS custom_delivery_instructions,
        
        -- JSON fields
        custom_fields_json,
        price_lists_json,
        
        -- Metadata
        _extracted_at,
        _source_system

    FROM {{ ref('clients_raw') }}
),

client_enhancement AS (
    SELECT
        *,
        
        -- Territory parsing (split by '>')
        CASE 
            WHEN territory_raw LIKE '%>%' THEN TRIM(SPLIT_PART(territory_raw, '>', 1))
            WHEN territory_raw != '' THEN territory_raw
            ELSE NULL
        END AS territory_level_1,
        
        CASE 
            WHEN territory_raw LIKE '%>%' THEN TRIM(SPLIT_PART(territory_raw, '>', 2))
            ELSE NULL
        END AS territory_level_2,
        
        -- Full address concatenation
        CASE 
            WHEN street_address != '' OR city != '' OR state != '' THEN
                CONCAT_WS(', ',
                    NULLIF(street_address, ''),
                    NULLIF(city, ''),
                    NULLIF(state, ''),
                    NULLIF(CONCAT_WS('-', NULLIF(zip_code, ''), NULLIF(zip_ext, '')), ''),
                    NULLIF(country, '')
                )
            ELSE NULL
        END AS full_address,
        
        -- Email cleaning and validation
        CASE 
            WHEN email_raw = '' THEN NULL
            WHEN email_raw LIKE '%@%.%' AND LENGTH(email_raw) > 5 THEN email_raw
            ELSE NULL
        END AS email_clean,
        
        -- Phone number cleaning (remove common formatting)
        CASE 
            WHEN phone_raw = '' THEN NULL
            ELSE REGEXP_REPLACE(phone_raw, '[^0-9+]', '', 'g')
        END AS phone_clean,
        
        CASE 
            WHEN mobile_raw = '' THEN NULL
            ELSE REGEXP_REPLACE(mobile_raw, '[^0-9+]', '', 'g')
        END AS mobile_clean,
        
        -- Address quality assessment (moved here so it can be referenced later)
        CASE 
            WHEN street_address != '' AND city != '' AND state != '' AND zip_code != '' THEN 'Complete'
            WHEN (street_address != '' OR city != '') AND (state != '' OR zip_code != '') THEN 'Partial'
            WHEN city != '' OR state != '' OR zip_code != '' THEN 'Minimal'
            ELSE 'Missing'
        END AS address_quality,
        
        -- Contact quality assessment (moved here so it can be referenced later)
        CASE 
            WHEN email_raw LIKE '%@%.%' AND phone_raw != '' AND contact_name != '' THEN 'Excellent'
            WHEN (email_raw LIKE '%@%.%' OR phone_raw != '') AND contact_name != '' THEN 'Good'
            WHEN email_raw LIKE '%@%.%' OR phone_raw != '' THEN 'Fair'
            ELSE 'Poor'
        END AS contact_quality

    FROM client_cleaning
),

client_business_logic AS (
    SELECT
        *,
        
        -- Client tier based on various factors
        CASE 
            WHEN representative_name != '' AND territory_level_1 != '' AND email_clean IS NOT NULL THEN 'Enterprise'
            WHEN representative_name != '' AND (territory_level_1 != '' OR email_clean IS NOT NULL) THEN 'Premium'
            WHEN representative_name != '' OR territory_level_1 != '' OR email_clean IS NOT NULL THEN 'Standard'
            WHEN name != '' AND (city != '' OR state != '') THEN 'Basic'
            ELSE 'Unknown'
        END AS client_tier,
        
        -- GPS availability
        CASE 
            WHEN gps_latitude IS NOT NULL AND gps_longitude IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS has_gps_coordinates,
        
        -- State/Province standardization for US
        CASE 
            WHEN country = 'United States' AND state = 'CA' THEN 'California'
            WHEN country = 'United States' AND state = 'California' THEN 'California'
            WHEN country = 'United States' AND state = 'TX' THEN 'Texas'
            WHEN country = 'United States' AND state = 'NY' THEN 'New York'
            WHEN country = 'United States' AND state = 'FL' THEN 'Florida'
            ELSE state
        END AS state_standardized,
        
        -- Business type inference from name
        CASE 
            WHEN LOWER(name) LIKE '%dispensary%' OR LOWER(name) LIKE '%cannabis%' THEN 'Dispensary'
            WHEN LOWER(name) LIKE '%delivery%' THEN 'Delivery Service'
            WHEN LOWER(name) LIKE '%collective%' OR LOWER(name) LIKE '%collective%' THEN 'Collective'
            WHEN LOWER(name) LIKE '%higher level%' THEN 'Chain Store'
            WHEN LOWER(name) LIKE '%deli%' THEN 'Retail Store'
            ELSE 'Other'
        END AS business_type_inferred,
        
        -- Data quality flag (now address_quality and contact_quality exist)
        CASE 
            WHEN address_quality = 'Complete' AND contact_quality IN ('Excellent', 'Good') AND is_active IS NOT NULL THEN 'High Quality'
            WHEN address_quality IN ('Complete', 'Partial') AND contact_quality IN ('Excellent', 'Good', 'Fair') THEN 'Good Quality'
            WHEN address_quality IN ('Partial', 'Minimal') OR contact_quality = 'Fair' THEN 'Needs Improvement'
            ELSE 'Poor Quality'
        END AS data_quality_flag,
        
        -- Representative workload (clients per rep)
        COUNT(*) OVER (PARTITION BY representative_code) AS clients_per_representative

    FROM client_enhancement
)

SELECT * 
FROM client_business_logic
ORDER BY client_tier DESC, state_standardized, city, name