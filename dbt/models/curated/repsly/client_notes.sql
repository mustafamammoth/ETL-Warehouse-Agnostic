-- Cleaned and standardized client notes data
{{ config(materialized='table') }}

WITH note_cleaning AS (
    SELECT 
        client_note_id,
        CAST(timestamp_raw AS BIGINT) as timestamp_value,
        client_code,
        
        -- Clean representative information
        TRIM(COALESCE(representative_code, '')) as representative_code,
        TRIM(COALESCE(representative_name, '')) as representative_name,
        
        -- Clean client information
        TRIM(COALESCE(client_name, '')) as client_name,
        TRIM(COALESCE(street_address, '')) as street_address,
        TRIM(COALESCE(zip_code, '')) as zip_code,
        TRIM(COALESCE(zip_ext, '')) as zip_ext,
        TRIM(COALESCE(city, '')) as city,
        TRIM(COALESCE(state, '')) as state,
        TRIM(COALESCE(country, '')) as country,
        TRIM(COALESCE(email, '')) as email,
        TRIM(COALESCE(phone, '')) as phone,
        TRIM(COALESCE(mobile, '')) as mobile,
        TRIM(COALESCE(territory, '')) as territory,
        
        -- GPS coordinates
        CASE 
            WHEN longitude_raw IS NOT NULL AND longitude_raw != '' AND longitude_raw != 'NULL'
            THEN CAST(longitude_raw AS DECIMAL(10,6))
            ELSE NULL
        END AS longitude,
        
        CASE 
            WHEN latitude_raw IS NOT NULL AND latitude_raw != '' AND latitude_raw != 'NULL'
            THEN CAST(latitude_raw AS DECIMAL(10,6))
            ELSE NULL
        END AS latitude,
        
        -- Clean and process the note text
        CASE 
            WHEN note IS NOT NULL AND note != '' THEN 
                REPLACE(REPLACE(TRIM(note), CHR(10), ' '), CHR(13), ' ')
            ELSE NULL
        END as note_cleaned,
        
        -- Visit ID
        visit_id,
        
        -- Parse date from Microsoft JSON date format /Date(1665059530000+0000)/
        CASE 
            WHEN date_and_time_raw LIKE '/Date(%' THEN
                TO_TIMESTAMP(
                    CAST(
                        SUBSTRING(
                            date_and_time_raw, 
                            7, 
                            LENGTH(date_and_time_raw) - 13
                        ) AS BIGINT
                    ) / 1000
                )
            ELSE NULL
        END AS note_datetime,
        
        -- Extract just the date
        CASE 
            WHEN date_and_time_raw LIKE '/Date(%' THEN
                DATE(TO_TIMESTAMP(
                    CAST(
                        SUBSTRING(
                            date_and_time_raw, 
                            7, 
                            LENGTH(date_and_time_raw) - 13
                        ) AS BIGINT
                    ) / 1000
                ))
            ELSE NULL
        END AS note_date,
        
        -- Metadata
        extracted_at,
        source_system,
        endpoint,
        testing_mode

    FROM {{ ref('client_notes_raw') }}
),

note_enhancement AS (
    SELECT
        *,
        
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
        
        -- Extract year, month, day of week for analysis
        CASE 
            WHEN note_date IS NOT NULL 
            THEN EXTRACT(YEAR FROM note_date)
            ELSE NULL
        END AS note_year,
        
        CASE 
            WHEN note_date IS NOT NULL 
            THEN EXTRACT(MONTH FROM note_date)
            ELSE NULL
        END AS note_month,
        
        CASE 
            WHEN note_date IS NOT NULL 
            THEN EXTRACT(DOW FROM note_date)  -- 0=Sunday, 6=Saturday
            ELSE NULL
        END AS note_day_of_week,
        
        CASE 
            WHEN note_datetime IS NOT NULL 
            THEN EXTRACT(HOUR FROM note_datetime)
            ELSE NULL
        END AS note_hour,
        
        -- Note length and characteristics
        CASE 
            WHEN note_cleaned IS NOT NULL 
            THEN LENGTH(note_cleaned)
            ELSE 0
        END AS note_length,
        
        -- Territory parsing (split by '>')
        CASE 
            WHEN territory LIKE '%>%' THEN TRIM(SPLIT_PART(territory, '>', 1))
            WHEN territory != '' THEN territory
            ELSE NULL
        END AS territory_level_1,
        
        CASE 
            WHEN territory LIKE '%>%' THEN TRIM(SPLIT_PART(territory, '>', 2))
            ELSE NULL
        END AS territory_level_2,
        
        -- State standardization for US
        CASE 
            WHEN country = 'United States' AND state = 'CA' THEN 'California'
            WHEN country = 'United States' AND state = 'California' THEN 'California'
            WHEN country = 'United States' AND state = 'TX' THEN 'Texas'
            WHEN country = 'United States' AND state = 'NY' THEN 'New York'
            WHEN country = 'United States' AND state = 'FL' THEN 'Florida'
            WHEN country = 'United States' AND state = 'MA' THEN 'Massachusetts'
            ELSE state
        END AS state_standardized,
        
        -- Note categorization based on content (moved here so it can be referenced later)
        CASE 
            WHEN LOWER(note_cleaned) LIKE '%out of stock%' OR LOWER(note_cleaned) LIKE '%no stock%' THEN 'Inventory Issue'
            WHEN LOWER(note_cleaned) LIKE '%reorder%' OR LOWER(note_cleaned) LIKE '%restock%' OR LOWER(note_cleaned) LIKE '%order%' THEN 'Ordering'
            WHEN LOWER(note_cleaned) LIKE '%promo%' OR LOWER(note_cleaned) LIKE '%deal%' OR LOWER(note_cleaned) LIKE '%sale%' THEN 'Promotion'
            WHEN LOWER(note_cleaned) LIKE '%display%' OR LOWER(note_cleaned) LIKE '%demo%' THEN 'Display/Demo'
            WHEN LOWER(note_cleaned) LIKE '%manager%' OR LOWER(note_cleaned) LIKE '%owner%' OR LOWER(note_cleaned) LIKE '%staff%' THEN 'Staff Interaction'
            WHEN LOWER(note_cleaned) LIKE '%vacation%' OR LOWER(note_cleaned) LIKE '%closed%' OR LOWER(note_cleaned) LIKE '%busy%' THEN 'Availability'
            WHEN LOWER(note_cleaned) LIKE '%training%' OR LOWER(note_cleaned) LIKE '%education%' THEN 'Training'
            WHEN LOWER(note_cleaned) LIKE '%product%' OR LOWER(note_cleaned) LIKE '%cart%' OR LOWER(note_cleaned) LIKE '%gummies%' THEN 'Product Discussion'
            ELSE 'General'
        END AS note_category,
        
        -- Sentiment analysis (basic) (moved here so it can be referenced later)
        CASE 
            WHEN LOWER(note_cleaned) LIKE '%excited%' OR LOWER(note_cleaned) LIKE '%great%' OR LOWER(note_cleaned) LIKE '%helpful%' OR LOWER(note_cleaned) LIKE '%fast%' THEN 'Positive'
            WHEN LOWER(note_cleaned) LIKE '%problem%' OR LOWER(note_cleaned) LIKE '%issue%' OR LOWER(note_cleaned) LIKE '%concern%' THEN 'Negative'
            ELSE 'Neutral'
        END AS note_sentiment,
        
        -- Business priority (moved here so it can be referenced later)
        CASE 
            WHEN LOWER(note_cleaned) LIKE '%urgent%' OR LOWER(note_cleaned) LIKE '%asap%' OR LOWER(note_cleaned) LIKE '%immediately%' THEN 'High'
            WHEN LOWER(note_cleaned) LIKE '%follow up%' OR LOWER(note_cleaned) LIKE '%reorder%' OR LOWER(note_cleaned) LIKE '%need%' THEN 'Medium'
            ELSE 'Low'
        END AS business_priority,
        
        -- GPS availability (moved here so it can be referenced later)
        CASE 
            WHEN latitude IS NOT NULL AND longitude IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS has_gps_coordinates

    FROM note_cleaning
),

note_business_logic AS (
    SELECT
        *,
        
        -- Day of week name
        CASE note_day_of_week
            WHEN 0 THEN 'Sunday'
            WHEN 1 THEN 'Monday'
            WHEN 2 THEN 'Tuesday'
            WHEN 3 THEN 'Wednesday'
            WHEN 4 THEN 'Thursday'
            WHEN 5 THEN 'Friday'
            WHEN 6 THEN 'Saturday'
            ELSE 'Unknown'
        END AS day_of_week_name,
        
        -- Time of day categorization
        CASE 
            WHEN note_hour BETWEEN 6 AND 11 THEN 'Morning'
            WHEN note_hour BETWEEN 12 AND 17 THEN 'Afternoon'
            WHEN note_hour BETWEEN 18 AND 21 THEN 'Evening'
            ELSE 'Other'
        END AS time_of_day,
        
        -- Note quality assessment (now note_category exists)
        CASE 
            WHEN note_length >= 100 AND note_category != 'General' THEN 'Detailed'
            WHEN note_length >= 50 THEN 'Adequate'
            WHEN note_length >= 20 THEN 'Brief'
            WHEN note_length > 0 THEN 'Minimal'
            ELSE 'Empty'
        END AS note_quality,
        
        -- Data completeness score (0-100) (now has_gps_coordinates exists)
        (
            CASE WHEN client_code != '' THEN 10 ELSE 0 END +
            CASE WHEN representative_code != '' THEN 10 ELSE 0 END +
            CASE WHEN note_cleaned IS NOT NULL AND LENGTH(note_cleaned) > 10 THEN 20 ELSE 0 END +
            CASE WHEN city != '' THEN 10 ELSE 0 END +
            CASE WHEN state != '' THEN 10 ELSE 0 END +
            CASE WHEN has_gps_coordinates THEN 15 ELSE 0 END +
            CASE WHEN visit_id IS NOT NULL THEN 10 ELSE 0 END +
            CASE WHEN note_datetime IS NOT NULL THEN 15 ELSE 0 END
        ) AS data_completeness_score

    FROM note_enhancement
)

SELECT * 
FROM note_business_logic
ORDER BY note_datetime DESC, client_note_id