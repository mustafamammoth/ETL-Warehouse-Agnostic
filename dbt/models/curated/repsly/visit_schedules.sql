-- Cleaned visit schedules data (simple structure)
-- dbt/models/curated/repsly/visit_schedules.sql

{{ config(materialized='table') }}

SELECT
    -- Representative information
    TRIM(representative_code) as representative_code,
    TRIM(representative_name) as representative_name,
    
    -- Client information
    TRIM(client_code) as client_code,
    TRIM(client_name) as client_name,
    
    -- Address fields
    TRIM(COALESCE(street_address, '')) as street_address,
    TRIM(COALESCE(zip_code, '')) as zip_code,
    TRIM(COALESCE(zip_ext, '')) as zip_ext,
    TRIM(COALESCE(city, '')) as city,
    TRIM(COALESCE(state, '')) as state,
    TRIM(COALESCE(country, '')) as country,
    
    -- Schedule details
    TRIM(COALESCE(visit_note, '')) as visit_note,
    
    -- Parse Microsoft JSON date: /Date(1750118400000+0000)/
    CASE WHEN schedule_date_and_time_raw LIKE '/Date(%' THEN
         TO_TIMESTAMP(SUBSTRING(schedule_date_and_time_raw, 7, 13)::BIGINT / 1000)
    END as scheduled_datetime,
    
    DATE(CASE WHEN schedule_date_and_time_raw LIKE '/Date(%' THEN
         TO_TIMESTAMP(SUBSTRING(schedule_date_and_time_raw, 7, 13)::BIGINT / 1000)
    END) as scheduled_date,
    
    -- Boolean flag
    CASE WHEN UPPER(TRIM(COALESCE(due_date_raw, 'FALSE'))) = 'TRUE' THEN TRUE ELSE FALSE END as is_due_date,
    
    -- Enhanced fields
    CASE WHEN street_address <> '' AND city <> '' AND state <> '' THEN
         CONCAT_WS(', ', street_address, city, state, zip_code, country)
    END as full_address,
    
    -- State standardization
    CASE
        WHEN country IN ('US', 'United States of America', 'United States') AND state = 'CA' THEN 'California'
        WHEN country IN ('US', 'United States of America', 'United States') AND state = 'NY' THEN 'New York'
        ELSE state
    END as state_standardized,
    
    -- Country standardization
    CASE
        WHEN country IN ('US', 'United States of America') THEN 'United States'
        ELSE country
    END as country_standardized,
    
    -- Parse client DBA information
    CASE WHEN client_name LIKE '%DBA:%' 
         THEN TRIM(SUBSTRING(client_name FROM POSITION('DBA:' IN client_name) + 4))
         ELSE client_name
    END as dba_name,
    
    CASE WHEN client_name LIKE '%DBA:%' 
         THEN TRIM(SUBSTRING(client_name FROM 1 FOR POSITION('DBA:' IN client_name) - 1))
         ELSE client_name
    END as legal_name,
    
    -- Date analysis
    EXTRACT(DOW FROM CASE WHEN schedule_date_and_time_raw LIKE '/Date(%' THEN
         TO_TIMESTAMP(SUBSTRING(schedule_date_and_time_raw, 7, 13)::BIGINT / 1000)
    END) as scheduled_day_of_week,
    
    EXTRACT(HOUR FROM CASE WHEN schedule_date_and_time_raw LIKE '/Date(%' THEN
         TO_TIMESTAMP(SUBSTRING(schedule_date_and_time_raw, 7, 13)::BIGINT / 1000)
    END) as scheduled_hour,
    
    -- Business categorizations
    CASE EXTRACT(DOW FROM CASE WHEN schedule_date_and_time_raw LIKE '/Date(%' THEN
         TO_TIMESTAMP(SUBSTRING(schedule_date_and_time_raw, 7, 13)::BIGINT / 1000)
    END)
        WHEN 0 THEN 'Sunday'
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
    END as day_of_week_name,
    
    CASE 
        WHEN EXTRACT(DOW FROM CASE WHEN schedule_date_and_time_raw LIKE '/Date(%' THEN
             TO_TIMESTAMP(SUBSTRING(schedule_date_and_time_raw, 7, 13)::BIGINT / 1000)
        END) IN (0, 6) THEN 'Weekend'
        ELSE 'Weekday'
    END as work_day_type,
    
    CASE
        WHEN EXTRACT(HOUR FROM CASE WHEN schedule_date_and_time_raw LIKE '/Date(%' THEN
             TO_TIMESTAMP(SUBSTRING(schedule_date_and_time_raw, 7, 13)::BIGINT / 1000)
        END) BETWEEN 6 AND 11 THEN 'Morning'
        WHEN EXTRACT(HOUR FROM CASE WHEN schedule_date_and_time_raw LIKE '/Date(%' THEN
             TO_TIMESTAMP(SUBSTRING(schedule_date_and_time_raw, 7, 13)::BIGINT / 1000)
        END) BETWEEN 12 AND 17 THEN 'Afternoon'
        WHEN EXTRACT(HOUR FROM CASE WHEN schedule_date_and_time_raw LIKE '/Date(%' THEN
             TO_TIMESTAMP(SUBSTRING(schedule_date_and_time_raw, 7, 13)::BIGINT / 1000)
        END) BETWEEN 18 AND 21 THEN 'Evening'
        ELSE 'Other'
    END as time_of_day,
    
    -- Geographic region
    CASE 
        WHEN state = 'CA' OR state = 'California' THEN 'California'
        WHEN state = 'NY' OR state = 'New York' THEN 'New York'
        ELSE 'Other States'
    END as geographic_region,
    
    -- Client business type
    CASE
        WHEN client_name LIKE '%DBA:%' THEN 'Corporate Client (DBA)'
        WHEN client_name LIKE '%LLC%' OR client_name LIKE '%Inc%' OR client_name LIKE '%Corp%' THEN 'Business Entity'
        ELSE 'Standard Business'
    END as client_business_type,
    
    -- Data quality flags
    (visit_note <> '') as has_visit_notes,
    (street_address <> '' AND city <> '' AND state <> '') as has_complete_address,
    (CASE WHEN schedule_date_and_time_raw LIKE '/Date(%' THEN
         TO_TIMESTAMP(SUBSTRING(schedule_date_and_time_raw, 7, 13)::BIGINT / 1000)
    END IS NOT NULL) as has_valid_datetime,
    
    -- Data completeness score (0-100)
    (
        CASE WHEN client_code <> '' THEN 25 ELSE 0 END +
        CASE WHEN representative_code <> '' THEN 25 ELSE 0 END +
        CASE WHEN (street_address <> '' AND city <> '' AND state <> '') = TRUE THEN 25 ELSE 0 END +
        CASE WHEN (CASE WHEN schedule_date_and_time_raw LIKE '/Date(%' THEN
             TO_TIMESTAMP(SUBSTRING(schedule_date_and_time_raw, 7, 13)::BIGINT / 1000)
        END IS NOT NULL) = TRUE THEN 25 ELSE 0 END
    ) as data_completeness_score,
    
    extracted_at,
    source_system

FROM {{ ref('visit_schedules_raw') }}
ORDER BY scheduled_datetime DESC, representative_code, client_code