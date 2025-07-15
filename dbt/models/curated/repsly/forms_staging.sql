-- Forms staging: Clean scalars, keep Items as JSON
-- dbt/models/staging/repsly/forms_staging.sql

{{ config(materialized='table') }}

SELECT
    -- Core IDs
    form_id::BIGINT as form_id,
    visit_id::BIGINT as visit_id,
    
    -- Form details
    TRIM(form_name) as form_name,
    client_code,
    TRIM(client_name) as client_name,
    TRIM(COALESCE(representative_code, '')) as representative_code,
    TRIM(COALESCE(representative_name, '')) as representative_name,
    
    -- Location
    TRIM(COALESCE(street_address, '')) as street_address,
    TRIM(COALESCE(city, '')) as city,
    CASE WHEN state = 'CA' THEN 'California'
         WHEN state = 'NY' THEN 'New York' 
         ELSE state END as state,
    TRIM(COALESCE(country, '')) as country,
    
    -- Contact
    TRIM(COALESCE(email, '')) as email,
    TRIM(COALESCE(phone, '')) as phone,
    
    -- GPS coordinates
    CASE WHEN longitude_raw ~ '^-?[0-9]+\.?[0-9]*$' 
         THEN longitude_raw::DECIMAL(12,8) END as longitude,
    CASE WHEN latitude_raw ~ '^-?[0-9]+\.?[0-9]*$' 
         THEN latitude_raw::DECIMAL(12,8) END as latitude,
    
    -- Signature
    signature_url,
    
    -- Parse Microsoft JSON dates: /Date(1665749886000+0000)/ 
    CASE WHEN visit_start_raw LIKE '/Date(%' THEN
         TO_TIMESTAMP(SUBSTRING(visit_start_raw, 7, 13)::BIGINT / 1000)
    END as visit_start_ts,
    
    CASE WHEN visit_end_raw LIKE '/Date(%' THEN
         TO_TIMESTAMP(SUBSTRING(visit_end_raw, 7, 13)::BIGINT / 1000)
    END as visit_end_ts,
    
    CASE WHEN date_and_time_raw LIKE '/Date(%' THEN
         TO_TIMESTAMP(SUBSTRING(date_and_time_raw, 7, 13)::BIGINT / 1000)
    END as form_submitted_ts,
    
    -- Visit duration
    CASE WHEN visit_start_raw LIKE '/Date(%' AND visit_end_raw LIKE '/Date(%' THEN
         EXTRACT(EPOCH FROM (
             TO_TIMESTAMP(SUBSTRING(visit_end_raw, 7, 13)::BIGINT / 1000) - 
             TO_TIMESTAMP(SUBSTRING(visit_start_raw, 7, 13)::BIGINT / 1000)
         )) / 60
    END as minutes_on_site,
    
    -- Keep Items as raw JSON for explosion
    items_raw,
    
    -- Metadata
    extracted_at,
    source_system

FROM {{ ref('forms_raw') }}