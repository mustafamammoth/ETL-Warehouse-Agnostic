-- Cleaned photos data (direct from raw to business-ready)
-- dbt/models/curated/repsly/photos.sql

{{ config(materialized='table') }}

SELECT
    -- Core identifiers
    photo_id::BIGINT as photo_id,
    visit_id::BIGINT as visit_id,
    client_code,
    TRIM(client_name) as client_name,
    TRIM(COALESCE(representative_code, '')) as representative_code,
    TRIM(COALESCE(representative_name, '')) as representative_name,
    
    -- Photo details
    photo_url,
    TRIM(COALESCE(note, '')) as note,
    
    -- Parse Microsoft JSON date: /Date(1665059516000+0000)/
    CASE WHEN date_and_time_raw LIKE '/Date(%' THEN
         TO_TIMESTAMP(SUBSTRING(date_and_time_raw, 7, 13)::BIGINT / 1000)
    END as photo_timestamp,
    
    DATE(CASE WHEN date_and_time_raw LIKE '/Date(%' THEN
         TO_TIMESTAMP(SUBSTRING(date_and_time_raw, 7, 13)::BIGINT / 1000)
    END) as photo_date,
    
    -- Parse tags (comma-separated)
    TRIM(COALESCE(tag, '')) as tags_raw,
    
    -- Tag analysis
    CASE WHEN tag LIKE '%Competition%' THEN TRUE ELSE FALSE END as is_competition_photo,
    CASE WHEN tag LIKE '%Display%' THEN TRUE ELSE FALSE END as is_display_photo,
    CASE WHEN tag LIKE '%@marketing%' THEN TRUE ELSE FALSE END as is_marketing_photo,
    CASE WHEN tag LIKE '%Social Media Worthy%' THEN TRUE ELSE FALSE END as is_social_media_worthy,
    
    -- Photo categorization
    CASE 
        WHEN tag LIKE '%Competition%' AND tag LIKE '%Display%' THEN 'Competition & Display'
        WHEN tag LIKE '%Competition%' THEN 'Competition Analysis'
        WHEN tag LIKE '%Display%' THEN 'Display Documentation'
        WHEN tag LIKE '%@marketing%' THEN 'Marketing Content'
        WHEN tag LIKE '%Social Media Worthy%' THEN 'Social Media Content'
        WHEN tag IS NULL OR tag = '' THEN 'General Documentation'
        ELSE 'Other'
    END as photo_category,
    
    -- Business flags
    (note IS NOT NULL AND note != '') as has_note,
    (photo_url LIKE 'https://repsly.s3.amazonaws.com/%') as is_valid_url,
    
    -- Extract hour for timing analysis
    EXTRACT(HOUR FROM CASE WHEN date_and_time_raw LIKE '/Date(%' THEN
         TO_TIMESTAMP(SUBSTRING(date_and_time_raw, 7, 13)::BIGINT / 1000)
    END) as photo_hour,
    
    -- Time categorization
    CASE
        WHEN EXTRACT(HOUR FROM CASE WHEN date_and_time_raw LIKE '/Date(%' THEN
             TO_TIMESTAMP(SUBSTRING(date_and_time_raw, 7, 13)::BIGINT / 1000)
        END) BETWEEN 6 AND 11 THEN 'Morning'
        WHEN EXTRACT(HOUR FROM CASE WHEN date_and_time_raw LIKE '/Date(%' THEN
             TO_TIMESTAMP(SUBSTRING(date_and_time_raw, 7, 13)::BIGINT / 1000)
        END) BETWEEN 12 AND 17 THEN 'Afternoon'
        WHEN EXTRACT(HOUR FROM CASE WHEN date_and_time_raw LIKE '/Date(%' THEN
             TO_TIMESTAMP(SUBSTRING(date_and_time_raw, 7, 13)::BIGINT / 1000)
        END) BETWEEN 18 AND 21 THEN 'Evening'
        ELSE 'Other'
    END as time_of_day,
    
    -- Photo priority (for business use)
    CASE
        WHEN tag LIKE '%Social Media Worthy%' OR tag LIKE '%@marketing%' THEN 'High Priority'
        WHEN tag LIKE '%Competition%' THEN 'Medium Priority'
        ELSE 'Standard'
    END as business_priority,
    
    extracted_at,
    source_system

FROM {{ ref('photos_raw') }}
ORDER BY photo_timestamp DESC, photo_id