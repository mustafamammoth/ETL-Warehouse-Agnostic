-- Forms business view: Key metrics derived from form items
-- dbt/models/curated/repsly/forms_business.sql

{{ config(materialized='table') }}

WITH form_flags AS (
    SELECT 
        form_id,
        -- Business flags from form responses (use BOOL_OR instead of MAX)
        BOOL_OR(field = 'Any follow ups?' AND LOWER(value) = 'yes') as needs_follow_up,
        BOOL_OR(field = 'Are they out of stock on any items?' AND LOWER(value) = 'yes') as out_of_stock,
        BOOL_OR(field = 'Do they carry THCv Gummies?' AND LOWER(value) = 'yes') as carries_thcv_gummies,
        BOOL_OR(field = 'Do they have the retail kit?' AND LOWER(value) = 'yes') as has_retail_kit,
        BOOL_OR(field = 'Are you doing an Education?' AND LOWER(value) = 'yes') as doing_education,
        
        -- Presentation quality
        MAX(CASE WHEN field = 'How does our retail presentation look?' THEN value END) as presentation_quality,
        
        -- Who they spoke with
        MAX(CASE WHEN field = 'Who did you speak with?' THEN value END) as spoke_with,
        
        -- Count photos taken
        COUNT(CASE WHEN field LIKE '%picture%' AND value LIKE 'https://%' THEN 1 END) as photos_taken
        
    FROM {{ ref('form_items') }}
    GROUP BY form_id
)

SELECT 
    f.form_id,
    f.visit_id,
    f.form_name,
    f.client_code,
    f.client_name,
    f.representative_name,
    f.city,
    f.state,
    DATE(f.visit_start_ts) as visit_date,
    f.visit_start_ts,
    f.visit_end_ts,
    f.minutes_on_site,
    f.longitude,
    f.latitude,
    
    -- Business flags
    fg.needs_follow_up,
    fg.out_of_stock,
    fg.carries_thcv_gummies,
    fg.has_retail_kit,
    fg.doing_education,
    fg.presentation_quality,
    fg.spoke_with,
    fg.photos_taken,
    
    -- Derived categories
    CASE 
        WHEN fg.presentation_quality = 'Good' THEN 'Good'
        WHEN fg.presentation_quality = 'Better' THEN 'Better'
        WHEN fg.presentation_quality = 'Average' THEN 'Average'
        ELSE 'Unknown'
    END as presentation_category,
    
    CASE 
        WHEN f.minutes_on_site < 10 THEN 'Quick Visit (<10 min)'
        WHEN f.minutes_on_site < 30 THEN 'Standard Visit (10-30 min)'
        WHEN f.minutes_on_site >= 30 THEN 'Long Visit (30+ min)'
        ELSE 'Unknown Duration'
    END as visit_duration_category,
    
    CASE 
        WHEN fg.needs_follow_up AND fg.out_of_stock THEN 'High Priority'
        WHEN fg.needs_follow_up OR fg.out_of_stock THEN 'Medium Priority'
        ELSE 'Standard Visit'
    END as priority_level,
    
    -- GPS quality
    (f.longitude IS NOT NULL AND f.latitude IS NOT NULL) as has_gps_location,
    
    f.extracted_at

FROM {{ ref('forms_staging') }} f
LEFT JOIN form_flags fg ON f.form_id = fg.form_id