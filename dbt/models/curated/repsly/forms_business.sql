-- Forms business view: Key metrics derived from form items - ClickHouse compatible
{{ config(
    materialized='table',
    schema=var('gold_schema', 'repsly_gold'),
    engine='MergeTree()',
    order_by='(form_id, visit_date)',
    partition_by='toYYYYMM(visit_date)',
    meta={
        'description': 'Business metrics derived from form responses - adaptable to changing forms',
        'update_strategy': 'full_refresh'
    }
) }}

WITH form_flags AS (
    SELECT 
        form_id,
        
        -- Dynamic business flags (adapt these as forms change)
        -- Using pattern matching to be resilient to form changes
        max(CASE WHEN lower(field) LIKE '%follow%up%' AND lower(value) = 'yes' THEN 1 ELSE 0 END) as needs_follow_up,
        max(CASE WHEN lower(field) LIKE '%out%stock%' AND lower(value) = 'yes' THEN 1 ELSE 0 END) as out_of_stock,
        max(CASE WHEN lower(field) LIKE '%thcv%gumm%' AND lower(value) = 'yes' THEN 1 ELSE 0 END) as carries_thcv_gummies,
        max(CASE WHEN lower(field) LIKE '%retail%kit%' AND lower(value) = 'yes' THEN 1 ELSE 0 END) as has_retail_kit,
        max(CASE WHEN lower(field) LIKE '%education%' AND lower(value) = 'yes' THEN 1 ELSE 0 END) as doing_education,
        
        -- Presentation quality (flexible field name matching)
        any(CASE WHEN lower(field) LIKE '%presentation%' THEN value END) as presentation_quality,
        
        -- Who they spoke with (flexible field name matching)
        any(CASE WHEN lower(field) LIKE '%speak%with%' OR lower(field) LIKE '%spoke%with%' THEN value END) as spoke_with,
        
        -- Count photos taken (any field containing picture/photo with URL)
        countIf(lower(field) LIKE '%picture%' OR lower(field) LIKE '%photo%' AND value LIKE 'https://%') as photos_taken,
        
        -- Generic comment/description field
        any(CASE WHEN lower(field) LIKE '%describe%visit%' OR lower(field) LIKE '%comment%' THEN value END) as visit_description,
        
        -- Follow-up comments
        any(CASE WHEN lower(field) LIKE '%follow%comment%' OR lower(field) LIKE '%follow%needed%' THEN value END) as followup_comments
        
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
    toDate(f.visit_start_ts) as visit_date,
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
    fg.visit_description,
    fg.followup_comments,
    
    -- Derived categories
    CASE 
        WHEN lower(fg.presentation_quality) = 'good' THEN 'Good'
        WHEN lower(fg.presentation_quality) = 'better' THEN 'Better'
        WHEN lower(fg.presentation_quality) = 'average' THEN 'Average'
        ELSE 'Unknown'
    END as presentation_category,
    
    CASE 
        WHEN f.minutes_on_site < 10 THEN 'Quick Visit (<10 min)'
        WHEN f.minutes_on_site < 30 THEN 'Standard Visit (10-30 min)'
        WHEN f.minutes_on_site >= 30 THEN 'Long Visit (30+ min)'
        ELSE 'Unknown Duration'
    END as visit_duration_category,
    
    CASE 
        WHEN fg.needs_follow_up = 1 AND fg.out_of_stock = 1 THEN 'High Priority'
        WHEN fg.needs_follow_up = 1 OR fg.out_of_stock = 1 THEN 'Medium Priority'
        ELSE 'Standard Visit'
    END as priority_level,
    
    -- GPS quality
    CASE WHEN f.longitude IS NOT NULL AND f.latitude IS NOT NULL THEN 1 ELSE 0 END as has_gps_location,
    
    -- System metadata
    f.data_extracted_at,
    f.bronze_processed_at,
    f.processed_at

FROM {{ ref('forms_staging') }} f
LEFT JOIN form_flags fg ON f.form_id = fg.form_id