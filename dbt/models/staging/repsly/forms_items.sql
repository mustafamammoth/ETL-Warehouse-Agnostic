{{ config(
    materialized='table',
    schema=var('silver_schema', 'repsly_silver'),
    engine='MergeTree()',
    order_by='item_ordinal',
    partition_by='toYYYYMM(bronze_processed_at)',
    meta={
        'description': 'Exploded form items - one row per form field/value pair',
        'update_strategy': 'full_refresh'
    }
) }}

WITH json_exploded AS (
    SELECT 
        form_id,
        visit_id,
        bronze_processed_at,
        processed_at,
        source_system,
        endpoint,
        record_hash,
        
        -- Parse JSON array and extract Field/Value pairs
        arrayJoin(
            arrayMap(
                x -> (
                    JSONExtractString(x, 'Field'),
                    JSONExtractString(x, 'Value')
                ),
                JSONExtract(items_raw, 'Array(String)')
            )
        ) AS item_tuple
        
    FROM {{ ref('forms_staging') }}
    WHERE items_raw IS NOT NULL 
      AND items_raw != '' 
      AND isValidJSON(items_raw)
      AND form_id IS NOT NULL
),

numbered_items AS (
    SELECT 
        form_id,
        visit_id,
        bronze_processed_at,
        processed_at,
        source_system,
        endpoint,
        record_hash,
        item_tuple.1 as field,
        item_tuple.2 as value,
        row_number() OVER (PARTITION BY form_id ORDER BY item_tuple.1) as item_ordinal
    FROM json_exploded
)

SELECT 
    form_id,
    visit_id,
    item_ordinal,
    trimBoth(field) as field,
    trimBoth(value) as value,
    
    -- System metadata
    bronze_processed_at,
    processed_at,
    source_system,
    endpoint,
    record_hash
    
FROM numbered_items
WHERE field IS NOT NULL AND field != ''
  AND form_id IS NOT NULL