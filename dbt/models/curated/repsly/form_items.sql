-- Form items: Explode JSON into one row per field/value
-- dbt/models/staging/repsly/form_items.sql

{{ config(materialized='table') }}

WITH json_exploded AS (
    SELECT 
        form_id,
        visit_id,
        ROW_NUMBER() OVER (PARTITION BY form_id ORDER BY item_index) as item_ordinal,
        JSON_EXTRACT_PATH_TEXT(item_json, 'Field') as field,
        JSON_EXTRACT_PATH_TEXT(item_json, 'Value') as value
    FROM {{ ref('forms_staging') }},
    LATERAL JSON_ARRAY_ELEMENTS(items_raw::JSON) WITH ORDINALITY AS t(item_json, item_index)
    WHERE items_raw IS NOT NULL AND items_raw != ''
)

SELECT 
    form_id,
    visit_id,
    item_ordinal,
    TRIM(field) as field,
    TRIM(value) as value
FROM json_exploded
WHERE field IS NOT NULL AND field != ''