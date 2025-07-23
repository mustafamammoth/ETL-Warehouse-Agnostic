-- models/raw/repsly/photos_raw.sql
-- Raw photos (bronze) – keep everything as text, incremental MergeTree

{{ config(
    materialized = 'incremental',
    schema       = var('bronze_schema', 'repsly_bronze'),
    unique_key   = 'photo_id',
    engine       = 'MergeTree()',
    order_by     = '(photo_id, extracted_at_nn)',
    partition_by = 'toYYYYMM(extracted_at_nn)',
    on_schema_change = 'sync_all_columns'
) }}

WITH src AS (
    SELECT
        -- original API fields as text
        toString(PhotoID)              AS photo_id,
        toString(ClientCode)           AS client_code,
        toString(ClientName)           AS client_name,
        toString(Note)                 AS note_raw,
        toString(DateAndTime)          AS date_and_time_raw,
        toString(PhotoURL)             AS photo_url,
        toString(RepresentativeCode)   AS representative_code,
        toString(RepresentativeName)   AS representative_name,
        toString(VisitID)              AS visit_id,
        toString(Tag)                  AS tag,

        -- system metadata (already proper types in bronze source)
        _extracted_at                  AS extracted_at,
        _source_system                 AS source_system,
        _endpoint                      AS endpoint
    FROM {{ source('repsly_raw', 'raw_photos') }}
    WHERE PhotoID IS NOT NULL
)

SELECT
    *,
    -- non-nullable version for engine keys
    CAST(ifNull(toDate(extracted_at), toDate('1970-01-01')) AS Date) AS extracted_at_nn
FROM src

{% if is_incremental() %}
WHERE extracted_at > (SELECT max(extracted_at) FROM {{ this }})
{% endif %}
