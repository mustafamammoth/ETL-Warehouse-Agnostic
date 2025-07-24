{{ config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='append_new_columns',
    meta={
        'description': 'Raw client notes data from Repsly API - append-only, all fields as strings',
        'data_source': 'repsly_api',
        'update_frequency': 'incremental'
    }
) }}

-- Bronze layer: Pure append-only storage - NO FILTERING OR DEDUPLICATION
SELECT 
    -- Cast ALL API fields to String to prevent type conflicts
    CAST(COALESCE("ClientNoteID", '') AS String) AS client_note_id,
    CAST(COALESCE("TimeStamp", '') AS String) AS timestamp_raw,
    CAST(COALESCE("DateAndTime", '') AS String) AS date_and_time_raw,
    CAST(COALESCE("RepresentativeCode", '') AS String) AS representative_code,
    CAST(COALESCE("RepresentativeName", '') AS String) AS representative_name,
    CAST(COALESCE("ClientCode", '') AS String) AS client_code,
    CAST(COALESCE("ClientName", '') AS String) AS client_name,
    CAST(COALESCE("StreetAddress", '') AS String) AS street_address,
    CAST(COALESCE("ZIP", '') AS String) AS zip_code,
    CAST(COALESCE("ZIPExt", '') AS String) AS zip_ext,
    CAST(COALESCE("City", '') AS String) AS city,
    CAST(COALESCE("State", '') AS String) AS state,
    CAST(COALESCE("Country", '') AS String) AS country,
    CAST(COALESCE("Email", '') AS String) AS email,
    CAST(COALESCE("Phone", '') AS String) AS phone,
    CAST(COALESCE("Mobile", '') AS String) AS mobile,
    CAST(COALESCE("Territory", '') AS String) AS territory,
    CAST(COALESCE("Longitude", '') AS String) AS longitude_raw,
    CAST(COALESCE("Latitude", '') AS String) AS latitude_raw,
    CAST(COALESCE("Note", '') AS String) AS note_raw,
    CAST(COALESCE("VisitID", '') AS String) AS visit_id,

    -- System metadata
    CAST("_extracted_at" AS String) AS extracted_at,
    CAST("_source_system" AS String) AS source_system,
    CAST("_endpoint" AS String) AS endpoint,
    now() AS dbt_loaded_at,
    
    cityHash64(
        concat(
            COALESCE("ClientNoteID", ''),
            COALESCE("ClientCode", ''),
            COALESCE("DateAndTime", ''),
            left(COALESCE("Note", ''), 100),
            "_extracted_at"
        )
    ) AS record_hash

FROM {{ source('repsly_raw', 'raw_client_notes') }}

-- CRITICAL: Only prevent duplicate appends in incremental mode
{% if is_incremental() %}
WHERE parseDateTimeBestEffort("_extracted_at") > 
    (SELECT COALESCE(max(parseDateTimeBestEffort(extracted_at)), toDateTime('1900-01-01'))
     FROM {{ this }})
{% endif %}