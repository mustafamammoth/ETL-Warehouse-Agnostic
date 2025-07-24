{{ config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='append_new_columns',
    meta={
        'description': 'Raw clients data from Repsly API - append-only, all fields as strings',
        'data_source': 'repsly_api',
        'update_frequency': 'incremental'
    }
) }}

-- Bronze layer: Pure append-only storage
-- All fields stored as strings to avoid type conflicts
SELECT 
    CAST(COALESCE("ClientID", '') AS String) AS client_id,
    CAST(COALESCE("Code", '') AS String) AS client_code,           -- Use actual API field
    CAST(COALESCE("Name", '') AS String) AS client_name,           -- Use actual API field  
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
    CAST(COALESCE("GPSLongitude", '') AS String) AS longitude_raw,
    CAST(COALESCE("GPSLatitude", '') AS String) AS latitude_raw,
    CAST(COALESCE("TimeStamp", '') AS String) AS timestamp_raw,
    CAST(COALESCE("Active", '') AS String) AS active_raw,
    CAST(COALESCE("RepresentativeCode", '') AS String) AS representative_code,
    CAST(COALESCE("RepresentativeName", '') AS String) AS representative_name,
    CAST(COALESCE("Tag", '') AS String) AS tags_raw,
    CAST(COALESCE("Note", '') AS String) AS notes_raw,
    CAST(COALESCE("Website", '') AS String) AS website,
    CAST(COALESCE("ContactName", '') AS String) AS contact_person,
    CAST(COALESCE("ContactTitle", '') AS String) AS contact_title,
    CAST(COALESCE("Status", '') AS String) AS client_status,
    CAST(COALESCE("PriceLists", '') AS String) AS price_lists,
    CAST(COALESCE("CustomFields", '') AS String) AS custom_fields_raw,
    CAST(COALESCE("AccountCode", '') AS String) AS account_code,

    -- System metadata
    CAST("_extracted_at" AS String) AS extracted_at,
    CAST("_source_system" AS String) AS source_system,
    CAST("_endpoint" AS String) AS endpoint,
    now() AS dbt_loaded_at,
    
    cityHash64(
        concat(
            COALESCE("ClientID", ''),
            COALESCE("Code", ''),        -- Use actual field name
            COALESCE("TimeStamp", ''),
            "_extracted_at"
        )
    ) AS record_hash

FROM {{ source('repsly_raw', 'raw_clients') }}

{% if is_incremental() %}
WHERE parseDateTimeBestEffort("_extracted_at") > 
    (SELECT COALESCE(max(parseDateTimeBestEffort(extracted_at)), toDateTime('1900-01-01'))
     FROM {{ this }})
{% endif %}