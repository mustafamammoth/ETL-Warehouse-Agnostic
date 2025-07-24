{{ config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='append_new_columns',
    meta={
        'description': 'Raw users data from Repsly API - append-only, all fields as strings',
        'data_source': 'repsly_api',
        'update_frequency': 'incremental'
    }
) }}

-- Bronze layer: Pure append-only storage
-- All fields stored as strings to avoid type conflicts
-- No filtering - store data exactly as received from API
SELECT 
    -- Cast ALL API fields to String to prevent type conflicts
    CAST(COALESCE("ID", '') AS String) AS user_id,
    CAST(COALESCE("Code", '') AS String) AS code,
    CAST(COALESCE("Name", '') AS String) AS name,
    CAST(COALESCE("Email", '') AS String) AS email,
    CAST(COALESCE("Active", '') AS String) AS active_raw,
    CAST(COALESCE("Role", '') AS String) AS role_raw,
    CAST(COALESCE("Note", '') AS String) AS note,
    CAST(COALESCE("Phone", '') AS String) AS phone,
    CAST(COALESCE("Territories", '') AS String) AS territories_json,
    CAST(COALESCE("SendEmailEnabled", '') AS String) AS send_email_enabled_raw,
    CAST(COALESCE("Address1", '') AS String) AS address1,
    CAST(COALESCE("Address2", '') AS String) AS address2,
    CAST(COALESCE("City", '') AS String) AS city,
    CAST(COALESCE("State", '') AS String) AS state,
    CAST(COALESCE("ZipCode", '') AS String) AS zip_code,
    CAST(COALESCE("ZipCodeExt", '') AS String) AS zip_code_ext,
    CAST(COALESCE("Country", '') AS String) AS country,
    CAST(COALESCE("CountryCode", '') AS String) AS country_code,
    CAST(COALESCE("Attributes", '') AS String) AS attributes_json,
    CAST(COALESCE("Permissions", '') AS String) AS permissions_json,
    CAST(COALESCE("Teams", '') AS String) AS teams_json,
    CAST(COALESCE("ShowActivitiesWithoutTeam", '') AS String) AS show_activities_without_team_raw,

    -- System metadata
    CAST("_extracted_at" AS String) AS extracted_at,
    CAST("_source_system" AS String) AS source_system,
    CAST("_endpoint" AS String) AS endpoint,
    now() AS dbt_loaded_at,
    
    cityHash64(
        concat(
            COALESCE("ID", ''),
            COALESCE("Code", ''),
            COALESCE("Name", ''),
            "_extracted_at"
        )
    ) AS record_hash

FROM {{ source('repsly_raw', 'raw_users') }}

{% if is_incremental() %}
WHERE parseDateTimeBestEffort("_extracted_at") > 
    (SELECT COALESCE(max(parseDateTimeBestEffort(extracted_at)), toDateTime('1900-01-01'))
     FROM {{ this }})
{% endif %}