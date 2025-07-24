{{ config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='append_new_columns',
    meta={
        'description': 'Raw visits data from Repsly API - append-only, all fields as strings',
        'data_source': 'repsly_api',
        'update_frequency': 'incremental'
    }
) }}

-- Bronze layer: Pure append-only storage
-- All fields stored as strings to avoid type conflicts
SELECT 
    -- Cast ALL API fields to String to prevent type conflicts
    CAST(COALESCE("VisitID", '') AS String) AS visit_id,
    CAST(COALESCE("TimeStamp", '') AS String) AS timestamp_raw,
    CAST(COALESCE("Date", '') AS String) AS date_raw,
    CAST(COALESCE("RepresentativeCode", '') AS String) AS representative_code,
    CAST(COALESCE("RepresentativeName", '') AS String) AS representative_name,
    CAST(COALESCE("ExplicitCheckIn", '') AS String) AS explicit_check_in_raw,
    CAST(COALESCE("DateAndTimeStart", '') AS String) AS date_and_time_start_raw,
    CAST(COALESCE("DateAndTimeEnd", '') AS String) AS date_and_time_end_raw,
    CAST(COALESCE("ClientCode", '') AS String) AS client_code,
    CAST(COALESCE("ClientName", '') AS String) AS client_name,
    CAST(COALESCE("StreetAddress", '') AS String) AS street_address,
    CAST(COALESCE("ZIP", '') AS String) AS zip_code,
    CAST(COALESCE("ZIPExt", '') AS String) AS zip_ext,
    CAST(COALESCE("City", '') AS String) AS city,
    CAST(COALESCE("State", '') AS String) AS state,
    CAST(COALESCE("Country", '') AS String) AS country,
    CAST(COALESCE("Territory", '') AS String) AS territory,
    CAST(COALESCE("LatitudeStart", '') AS String) AS latitude_start_raw,
    CAST(COALESCE("LongitudeStart", '') AS String) AS longitude_start_raw,
    CAST(COALESCE("LatitudeEnd", '') AS String) AS latitude_end_raw,
    CAST(COALESCE("LongitudeEnd", '') AS String) AS longitude_end_raw,
    CAST(COALESCE("PrecisionStart", '') AS String) AS precision_start_raw,
    CAST(COALESCE("PrecisionEnd", '') AS String) AS precision_end_raw,
    CAST(COALESCE("VisitStatusBySchedule", '') AS String) AS visit_status_by_schedule_raw,
    CAST(COALESCE("VisitEnded", '') AS String) AS visit_ended_raw,

    -- System metadata
    CAST("_extracted_at" AS String) AS extracted_at,
    CAST("_source_system" AS String) AS source_system,
    CAST("_endpoint" AS String) AS endpoint,
    now() AS dbt_loaded_at,
    
    cityHash64(
        concat(
            COALESCE("VisitID", ''),
            COALESCE("TimeStamp", ''),
            "_extracted_at"
        )
    ) AS record_hash

FROM {{ source('repsly_raw', 'raw_visits') }}

{% if is_incremental() %}
WHERE parseDateTimeBestEffort("_extracted_at") > 
    (SELECT COALESCE(max(parseDateTimeBestEffort(extracted_at)), toDateTime('1900-01-01'))
     FROM {{ this }})
{% endif %}