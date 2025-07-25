{{ config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='append_new_columns',
    meta={
        'description': 'Raw visit schedule realizations data from Repsly API - append-only, all fields as strings',
        'data_source': 'repsly_api',
        'update_frequency': 'incremental'
    }
) }}

-- Bronze layer: Pure append-only storage for visit schedule realizations
-- All fields stored as strings to avoid type conflicts
SELECT 
    CAST(COALESCE("ScheduleId", '') AS String) AS ScheduleId,
    CAST(COALESCE("ProjectId", '') AS String) AS ProjectId,
    CAST(COALESCE("EmployeeId", '') AS String) AS EmployeeId,
    CAST(COALESCE("EmployeeCode", '') AS String) AS EmployeeCode,
    CAST(COALESCE("PlaceId", '') AS String) AS PlaceId,
    CAST(COALESCE("PlaceCode", '') AS String) AS PlaceCode,
    CAST(COALESCE("ModifiedUTC", '') AS String) AS ModifiedUTC,
    CAST(COALESCE("TimeZone", '') AS String) AS TimeZone,
    CAST(COALESCE("ScheduleNote", '') AS String) AS ScheduleNote,
    CAST(COALESCE("Status", '') AS String) AS Status,
    CAST(COALESCE("DateTimeStart", '') AS String) AS DateTimeStart,
    CAST(COALESCE("DateTimeStartUTC", '') AS String) AS DateTimeStartUTC,
    CAST(COALESCE("DateTimeEnd", '') AS String) AS DateTimeEnd,
    CAST(COALESCE("DateTimeEndUTC", '') AS String) AS DateTimeEndUTC,
    CAST(COALESCE("PlanDateTimeStart", '') AS String) AS PlanDateTimeStart,
    CAST(COALESCE("PlanDateTimeStartUTC", '') AS String) AS PlanDateTimeStartUTC,
    CAST(COALESCE("PlanDateTimeEnd", '') AS String) AS PlanDateTimeEnd,
    CAST(COALESCE("PlanDateTimeEndUTC", '') AS String) AS PlanDateTimeEndUTC,
    CAST(COALESCE("Tasks", '') AS String) AS Tasks,

    -- System metadata
    CAST("_extracted_at" AS String) AS extracted_at,
    CAST("_source_system" AS String) AS source_system,
    CAST("_endpoint" AS String) AS endpoint,
    now() AS dbt_loaded_at,
    
    cityHash64(
        concat(
            COALESCE("ScheduleId", ''),
            COALESCE("ModifiedUTC", ''),
            COALESCE("EmployeeCode", ''),
            "_extracted_at"
        )
    ) AS record_hash

FROM {{ source('repsly_raw', 'raw_visit_schedule_realizations') }}

{% if is_incremental() %}
WHERE parseDateTimeBestEffort("_extracted_at") > 
    (SELECT COALESCE(max(parseDateTimeBestEffort(extracted_at)), toDateTime('1900-01-01'))
     FROM {{ this }})
{% endif %}