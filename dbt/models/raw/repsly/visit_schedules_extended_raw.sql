{{ config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='append_new_columns',
    meta={
        'description': 'Raw visit schedules extended data from Repsly API - append-only, all fields as strings',
        'data_source': 'repsly_api',
        'update_frequency': 'incremental'
    }
) }}

-- Bronze layer: Pure append-only storage for visit schedules extended
-- All fields stored as strings to avoid type conflicts
SELECT 
    CAST(COALESCE("ScheduleCode", '') AS String) AS ScheduleCode,
    CAST(COALESCE("ScheduledDate", '') AS String) AS ScheduledDate,
    CAST(COALESCE("ScheduledTime", '') AS String) AS ScheduledTime,
    CAST(COALESCE("UserID", '') AS String) AS UserID,
    CAST(COALESCE("RepresentativeName", '') AS String) AS RepresentativeName,
    CAST(COALESCE("ClientCode", '') AS String) AS ClientCode,
    CAST(COALESCE("ClientName", '') AS String) AS ClientName,
    CAST(COALESCE("StreetAddress", '') AS String) AS StreetAddress,
    CAST(COALESCE("ZIP", '') AS String) AS ZIP,
    CAST(COALESCE("ZIPExt", '') AS String) AS ZIPExt,
    CAST(COALESCE("City", '') AS String) AS City,
    CAST(COALESCE("State", '') AS String) AS State,
    CAST(COALESCE("Country", '') AS String) AS Country,
    CAST(COALESCE("DueDate", '') AS String) AS DueDate,
    CAST(COALESCE("VisitNote", '') AS String) AS VisitNote,
    CAST(COALESCE("ProjectName", '') AS String) AS ProjectName,
    CAST(COALESCE("ScheduledDuration", '') AS String) AS ScheduledDuration,
    CAST(COALESCE("RepeatEveryWeeks", '') AS String) AS RepeatEveryWeeks,
    CAST(COALESCE("RepeatDays", '') AS String) AS RepeatDays,
    CAST(COALESCE("RepeatEndDate", '') AS String) AS RepeatEndDate,
    CAST(COALESCE("AlertUsersIfMissed", '') AS String) AS AlertUsersIfMissed,
    CAST(COALESCE("AlertUsersIfLate", '') AS String) AS AlertUsersIfLate,
    CAST(COALESCE("AlertUsersIfDone", '') AS String) AS AlertUsersIfDone,
    CAST(COALESCE("ExternalID", '') AS String) AS ExternalID,
    CAST(COALESCE("Tasks", '') AS String) AS Tasks,

    -- System metadata
    CAST("_extracted_at" AS String) AS extracted_at,
    CAST("_source_system" AS String) AS source_system,
    CAST("_endpoint" AS String) AS endpoint,
    now() AS dbt_loaded_at,
    
    cityHash64(
        concat(
            COALESCE("ScheduleCode", ''),
            COALESCE("ScheduledDate", ''),
            COALESCE("UserID", ''),
            COALESCE("ClientCode", ''),
            "_extracted_at"
        )
    ) AS record_hash

FROM {{ source('bronze_repsly', 'raw_visit_schedules_extended') }}

{% if is_incremental() %}
WHERE parseDateTimeBestEffort("_extracted_at") > 
    (SELECT COALESCE(max(parseDateTimeBestEffort(extracted_at)), toDateTime('1900-01-01'))
     FROM {{ this }})
{% endif %}