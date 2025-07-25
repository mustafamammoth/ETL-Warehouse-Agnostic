{{ config(
    materialized='table',
    schema=var('silver_schema', 'repsly_silver'),
    engine='MergeTree()',
    order_by='(bronze_processed_at, processed_at)',
    partition_by='toYYYYMM(bronze_processed_at)',
    meta={
        'description': 'Cleaned visit schedule realizations data from Repsly',
        'update_strategy': 'latest_wins'
    }
) }}

-- Silver layer: simple cleaning with basic type conversion (no filtering)
WITH cleaned AS (

    SELECT
        -- Primary identifiers
        NULLIF(trimBoth(ScheduleId), '') AS ScheduleId,
        NULLIF(trimBoth(ProjectId), '') AS ProjectId,
        NULLIF(trimBoth(EmployeeId), '') AS EmployeeId,
        NULLIF(trimBoth(EmployeeCode), '') AS EmployeeCode,
        NULLIF(trimBoth(PlaceId), '') AS PlaceId,
        NULLIF(trimBoth(PlaceCode), '') AS PlaceCode,

        -- Date-time fields
        parseDateTimeBestEffortOrNull(ModifiedUTC) AS ModifiedUTC,
        parseDateTimeBestEffortOrNull(DateTimeStart) AS DateTimeStart,
        parseDateTimeBestEffortOrNull(DateTimeStartUTC) AS DateTimeStartUTC,
        parseDateTimeBestEffortOrNull(DateTimeEnd) AS DateTimeEnd,
        parseDateTimeBestEffortOrNull(DateTimeEndUTC) AS DateTimeEndUTC,
        parseDateTimeBestEffortOrNull(PlanDateTimeStart) AS PlanDateTimeStart,
        parseDateTimeBestEffortOrNull(PlanDateTimeStartUTC) AS PlanDateTimeStartUTC,
        parseDateTimeBestEffortOrNull(PlanDateTimeEnd) AS PlanDateTimeEnd,
        parseDateTimeBestEffortOrNull(PlanDateTimeEndUTC) AS PlanDateTimeEndUTC,

        -- Status
        CASE lower(trimBoth(Status))
             WHEN 'planned' THEN 'planned'
             WHEN 'unplanned' THEN 'unplanned'
             WHEN 'missing' THEN 'missing'
             WHEN 'inactive' THEN 'inactive'
             ELSE NULLIF(trimBoth(Status), '')
        END AS Status,

        -- Other fields
        NULLIF(trimBoth(TimeZone), '') AS TimeZone,
        NULLIF(trimBoth(ScheduleNote), '') AS ScheduleNote,
        NULLIF(trimBoth(Tasks), '') AS Tasks,

        -- System metadata
        parseDateTimeBestEffortOrNull(extracted_at) AS data_extracted_at,
        dbt_loaded_at AS bronze_processed_at,
        now() AS processed_at,
        source_system,
        endpoint,
        record_hash

    FROM {{ ref('visit_schedule_realizations_raw') }}
)

SELECT *
FROM cleaned