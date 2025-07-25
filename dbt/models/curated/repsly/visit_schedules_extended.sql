{{ config(
    materialized='table',
    schema=var('silver_schema', 'repsly_silver'),
    engine='MergeTree()',
    order_by='(bronze_processed_at, processed_at)',
    partition_by='toYYYYMM(bronze_processed_at)',
    meta={
        'description': 'Cleaned visit schedules extended data from Repsly',
        'update_strategy': 'latest_wins'
    }
) }}

-- Silver layer: simple cleaning with basic type conversion (no filtering)
WITH cleaned AS (

    SELECT
        -- Primary identifiers
        NULLIF(trimBoth(ScheduleCode), '') AS ScheduleCode,
        NULLIF(trimBoth(UserID), '') AS UserID,
        NULLIF(trimBoth(ClientCode), '') AS ClientCode,
        
        -- Date and time fields
        parseDateTimeBestEffortOrNull(ScheduledDate) AS ScheduledDate,
        NULLIF(trimBoth(ScheduledTime), '') AS ScheduledTime,
        parseDateTimeBestEffortOrNull(RepeatEndDate) AS RepeatEndDate,
        
        -- Boolean field
        CASE 
            WHEN lower(trimBoth(DueDate)) IN ('true', '1', 't', 'yes') THEN 1
            WHEN lower(trimBoth(DueDate)) IN ('false', '0', 'f', 'no') THEN 0
            ELSE NULL
        END AS DueDate,
        
        -- Integer fields
        toInt32OrNull(NULLIF(trimBoth(ScheduledDuration), '')) AS ScheduledDuration,
        toInt32OrNull(NULLIF(trimBoth(RepeatEveryWeeks), '')) AS RepeatEveryWeeks,
        
        -- String fields - just remove nulls and trim
        NULLIF(trimBoth(RepresentativeName), '') AS RepresentativeName,
        NULLIF(trimBoth(ClientName), '') AS ClientName,
        NULLIF(trimBoth(StreetAddress), '') AS StreetAddress,
        NULLIF(trimBoth(ZIP), '') AS ZIP,
        NULLIF(trimBoth(ZIPExt), '') AS ZIPExt,
        NULLIF(trimBoth(City), '') AS City,
        NULLIF(trimBoth(State), '') AS State,
        NULLIF(trimBoth(Country), '') AS Country,
        NULLIF(trimBoth(VisitNote), '') AS VisitNote,
        NULLIF(trimBoth(ProjectName), '') AS ProjectName,
        NULLIF(trimBoth(RepeatDays), '') AS RepeatDays,
        NULLIF(trimBoth(AlertUsersIfMissed), '') AS AlertUsersIfMissed,
        NULLIF(trimBoth(AlertUsersIfLate), '') AS AlertUsersIfLate,
        NULLIF(trimBoth(AlertUsersIfDone), '') AS AlertUsersIfDone,
        NULLIF(trimBoth(ExternalID), '') AS ExternalID,
        
        -- Tasks array - keep as string for now (JSON)
        NULLIF(trimBoth(Tasks), '') AS Tasks,

        -- System metadata
        parseDateTimeBestEffortOrNull(extracted_at) AS data_extracted_at,
        dbt_loaded_at AS bronze_processed_at,
        now() AS processed_at,
        source_system,
        endpoint,
        record_hash

    FROM {{ ref('visit_schedules_extended_raw') }}
)

SELECT *
FROM cleaned