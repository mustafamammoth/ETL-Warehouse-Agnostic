{{ config(
    materialized='table',
    schema=var('silver_schema', 'repsly_silver'),
    engine='MergeTree()',
    order_by='(bronze_processed_at, processed_at)',
    partition_by='toYYYYMM(bronze_processed_at)',
    meta={
        'description': 'Cleaned visit schedules data from Repsly',
        'update_strategy': 'latest_wins'
    }
) }}

-- Silver layer: simple cleaning with basic type conversion (no filtering)
WITH cleaned AS (

    SELECT
        -- Convert datetime fields
        parseDateTimeBestEffortOrNull(ScheduleDateAndTime) AS ScheduleDateAndTime,
        parseDateTimeBestEffortOrNull(DueDate) AS DueDate,
        
        -- String fields - just remove nulls and trim
        NULLIF(trimBoth(RepresentativeCode), '') AS RepresentativeCode,
        NULLIF(trimBoth(RepresentativeName), '') AS RepresentativeName,
        NULLIF(trimBoth(ClientCode), '') AS ClientCode,
        NULLIF(trimBoth(ClientName), '') AS ClientName,
        NULLIF(trimBoth(StreetAddress), '') AS StreetAddress,
        NULLIF(trimBoth(ZIP), '') AS ZIP,
        NULLIF(trimBoth(ZIPExt), '') AS ZIPExt,
        NULLIF(trimBoth(City), '') AS City,
        NULLIF(trimBoth(State), '') AS State,
        NULLIF(trimBoth(Country), '') AS Country,
        NULLIF(trimBoth(VisitNote), '') AS VisitNote,

        -- System metadata
        parseDateTimeBestEffortOrNull(extracted_at) AS data_extracted_at,
        dbt_loaded_at AS bronze_processed_at,
        now() AS processed_at,
        source_system,
        endpoint,
        record_hash

    FROM {{ ref('visit_schedules_raw') }}
)

SELECT *
FROM cleaned