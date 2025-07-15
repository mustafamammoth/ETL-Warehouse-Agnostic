-- Raw visit schedules data with basic filtering and renaming
-- dbt/models/raw/repsly/visit_schedules_raw.sql

{{ config(materialized='view') }}

SELECT
    "ScheduleDateAndTime"::TEXT as schedule_date_and_time_raw,
    "RepresentativeCode"::TEXT as representative_code,
    "RepresentativeName"::TEXT as representative_name,
    "ClientCode"::TEXT as client_code,
    "ClientName"::TEXT as client_name,
    "StreetAddress"::TEXT as street_address,
    "ZIP"::TEXT as zip_code,
    "ZIPExt"::TEXT as zip_ext,
    "City"::TEXT as city,
    "State"::TEXT as state,
    "Country"::TEXT as country,
    "DueDate"::TEXT as due_date_raw,
    "VisitNote"::TEXT as visit_note,
    "_extracted_at" as extracted_at,
    "_source_system" as source_system,
    "_endpoint" as endpoint

FROM {{ source('repsly_raw', 'raw_visit_schedules') }}
WHERE "RepresentativeCode" IS NOT NULL