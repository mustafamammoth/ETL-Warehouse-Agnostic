-- Raw visits data with basic filtering and renaming (consistent TEXT approach) (dbt/models/raw/resply/visits_raw.sql)
{{ config(materialized='view') }}

SELECT 
    "VisitID"::TEXT as visit_id,
    "TimeStamp"::TEXT as timestamp_raw,
    "Date"::TEXT as date_raw,
    "RepresentativeCode"::TEXT as representative_code,
    "RepresentativeName"::TEXT as representative_name,
    "ExplicitCheckIn"::TEXT as explicit_check_in_raw,
    "DateAndTimeStart"::TEXT as date_and_time_start_raw,
    "DateAndTimeEnd"::TEXT as date_and_time_end_raw,
    "ClientCode"::TEXT as client_code,
    "ClientName"::TEXT as client_name,
    "StreetAddress"::TEXT as street_address,
    "ZIP"::TEXT as zip_code,
    "ZIPExt"::TEXT as zip_ext,
    "City"::TEXT as city,
    "State"::TEXT as state,
    "Country"::TEXT as country,
    "Territory"::TEXT as territory,
    "LatitudeStart"::TEXT as latitude_start_raw,
    "LongitudeStart"::TEXT as longitude_start_raw,
    "LatitudeEnd"::TEXT as latitude_end_raw,
    "LongitudeEnd"::TEXT as longitude_end_raw,
    "PrecisionStart"::TEXT as precision_start_raw,
    "PrecisionEnd"::TEXT as precision_end_raw,
    "VisitStatusBySchedule"::TEXT as visit_status_by_schedule_raw,
    "VisitEnded"::TEXT as visit_ended_raw,
    "_extracted_at" as extracted_at,
    "_source_system" as source_system,
    "_endpoint" as endpoint

FROM {{ source('repsly_raw', 'raw_visits') }}
WHERE "VisitID" IS NOT NULL