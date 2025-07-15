-- Raw photos data with basic filtering and renaming
-- dbt/models/raw/repsly/photos_raw.sql

{{ config(materialized='view') }}

SELECT
    "PhotoID"::TEXT as photo_id,
    "ClientCode"::TEXT as client_code,
    "ClientName"::TEXT as client_name,
    "Note"::TEXT as note,
    "DateAndTime"::TEXT as date_and_time_raw,
    "PhotoURL"::TEXT as photo_url,
    "RepresentativeCode"::TEXT as representative_code,
    "RepresentativeName"::TEXT as representative_name,
    "VisitID"::TEXT as visit_id,
    "Tag"::TEXT as tag,
    "_extracted_at" as extracted_at,
    "_source_system" as source_system,
    "_endpoint" as endpoint

FROM {{ source('repsly_raw', 'raw_photos') }}
WHERE "PhotoID" IS NOT NULL