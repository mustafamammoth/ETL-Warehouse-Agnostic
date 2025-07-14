-- Raw client notes data with basic filtering and renaming (consistent TEXT approach)
{{ config(materialized='view') }}

SELECT 
    "ClientNoteID"::TEXT as client_note_id,
    "TimeStamp"::TEXT as timestamp_raw,
    "DateAndTime"::TEXT as date_and_time_raw,
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
    "Email"::TEXT as email,
    "Phone"::TEXT as phone,
    "Mobile"::TEXT as mobile,
    "Territory"::TEXT as territory,
    "Longitude"::TEXT as longitude_raw,
    "Latitude"::TEXT as latitude_raw,
    "Note"::TEXT as note,
    "VisitID"::TEXT as visit_id,
    "_extracted_at" as extracted_at,
    "_source_system" as source_system,
    "_endpoint" as endpoint,
    "_testing_mode" as testing_mode

FROM {{ source('repsly_raw', 'raw_client_notes') }}
WHERE "ClientNoteID" IS NOT NULL