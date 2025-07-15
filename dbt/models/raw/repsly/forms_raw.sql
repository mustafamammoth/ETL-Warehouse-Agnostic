-- Raw forms data with basic filtering and renaming
-- dbt/models/raw/repsly/forms_raw.sql

{{ config(materialized='view') }}

SELECT
    "FormID"::TEXT as form_id,
    "FormName"::TEXT as form_name,
    "ClientCode"::TEXT as client_code,
    "ClientName"::TEXT as client_name,
    "DateAndTime"::TEXT as date_and_time_raw,
    "RepresentativeCode"::TEXT as representative_code,
    "RepresentativeName"::TEXT as representative_name,
    "StreetAddress"::TEXT as street_address,
    "ZIP"::TEXT as zip_code,
    "City"::TEXT as city,
    "State"::TEXT as state,
    "Country"::TEXT as country,
    "Email"::TEXT as email,
    "Phone"::TEXT as phone,
    "Longitude"::TEXT as longitude_raw,
    "Latitude"::TEXT as latitude_raw,
    "SignatureURL"::TEXT as signature_url,
    "VisitStart"::TEXT as visit_start_raw,
    "VisitEnd"::TEXT as visit_end_raw,
    "Items"::TEXT as items_raw,
    "VisitID"::TEXT as visit_id,
    "_extracted_at" as extracted_at,
    "_source_system" as source_system,
    "_endpoint" as endpoint

FROM {{ source('repsly_raw', 'raw_forms') }}
WHERE "FormID" IS NOT NULL