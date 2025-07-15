-- Raw representatives data with basic filtering and renaming (consistent TEXT approach) (dbt/models/raw/repsly/representatives_raw.sql)
{{ config(materialized='view') }}

SELECT 
    "Code"::TEXT as code,
    "Name"::TEXT as name,
    "Note"::TEXT as note,
    "Password"::TEXT as password_raw,
    "Email"::TEXT as email,
    "Phone"::TEXT as phone,
    "Mobile"::TEXT as mobile,
    "Territories"::TEXT as territories_json,
    "Active"::TEXT as active_raw,
    "Attributes"::TEXT as attributes_json,
    "Address1"::TEXT as address1,
    "Address2"::TEXT as address2,
    "City"::TEXT as city,
    "State"::TEXT as state,
    "ZipCode"::TEXT as zip_code,
    "ZipCodeExt"::TEXT as zip_code_ext,
    "Country"::TEXT as country,
    "CountryCode"::TEXT as country_code,
    "_extracted_at" as extracted_at,
    "_source_system" as source_system,
    "_endpoint" as endpoint

FROM {{ source('repsly_raw', 'raw_representatives') }}
WHERE "Code" IS NOT NULL OR "Name" IS NOT NULL