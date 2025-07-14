-- Cleaned and standardized users data
-- Raw users data with basic filtering and renaming (consistent TEXT approach)
{{ config(materialized='view') }}

SELECT 
    "ID"::TEXT as user_id,
    "Code"::TEXT as code,
    "Name"::TEXT as name,
    "Email"::TEXT as email,
    "Active"::TEXT as active_raw,
    "Role"::TEXT as role_raw,
    "Note"::TEXT as note,
    "Phone"::TEXT as phone,
    "Territories"::TEXT as territories_json,
    "SendEmailEnabled"::TEXT as send_email_enabled_raw,
    "Address1"::TEXT as address1,
    "Address2"::TEXT as address2,
    "City"::TEXT as city,
    "State"::TEXT as state,
    "ZipCode"::TEXT as zip_code,
    "ZipCodeExt"::TEXT as zip_code_ext,
    "Country"::TEXT as country,
    "CountryCode"::TEXT as country_code,
    "Attributes"::TEXT as attributes_json,
    "Permissions"::TEXT as permissions_json,
    "Teams"::TEXT as teams_json,
    "ShowActivitiesWithoutTeam"::TEXT as show_activities_without_team_raw,
    "_extracted_at" as extracted_at,
    "_source_system" as source_system,
    "_endpoint" as endpoint,
    "_testing_mode" as testing_mode

FROM {{ source('repsly_raw', 'raw_users') }}
WHERE "ID" IS NOT NULL


