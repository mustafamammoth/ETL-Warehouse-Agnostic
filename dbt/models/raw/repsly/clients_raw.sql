-- Raw client data with basic filtering and renaming (consistent TEXT approach)
{{ config(materialized='view') }}

SELECT 
    "ClientID"::TEXT as client_id,
    "TimeStamp"::TEXT as timestamp_raw,
    "Code"::TEXT as code,
    "Name"::TEXT as name,
    "Active"::TEXT as active_raw,
    "Tag"::TEXT as tag,
    "Territory"::TEXT as territory,
    "RepresentativeCode"::TEXT as representative_code,
    "RepresentativeName"::TEXT as representative_name,
    "StreetAddress"::TEXT as street_address,
    "ZIP"::TEXT as zip_code,
    "ZIPExt"::TEXT as zip_ext,
    "City"::TEXT as city,
    "State"::TEXT as state,
    "Country"::TEXT as country,
    "Email"::TEXT as email,
    "Phone"::TEXT as phone,
    "Mobile"::TEXT as mobile,
    "Website"::TEXT as website,
    "ContactName"::TEXT as contact_name,
    "ContactTitle"::TEXT as contact_title,
    "Note"::TEXT as note,
    "Status"::TEXT as status,
    "CustomFields"::TEXT as custom_fields_json,
    "PriceLists"::TEXT as price_lists_json,
    "GPSLongitude"::TEXT as gps_longitude_raw,
    "GPSLatitude"::TEXT as gps_latitude_raw,
    "AccountCode"::TEXT as account_code,
    -- Custom fields (flattened from JSON during extraction)
    "custom_coa_emails"::TEXT as custom_coa_emails,
    "custom_dba"::TEXT as custom_dba,
    "custom_delivery_instructions"::TEXT as custom_delivery_instructions,
    _extracted_at,
    _source_system,
    _endpoint

FROM {{ source('repsly_raw', 'raw_clients') }}
WHERE "ClientID" IS NOT NULL