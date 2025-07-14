-- Raw customer data with basic filtering and renaming (consistent TEXT approach)
{{ config(materialized='view') }}

SELECT 
    id as customer_guid,
    "CustomerID"::TEXT as customer_id,
    "CustomerName"::TEXT as customer_name,
    "Email"::TEXT as email_raw,
    "CreditLimit"::TEXT as credit_limit_raw,
    "Status"::TEXT as status,
    "CustomerClass"::TEXT as customer_class,
    "TaxZone"::TEXT as tax_zone,
    "Terms"::TEXT as payment_terms,
    "CreatedDateTime"::TEXT as created_datetime_raw,
    "LastModifiedDateTime"::TEXT as last_modified_datetime_raw,
    note::TEXT,
    "AccountRef"::TEXT as account_ref,
    "ShippingZoneID"::TEXT as shipping_zone,
    "WriteOffLimit"::TEXT as write_off_limit_raw,
    _extracted_at,
    _source_system,
    _endpoint

FROM {{ source('acumatica_raw', 'raw_customers') }}
WHERE "CustomerID" IS NOT NULL

-- -- Raw customer data with basic filtering and renaming (customers_raw.sql)
-- {{ config(materialized='view') }}

-- SELECT 
--     id as customer_guid,
--     "CustomerID" as customer_id,
--     "CustomerName" as customer_name,
--     "Email" as email_raw,
--     "CreditLimit" as credit_limit_raw,
--     "Status" as status,
--     "CustomerClass" as customer_class,
--     "TaxZone" as tax_zone,
--     "Terms" as payment_terms,
--     "CreatedDateTime" as created_datetime_raw,
--     "LastModifiedDateTime" as last_modified_datetime_raw,
--     note,
--     "AccountRef" as account_ref,
--     "ShippingZoneID" as shipping_zone,
--     "WriteOffLimit" as write_off_limit_raw,
--     _extracted_at,
--     _source_system,
--     _endpoint

-- FROM {{ source('acumatica_raw', 'raw_customers') }}
-- WHERE "CustomerID" IS NOT NULL
--   AND "CustomerID" != ''