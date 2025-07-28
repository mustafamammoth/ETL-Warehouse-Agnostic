-- models/bronze/purchase_orders_raw.sql
-- Raw purchase orders data (bronze layer) – append-only base view
-- Mirrors source table raw_purchase_order (landing table in ClickHouse/Postgres).
-- Only filters out NULL OrderNbr.

{{ config(materialized='view') }}

SELECT 
    id                                AS purchase_order_guid,
    "rowNumber"::TEXT                 AS row_number,
    "note"::TEXT                      AS note,
    "BaseCurrencyID"::TEXT            AS base_currency_id,
    "Branch"::TEXT                    AS branch,
    "ControlTotal"::TEXT              AS control_total_raw,
    "CurrencyEffectiveDate"::TEXT     AS currency_effective_date_raw,
    "CurrencyID"::TEXT                AS currency_id,
    "CurrencyRate"::TEXT              AS currency_rate_raw,
    "CurrencyRateTypeID"::TEXT        AS currency_rate_type_id,
    "CurrencyReciprocalRate"::TEXT    AS currency_reciprocal_rate_raw,
    "Date"::TEXT                      AS order_date_raw,
    "Description"::TEXT               AS description,
    "Hold"::TEXT                      AS hold_raw,
    "IsTaxValid"::TEXT                AS is_tax_valid_raw,
    "LastModifiedDateTime"::TEXT      AS last_modified_datetime_raw,
    "LineTotal"::TEXT                 AS line_total_raw,
    "Location"::TEXT                  AS location,
    "OrderNbr"::TEXT                  AS order_number,
    "OrderTotal"::TEXT                AS order_total_raw,
    "Owner"::TEXT                     AS owner,
    "PromisedOn"::TEXT                AS promised_date_raw,
    "Status"::TEXT                    AS status,
    "TaxTotal"::TEXT                  AS tax_total_raw,
    "Terms"::TEXT                     AS payment_terms,
    "Type"::TEXT                      AS order_type,
    "VendorID"::TEXT                  AS vendor_id,
    "VendorRef"::TEXT                 AS vendor_ref,
    "VendorTaxZone"::TEXT             AS vendor_tax_zone,
    "custom"::TEXT                    AS custom,
    "_links"::TEXT                    AS source_links,
    "_extracted_at"                   AS extracted_at,
    "_source_system"                  AS source_system,
    "_endpoint"                       AS endpoint
FROM {{ source('acumatica_raw', 'raw_purchase_order') }}
WHERE "OrderNbr" IS NOT NULL
