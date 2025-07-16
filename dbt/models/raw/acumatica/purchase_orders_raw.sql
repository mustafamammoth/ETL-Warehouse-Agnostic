-- Raw purchase orders data with basic filtering and renaming
-- dbt/models/raw/acumatica/purchase_orders_raw.sql

{{ config(materialized='view') }}

SELECT
    "id"::TEXT as purchase_order_id,
    "rowNumber"::TEXT as row_number,
    "note"::TEXT as note,
    "BaseCurrencyID"::TEXT as base_currency_id,
    "Branch"::TEXT as branch,
    "ControlTotal"::TEXT as control_total_raw,
    "CurrencyEffectiveDate"::TEXT as currency_effective_date_raw,
    "CurrencyID"::TEXT as currency_id,
    "CurrencyRate"::TEXT as currency_rate_raw,
    "CurrencyRateTypeID"::TEXT as currency_rate_type_id,
    "CurrencyReciprocalRate"::TEXT as currency_reciprocal_rate_raw,
    "Date"::TEXT as date_raw,
    "Description"::TEXT as description,
    "Hold"::TEXT as hold_raw,
    "IsTaxValid"::TEXT as is_tax_valid_raw,
    "LastModifiedDateTime"::TEXT as last_modified_datetime_raw,
    "LineTotal"::TEXT as line_total_raw,
    "Location"::TEXT as location,
    "OrderNbr"::TEXT as order_number,
    "OrderTotal"::TEXT as order_total_raw,
    "Owner"::TEXT as owner,
    "PromisedOn"::TEXT as promised_on_raw,
    "Status"::TEXT as status_raw,
    "TaxTotal"::TEXT as tax_total_raw,
    "Terms"::TEXT as terms,
    "Type"::TEXT as type_raw,
    "VendorID"::TEXT as vendor_id,
    "VendorRef"::TEXT as vendor_ref,
    "VendorTaxZone"::TEXT as vendor_tax_zone,
    "custom"::TEXT as custom_raw,
    "_links"::TEXT as links_raw,
    "_extracted_at" as extracted_at,
    "_source_system" as source_system,
    "_endpoint" as endpoint

FROM {{ source('acumatica_raw', 'raw_purchase_orders') }}
WHERE "id" IS NOT NULL