-- Raw bills data with basic filtering and renaming
-- dbt/models/raw/acumatica/bills_raw.sql

{{ config(materialized='view') }}

SELECT
    "id"::TEXT as bill_id,
    "rowNumber"::TEXT as row_number,
    "note"::TEXT as note,
    "Amount"::TEXT as amount_raw,
    "ApprovedForPayment"::TEXT as approved_for_payment_raw,
    "Balance"::TEXT as balance_raw,
    "BranchID"::TEXT as branch_id,
    "CashAccount"::TEXT as cash_account,
    "CurrencyID"::TEXT as currency_id,
    "Date"::TEXT as date_raw,
    "Description"::TEXT as description,
    "DueDate"::TEXT as due_date_raw,
    "Hold"::TEXT as hold_raw,
    "IsTaxValid"::TEXT as is_tax_valid_raw,
    "LastModifiedDateTime"::TEXT as last_modified_datetime_raw,
    "LocationID"::TEXT as location_id,
    "PostPeriod"::TEXT as post_period,
    "ReferenceNbr"::TEXT as reference_number,
    "Status"::TEXT as status_raw,
    "TaxTotal"::TEXT as tax_total_raw,
    "Terms"::TEXT as terms,
    "Type"::TEXT as type_raw,
    "Vendor"::TEXT as vendor_code,
    "VendorRef"::TEXT as vendor_ref,
    "custom"::TEXT as custom_raw,
    "_links"::TEXT as links_raw,
    "_extracted_at" as extracted_at,
    "_source_system" as source_system,
    "_endpoint" as endpoint

FROM {{ source('acumatica_raw', 'raw_bills') }}
WHERE "id" IS NOT NULL