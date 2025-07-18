-- models/bronze/bills_raw.sql
-- Raw bills data (bronze layer) – append-only source. Keep everything; filtering only NULL ReferenceNbr.
{{ config(materialized='view') }}

SELECT 
    id as bill_guid,
    "rowNumber"::TEXT as row_number,
    note::TEXT,
    "Amount"::TEXT as amount_raw,
    "ApprovedForPayment"::TEXT as approved_for_payment_raw,
    "Balance"::TEXT as balance_raw,
    "BranchID"::TEXT as branch_id,
    "CashAccount"::TEXT as cash_account,
    "CurrencyID"::TEXT as currency_id,
    "Date"::TEXT as bill_date_raw,
    "Description"::TEXT as description,
    "DueDate"::TEXT as due_date_raw,
    "Hold"::TEXT as hold_raw,
    "IsTaxValid"::TEXT as is_tax_valid_raw,
    "LastModifiedDateTime"::TEXT as last_modified_datetime_raw,
    "LocationID"::TEXT as location_id,
    "PostPeriod"::TEXT as post_period,
    "ReferenceNbr"::TEXT as reference_number,
    "Status"::TEXT as status,
    "TaxTotal"::TEXT as tax_total_raw,
    "Terms"::TEXT as payment_terms,
    "Type"::TEXT as bill_type,
    "Vendor"::TEXT as vendor_code,
    "VendorRef"::TEXT as vendor_ref,
    custom::TEXT,
    "_links"::TEXT as source_links,
    "_extracted_at" as extracted_at,
    "_source_system" as source_system,
    "_endpoint" as endpoint
FROM {{ source('acumatica_raw', 'raw_bill') }}
WHERE "ReferenceNbr" IS NOT NULL
