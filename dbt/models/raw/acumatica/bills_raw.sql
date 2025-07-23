-- models/raw/acumatica/bills_raw.sql
-- Raw bills data (bronze layer) - ClickHouse compatible with incremental handling
{{ config(
    materialized='view',
    meta={
        'description': 'Raw bills data from Acumatica API with incremental support'
    }
) }}

SELECT 
    -- Primary identifiers
    coalesce(nullif(trim(id), ''), nullif(trim("ReferenceNbr"), '')) as bill_guid,
    trim("ReferenceNbr") as reference_number,
    
    -- Numeric fields (keep as text for flexibility in bronze layer)
    trim("Amount") as amount_raw,
    trim("Balance") as balance_raw,
    trim("TaxTotal") as tax_total_raw,
    
    -- Boolean fields (keep as text)
    trim("ApprovedForPayment") as approved_for_payment_raw,
    trim("Hold") as hold_raw,
    trim("IsTaxValid") as is_tax_valid_raw,
    
    -- Date/timestamp fields (keep as text for parsing flexibility)
    trim("Date") as bill_date_raw,
    trim("DueDate") as due_date_raw,
    trim("LastModifiedDateTime") as last_modified_datetime_raw,
    
    -- Text/categorical fields
    trim("BranchID") as branch_id,
    trim("CashAccount") as cash_account,
    trim("CurrencyID") as currency_id,
    trim("Description") as description,
    trim("LocationID") as location_id,
    trim("PostPeriod") as post_period,
    trim("Status") as status,
    trim("Terms") as payment_terms,
    trim("Type") as bill_type,
    trim("Vendor") as vendor_code,
    trim("VendorRef") as vendor_ref,
    
    -- Additional fields
    coalesce(trim("rowNumber"), '') as row_number,
    coalesce(trim(note), '') as note,
    coalesce(trim(custom), '') as custom,
    
    -- Metadata fields
    trim("_links") as source_links,
    "_extracted_at" as extracted_at,
    "_source_system" as source_system,
    "_endpoint" as endpoint

FROM {{ source('acumatica_raw', 'raw_bill') }}
WHERE 
    -- Filter out records without essential identifiers
    (
        (id IS NOT NULL AND trim(id) != '') 
        OR 
        ("ReferenceNbr" IS NOT NULL AND trim("ReferenceNbr") != '')
    )
    -- Filter out completely empty records
    AND NOT (
        coalesce(trim(id), '') = '' 
        AND coalesce(trim("ReferenceNbr"), '') = ''
        AND coalesce(trim("Amount"), '') = ''
        AND coalesce(trim("Vendor"), '') = ''
    )