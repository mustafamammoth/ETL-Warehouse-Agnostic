-- Cleaned bills data (direct from raw to business-ready)
-- dbt/models/curated/acumatica/bills.sql

{{ config(materialized='table') }}

SELECT
    -- Core identifiers
    bill_id,
    row_number::INTEGER as row_number,
    vendor_code,
    TRIM(COALESCE(vendor_ref, '')) as vendor_ref,
    
    -- Financial amounts
    amount_raw::DECIMAL(15,2) as amount,
    balance_raw::DECIMAL(15,2) as balance,
    tax_total_raw::DECIMAL(15,2) as tax_total,
    
    -- Dates (parse ISO format)
    date_raw::DATE as bill_date,
    due_date_raw::DATE as due_date,
    last_modified_datetime_raw::TIMESTAMP as last_modified_datetime,
    
    -- Status and flags
    CASE WHEN UPPER(approved_for_payment_raw) = 'TRUE' THEN TRUE ELSE FALSE END as is_approved_for_payment,
    CASE WHEN UPPER(hold_raw) = 'TRUE' THEN TRUE ELSE FALSE END as is_on_hold,
    CASE WHEN UPPER(is_tax_valid_raw) = 'TRUE' THEN TRUE ELSE FALSE END as is_tax_valid,
    
    -- Bill details
    TRIM(COALESCE(description, '')) as description,
    TRIM(COALESCE(note, '')) as note,
    TRIM(status_raw) as status,
    TRIM(type_raw) as bill_type,
    TRIM(COALESCE(terms, '')) as payment_terms,
    
    -- Organizational
    branch_id,
    location_id,
    cash_account,
    currency_id,
    post_period,
    reference_number,
    
    -- Date analysis
    EXTRACT(YEAR FROM date_raw::DATE) as bill_year,
    EXTRACT(MONTH FROM date_raw::DATE) as bill_month,
    EXTRACT(DOW FROM date_raw::DATE) as bill_day_of_week,
    
    -- Days calculations
    CASE WHEN due_date_raw IS NOT NULL AND date_raw IS NOT NULL THEN
         (due_date_raw::DATE - date_raw::DATE)
    END as days_to_due,
    
    CASE WHEN due_date_raw IS NOT NULL THEN
         (CURRENT_DATE - due_date_raw::DATE)
    END as days_past_due,
    
    -- Business categorizations
    CASE 
        WHEN TRIM(type_raw) = 'Credit Adj.' THEN 'Credit Adjustment'
        WHEN TRIM(type_raw) = 'Bill' THEN 'Regular Bill'
        WHEN TRIM(type_raw) = 'Debit Adj.' THEN 'Debit Adjustment'
        ELSE 'Other'
    END as bill_category,
    
    CASE 
        WHEN TRIM(status_raw) = 'Closed' THEN 'Closed'
        WHEN TRIM(status_raw) = 'Open' THEN 'Open'
        WHEN TRIM(status_raw) = 'Paid' THEN 'Paid'
        ELSE 'Other'
    END as status_category,
    
    CASE
        WHEN amount_raw::DECIMAL(15,2) = 0 THEN 'Zero Amount'
        WHEN amount_raw::DECIMAL(15,2) < 100 THEN 'Small (< $100)'
        WHEN amount_raw::DECIMAL(15,2) < 1000 THEN 'Medium ($100-$1K)'
        WHEN amount_raw::DECIMAL(15,2) < 10000 THEN 'Large ($1K-$10K)'
        ELSE 'Very Large ($10K+)'
    END as amount_category,
    
    CASE
        WHEN balance_raw::DECIMAL(15,2) = 0 THEN 'Fully Paid'
        WHEN balance_raw::DECIMAL(15,2) > 0 THEN 'Outstanding Balance'
        ELSE 'Credit Balance'
    END as balance_status,
    
    -- Payment terms analysis
    CASE 
        WHEN TRIM(COALESCE(terms, '')) = 'COD' THEN 'Cash on Delivery'
        WHEN TRIM(COALESCE(terms, '')) = 'NET15' THEN 'Net 15 Days'
        WHEN TRIM(COALESCE(terms, '')) = 'NET30' THEN 'Net 30 Days'
        WHEN TRIM(COALESCE(terms, '')) = 'NET60' THEN 'Net 60 Days'
        WHEN TRIM(COALESCE(terms, '')) = 'NET7' THEN 'Net 7 Days'
        WHEN TRIM(COALESCE(terms, '')) = '' THEN 'No Terms'
        ELSE 'Other Terms'
    END as payment_terms_category,
    
    -- Priority classification
    CASE
        WHEN UPPER(hold_raw) = 'TRUE' THEN 'On Hold'
        WHEN balance_raw::DECIMAL(15,2) > 0 AND (CURRENT_DATE - due_date_raw::DATE) > 30 THEN 'Overdue 30+ Days'
        WHEN balance_raw::DECIMAL(15,2) > 0 AND (CURRENT_DATE - due_date_raw::DATE) > 0 THEN 'Past Due'
        WHEN balance_raw::DECIMAL(15,2) > 0 THEN 'Open'
        ELSE 'Standard'
    END as priority_status,
    
    -- Vendor analysis
    CASE
        WHEN vendor_code LIKE 'V%' THEN 'Vendor'
        WHEN vendor_code LIKE 'C%' THEN 'Customer/Client'
        ELSE 'Other'
    END as vendor_type,
    
    -- Data quality flags
    (TRIM(COALESCE(description, '')) <> '') as has_description,
    (TRIM(COALESCE(note, '')) <> '') as has_note,
    (vendor_ref <> '') as has_vendor_ref,
    (tax_total_raw::DECIMAL(15,2) > 0) as has_tax,
    
    -- Data completeness score (0-100)
    (
        CASE WHEN vendor_code <> '' THEN 20 ELSE 0 END +
        CASE WHEN amount_raw::DECIMAL(15,2) IS NOT NULL THEN 20 ELSE 0 END +
        CASE WHEN date_raw IS NOT NULL THEN 20 ELSE 0 END +
        CASE WHEN status_raw <> '' THEN 20 ELSE 0 END +
        CASE WHEN TRIM(COALESCE(description, '')) <> '' THEN 10 ELSE 0 END +
        CASE WHEN TRIM(COALESCE(note, '')) <> '' THEN 10 ELSE 0 END
    ) as data_completeness_score,
    
    extracted_at,
    source_system

FROM {{ ref('bills_raw') }}
ORDER BY bill_date DESC, bill_id