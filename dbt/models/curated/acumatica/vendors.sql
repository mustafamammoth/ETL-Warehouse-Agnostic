-- Cleaned vendors data (direct from raw to business-ready)
-- dbt/models/curated/acumatica/vendors.sql

{{ config(materialized='table') }}

SELECT
    -- Core identifiers
    vendor_id,
    row_number::INTEGER as row_number,
    TRIM(vendor_code) as vendor_code,
    TRIM(vendor_name) as vendor_name,
    TRIM(COALESCE(legal_name, '')) as legal_name,
    TRIM(COALESCE(account_ref, '')) as account_ref,
    
    -- Status and classification
    TRIM(status_raw) as status,
    TRIM(COALESCE(vendor_class, '')) as vendor_class,
    
    -- Boolean flags
    CASE WHEN UPPER(f1099_vendor_raw) = 'TRUE' THEN TRUE ELSE FALSE END as is_1099_vendor,
    CASE WHEN UPPER(foreign_entity_raw) = 'TRUE' THEN TRUE ELSE FALSE END as is_foreign_entity,
    CASE WHEN UPPER(landed_cost_vendor_raw) = 'TRUE' THEN TRUE ELSE FALSE END as is_landed_cost_vendor,
    CASE WHEN UPPER(vendor_is_labor_union_raw) = 'TRUE' THEN TRUE ELSE FALSE END as is_labor_union,
    CASE WHEN UPPER(vendor_is_tax_agency_raw) = 'TRUE' THEN TRUE ELSE FALSE END as is_tax_agency,
    CASE WHEN UPPER(pay_separately_raw) = 'TRUE' THEN TRUE ELSE FALSE END as pay_separately,
    CASE WHEN UPPER(print_orders_raw) = 'TRUE' THEN TRUE ELSE FALSE END as print_orders,
    CASE WHEN UPPER(send_orders_by_email_raw) = 'TRUE' THEN TRUE ELSE FALSE END as send_orders_by_email,
    
    -- Financial settings
    TRIM(COALESCE(terms, '')) as payment_terms,
    TRIM(COALESCE(payment_method, '')) as payment_method,
    TRIM(COALESCE(payment_by, '')) as payment_by,
    TRIM(COALESCE(currency_id, '')) as currency_id,
    
    -- Amounts and thresholds
    CASE WHEN max_receipt_raw ~ '^[0-9]+\.?[0-9]*$' 
         THEN max_receipt_raw::DECIMAL(15,2) END as max_receipt_amount,
    CASE WHEN min_receipt_raw ~ '^[0-9]+\.?[0-9]*$' 
         THEN min_receipt_raw::DECIMAL(15,2) END as min_receipt_amount,
    CASE WHEN threshold_receipt_raw ~ '^[0-9]+\.?[0-9]*$' 
         THEN threshold_receipt_raw::DECIMAL(15,2) END as threshold_receipt_amount,
    
    -- Time settings
    CASE WHEN lead_time_days_raw ~ '^[0-9]+$' 
         THEN lead_time_days_raw::INTEGER END as lead_time_days,
    CASE WHEN payment_lead_time_days_raw ~ '^[0-9]+$' 
         THEN payment_lead_time_days_raw::INTEGER END as payment_lead_time_days,
    
    -- Operational settings
    TRIM(COALESCE(receipt_action, '')) as receipt_action,
    TRIM(COALESCE(shipping_terms, '')) as shipping_terms,
    TRIM(COALESCE(ship_via, '')) as ship_via,
    TRIM(COALESCE(warehouse, '')) as warehouse,
    
    -- Tax information
    TRIM(COALESCE(tax_zone, '')) as tax_zone,
    TRIM(COALESCE(tax_registration_id, '')) as tax_registration_id,
    TRIM(COALESCE(f1099_box, '')) as f1099_box,
    
    -- Account information
    ap_account,
    ap_subaccount,
    cash_account,
    TRIM(COALESCE(parent_account, '')) as parent_account,
    
    -- Dates
    created_datetime_raw::TIMESTAMP as created_datetime,
    last_modified_datetime_raw::TIMESTAMP as last_modified_datetime,
    
    -- Additional details
    TRIM(COALESCE(note, '')) as note,
    TRIM(COALESCE(location_name, '')) as location_name,
    TRIM(COALESCE(receiving_branch, '')) as receiving_branch,
    
    -- Business categorizations
    CASE 
        WHEN TRIM(status_raw) = 'Active' THEN 'Active'
        WHEN TRIM(status_raw) = 'Inactive' THEN 'Inactive'
        WHEN TRIM(status_raw) = 'On Hold' THEN 'On Hold'
        ELSE 'Other'
    END as status_category,
    
    CASE 
        WHEN TRIM(COALESCE(vendor_class, '')) = 'CANNABIS' THEN 'Cannabis Vendor'
        WHEN TRIM(COALESCE(vendor_class, '')) = 'SERVICE' THEN 'Service Provider'
        WHEN TRIM(COALESCE(vendor_class, '')) = 'SUPPLY' THEN 'Supply Vendor'
        WHEN TRIM(COALESCE(vendor_class, '')) = '' THEN 'Unclassified'
        ELSE 'Other Class'
    END as vendor_type,
    
    -- Vendor code analysis
    CASE
        WHEN vendor_code LIKE 'V%' THEN 'Vendor'
        WHEN vendor_code LIKE 'C%' THEN 'Customer/Client'
        ELSE 'Other'
    END as vendor_code_type,
    
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
    
    -- Tax classification
    CASE 
        WHEN TRIM(COALESCE(tax_zone, '')) = 'CNNABISTAX' THEN 'Cannabis Tax'
        WHEN TRIM(COALESCE(tax_zone, '')) = 'EXEMPT' THEN 'Tax Exempt'
        WHEN TRIM(COALESCE(tax_zone, '')) = '' THEN 'No Tax Zone'
        ELSE 'Other Tax Zone'
    END as tax_classification,
    
    -- Receipt amount categorization
    CASE
        WHEN max_receipt_raw ~ '^[0-9]+\.?[0-9]*$' AND max_receipt_raw::DECIMAL(15,2) = 100 THEN 'Standard ($100)'
        WHEN max_receipt_raw ~ '^[0-9]+\.?[0-9]*$' AND max_receipt_raw::DECIMAL(15,2) < 100 THEN 'Low Threshold'
        WHEN max_receipt_raw ~ '^[0-9]+\.?[0-9]*$' AND max_receipt_raw::DECIMAL(15,2) > 100 THEN 'High Threshold'
        ELSE 'Not Set'
    END as receipt_threshold_category,
    
    -- Complexity classification
    CASE
        WHEN (CASE WHEN UPPER(f1099_vendor_raw) = 'TRUE' THEN TRUE ELSE FALSE END) 
             AND (CASE WHEN UPPER(foreign_entity_raw) = 'TRUE' THEN TRUE ELSE FALSE END) THEN 'Complex (1099 + Foreign)'
        WHEN (CASE WHEN UPPER(f1099_vendor_raw) = 'TRUE' THEN TRUE ELSE FALSE END) THEN 'Tax Reporting Required'
        WHEN (CASE WHEN UPPER(foreign_entity_raw) = 'TRUE' THEN TRUE ELSE FALSE END) THEN 'Foreign Entity'
        WHEN (CASE WHEN UPPER(vendor_is_labor_union_raw) = 'TRUE' THEN TRUE ELSE FALSE END) 
             OR (CASE WHEN UPPER(vendor_is_tax_agency_raw) = 'TRUE' THEN TRUE ELSE FALSE END) THEN 'Special Entity'
        ELSE 'Standard Vendor'
    END as vendor_complexity,
    
    -- Data quality flags
    (TRIM(COALESCE(legal_name, '')) <> '') as has_legal_name,
    (TRIM(COALESCE(note, '')) <> '') as has_notes,
    (TRIM(COALESCE(tax_registration_id, '')) <> '') as has_tax_id,
    (TRIM(COALESCE(parent_account, '')) <> '') as has_parent_account,
    (lead_time_days_raw ~ '^[0-9]+$') as has_lead_time,
    
    -- Data completeness score (0-100)
    (
        CASE WHEN vendor_code <> '' THEN 15 ELSE 0 END +
        CASE WHEN vendor_name <> '' THEN 15 ELSE 0 END +
        CASE WHEN TRIM(COALESCE(legal_name, '')) <> '' THEN 15 ELSE 0 END +
        CASE WHEN status_raw <> '' THEN 15 ELSE 0 END +
        CASE WHEN TRIM(COALESCE(vendor_class, '')) <> '' THEN 10 ELSE 0 END +
        CASE WHEN TRIM(COALESCE(terms, '')) <> '' THEN 10 ELSE 0 END +
        CASE WHEN TRIM(COALESCE(tax_zone, '')) <> '' THEN 10 ELSE 0 END +
        CASE WHEN TRIM(COALESCE(payment_method, '')) <> '' THEN 10 ELSE 0 END
    ) as data_completeness_score,
    
    extracted_at,
    source_system

FROM {{ ref('vendors_raw') }}
ORDER BY created_datetime DESC, vendor_code