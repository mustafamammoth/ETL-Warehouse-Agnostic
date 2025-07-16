-- Cleaned purchase orders data (direct from raw to business-ready)
-- dbt/models/curated/acumatica/purchase_orders.sql

{{ config(materialized='table') }}

SELECT
    -- Core identifiers
    purchase_order_id,
    row_number::INTEGER as row_number,
    TRIM(order_number) as order_number,
    TRIM(vendor_id) as vendor_id,
    TRIM(COALESCE(vendor_ref, '')) as vendor_ref,
    TRIM(COALESCE(owner, '')) as owner,
    
    -- Financial amounts
    control_total_raw::DECIMAL(15,2) as control_total,
    line_total_raw::DECIMAL(15,2) as line_total,
    order_total_raw::DECIMAL(15,2) as order_total,
    tax_total_raw::DECIMAL(15,2) as tax_total,
    
    -- Currency information
    TRIM(COALESCE(currency_id, '')) as currency_id,
    TRIM(COALESCE(base_currency_id, '')) as base_currency_id,
    CASE WHEN currency_rate_raw ~ '^[0-9]+\.?[0-9]*$' 
         THEN currency_rate_raw::DECIMAL(10,6) END as currency_rate,
    CASE WHEN currency_reciprocal_rate_raw ~ '^[0-9]+\.?[0-9]*$' 
         THEN currency_reciprocal_rate_raw::DECIMAL(10,6) END as currency_reciprocal_rate,
    
    -- Dates
    date_raw::DATE as order_date,
    promised_on_raw::DATE as promised_date,
    currency_effective_date_raw::DATE as currency_effective_date,
    last_modified_datetime_raw::TIMESTAMP as last_modified_datetime,
    
    -- Status and flags
    CASE WHEN UPPER(hold_raw) = 'TRUE' THEN TRUE ELSE FALSE END as is_on_hold,
    CASE WHEN UPPER(is_tax_valid_raw) = 'TRUE' THEN TRUE ELSE FALSE END as is_tax_valid,
    
    -- Order details
    TRIM(COALESCE(description, '')) as description,
    TRIM(COALESCE(note, '')) as note,
    TRIM(status_raw) as status,
    TRIM(type_raw) as order_type,
    TRIM(COALESCE(terms, '')) as payment_terms,
    
    -- Organizational
    TRIM(COALESCE(branch, '')) as branch,
    TRIM(COALESCE(location, '')) as location,
    TRIM(COALESCE(vendor_tax_zone, '')) as vendor_tax_zone,
    TRIM(COALESCE(currency_rate_type_id, '')) as currency_rate_type_id,
    
    -- Date analysis
    EXTRACT(YEAR FROM date_raw::DATE) as order_year,
    EXTRACT(MONTH FROM date_raw::DATE) as order_month,
    EXTRACT(DOW FROM date_raw::DATE) as order_day_of_week,
    
    -- Days calculations
    CASE WHEN promised_on_raw IS NOT NULL AND date_raw IS NOT NULL THEN
         (promised_on_raw::DATE - date_raw::DATE)
    END as days_to_promise,
    
    CASE WHEN promised_on_raw IS NOT NULL THEN
         (CURRENT_DATE - promised_on_raw::DATE)
    END as days_since_promise,
    
    -- Business categorizations
    CASE 
        WHEN TRIM(status_raw) = 'Completed' THEN 'Completed'
        WHEN TRIM(status_raw) = 'Closed' THEN 'Closed'
        WHEN TRIM(status_raw) = 'Open' THEN 'Open'
        WHEN TRIM(status_raw) = 'Cancelled' THEN 'Cancelled'
        ELSE 'Other'
    END as status_category,
    
    CASE 
        WHEN TRIM(type_raw) = 'Normal' THEN 'Normal Purchase'
        WHEN TRIM(type_raw) = 'Drop-Ship' THEN 'Drop Ship'
        WHEN TRIM(type_raw) = 'Blanket' THEN 'Blanket Order'
        ELSE 'Other Type'
    END as order_type_category,
    
    CASE
        WHEN order_total_raw::DECIMAL(15,2) = 0 THEN 'Zero Amount'
        WHEN order_total_raw::DECIMAL(15,2) < 1000 THEN 'Small (< $1K)'
        WHEN order_total_raw::DECIMAL(15,2) < 10000 THEN 'Medium ($1K-$10K)'
        WHEN order_total_raw::DECIMAL(15,2) < 100000 THEN 'Large ($10K-$100K)'
        ELSE 'Very Large ($100K+)'
    END as order_size_category,
    
    -- Payment terms analysis
    CASE 
        WHEN TRIM(COALESCE(terms, '')) = 'COD' THEN 'Cash on Delivery'
        WHEN TRIM(COALESCE(terms, '')) = 'NET30' THEN 'Net 30 Days'
        WHEN TRIM(COALESCE(terms, '')) = 'NET15' THEN 'Net 15 Days'
        WHEN TRIM(COALESCE(terms, '')) = 'NET60' THEN 'Net 60 Days'
        WHEN TRIM(COALESCE(terms, '')) = '' THEN 'No Terms'
        ELSE 'Other Terms'
    END as payment_terms_category,
    
    -- Tax analysis
    CASE 
        WHEN TRIM(COALESCE(vendor_tax_zone, '')) = 'EXEMPT' THEN 'Tax Exempt'
        WHEN TRIM(COALESCE(vendor_tax_zone, '')) = 'CNNABISTAX' THEN 'Cannabis Tax'
        WHEN TRIM(COALESCE(vendor_tax_zone, '')) = '' THEN 'No Tax Zone'
        ELSE 'Other Tax Zone'
    END as tax_classification,
    
    -- Priority analysis
    CASE
        WHEN UPPER(hold_raw) = 'TRUE' THEN 'On Hold'
        WHEN TRIM(status_raw) = 'Open' AND promised_on_raw IS NOT NULL AND (CURRENT_DATE - promised_on_raw::DATE) > 0 THEN 'Overdue'
        WHEN TRIM(status_raw) = 'Open' AND promised_on_raw IS NOT NULL AND (promised_on_raw::DATE - CURRENT_DATE) <= 7 THEN 'Due Soon'
        WHEN TRIM(status_raw) = 'Open' THEN 'Active'
        ELSE 'Standard'
    END as priority_status,
    
    -- Vendor analysis
    CASE
        WHEN vendor_id LIKE 'V%' THEN 'Vendor'
        WHEN vendor_id LIKE 'C%' THEN 'Customer/Client'
        ELSE 'Other'
    END as vendor_type,
    
    -- Product category analysis (based on description)
    CASE
        WHEN UPPER(description) LIKE '%CART%' THEN 'Carts'
        WHEN UPPER(description) LIKE '%PACKAGING%' THEN 'Packaging'
        WHEN UPPER(description) LIKE '%LABEL%' THEN 'Labels'
        WHEN UPPER(description) LIKE '%TUBE%' OR UPPER(description) LIKE '%GLASS%' THEN 'Glass/Tubes'
        WHEN UPPER(description) LIKE '%BAG%' OR UPPER(description) LIKE '%MYLAR%' THEN 'Bags/Mylar'
        WHEN TRIM(COALESCE(description, '')) = '' THEN 'No Description'
        ELSE 'Other Products'
    END as product_category,
    
    -- Currency analysis
    CASE 
        WHEN TRIM(COALESCE(currency_id, '')) = 'USD' THEN 'US Dollar'
        WHEN TRIM(COALESCE(currency_id, '')) = '' THEN 'No Currency'
        ELSE 'Other Currency'
    END as currency_type,
    
    -- Data quality flags
    (TRIM(COALESCE(description, '')) <> '') as has_description,
    (TRIM(COALESCE(note, '')) <> '') as has_note,
    (TRIM(COALESCE(vendor_ref, '')) <> '') as has_vendor_ref,
    (promised_on_raw IS NOT NULL) as has_promised_date,
    (tax_total_raw::DECIMAL(15,2) > 0) as has_tax,
    (order_total_raw::DECIMAL(15,2) = line_total_raw::DECIMAL(15,2) + tax_total_raw::DECIMAL(15,2)) as totals_match,
    
    -- Data completeness score (0-100)
    (
        CASE WHEN vendor_id <> '' THEN 20 ELSE 0 END +
        CASE WHEN order_total_raw::DECIMAL(15,2) IS NOT NULL THEN 20 ELSE 0 END +
        CASE WHEN date_raw IS NOT NULL THEN 20 ELSE 0 END +
        CASE WHEN status_raw <> '' THEN 20 ELSE 0 END +
        CASE WHEN TRIM(COALESCE(description, '')) <> '' THEN 10 ELSE 0 END +
        CASE WHEN promised_on_raw IS NOT NULL THEN 10 ELSE 0 END
    ) as data_completeness_score,
    
    extracted_at,
    source_system

FROM {{ ref('purchase_orders_raw') }}
ORDER BY order_date DESC, order_number