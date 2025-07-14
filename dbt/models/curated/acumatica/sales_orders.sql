-- Cleaned and standardized sales orders data
{{ config(materialized='table') }}

WITH order_cleaning AS (
    SELECT 
        order_guid,
        order_number,
        customer_id,
        
        -- Clean text fields
        TRIM(COALESCE(customer_order, '')) as customer_order,
        UPPER(TRIM(COALESCE(order_type, ''))) as order_type,
        UPPER(TRIM(COALESCE(status, ''))) as status,
        TRIM(COALESCE(currency_id, 'USD')) as currency_id,
        TRIM(COALESCE(branch, '')) as branch,
        TRIM(COALESCE(location_id, '')) as location_id,
        TRIM(COALESCE(payment_method, '')) as payment_method,
        TRIM(COALESCE(payment_ref, '')) as payment_ref,
        TRIM(COALESCE(ship_via, '')) as ship_via,
        
        -- Clean description and notes
        CASE 
            WHEN description IS NOT NULL AND description != '' THEN 
                REPLACE(REPLACE(TRIM(description), CHR(10), ' '), CHR(13), ' ')
            ELSE NULL
        END as description_cleaned,
        
        CASE 
            WHEN note IS NOT NULL AND note != '' THEN 
                REPLACE(REPLACE(TRIM(note), CHR(10), ' '), CHR(13), ' ')
            ELSE NULL
        END as notes_cleaned,
        
        -- Clean numeric fields (convert TEXT to proper decimals)
        CASE 
            WHEN order_total_raw IS NOT NULL AND order_total_raw != '' AND order_total_raw != 'NULL'
            THEN CAST(order_total_raw AS DECIMAL(15,2))
            ELSE 0.00
        END as order_total,
        
        CASE 
            WHEN tax_total_raw IS NOT NULL AND tax_total_raw != '' AND tax_total_raw != 'NULL'
            THEN CAST(tax_total_raw AS DECIMAL(15,2))
            ELSE 0.00
        END as tax_total,
        
        CASE 
            WHEN control_total_raw IS NOT NULL AND control_total_raw != '' AND control_total_raw != 'NULL'
            THEN CAST(control_total_raw AS DECIMAL(15,2))
            ELSE 0.00
        END as control_total,
        
        CASE 
            WHEN ordered_qty_raw IS NOT NULL AND ordered_qty_raw != '' AND ordered_qty_raw != 'NULL'
            THEN CAST(ordered_qty_raw AS DECIMAL(15,2))
            ELSE 0.00
        END as ordered_qty,
        
        CASE 
            WHEN currency_rate_raw IS NOT NULL AND currency_rate_raw != '' AND currency_rate_raw != 'NULL'
            THEN CAST(currency_rate_raw AS DECIMAL(10,4))
            ELSE 1.0000
        END as currency_rate,
        
        -- Clean boolean fields
        CASE 
            WHEN UPPER(TRIM(COALESCE(approved_raw, 'FALSE'))) = 'TRUE' THEN TRUE
            ELSE FALSE
        END as approved,
        
        CASE 
            WHEN UPPER(TRIM(COALESCE(credit_hold_raw, 'FALSE'))) = 'TRUE' THEN TRUE
            ELSE FALSE
        END as credit_hold,
        
        CASE 
            WHEN UPPER(TRIM(COALESCE(hold_raw, 'FALSE'))) = 'TRUE' THEN TRUE
            ELSE FALSE
        END as hold,
        
        CASE 
            WHEN UPPER(TRIM(COALESCE(is_tax_valid_raw, 'FALSE'))) = 'TRUE' THEN TRUE
            ELSE FALSE
        END as is_tax_valid,
        
        CASE 
            WHEN UPPER(TRIM(COALESCE(will_call_raw, 'FALSE'))) = 'TRUE' THEN TRUE
            ELSE FALSE
        END as will_call,
        
        CASE 
            WHEN UPPER(TRIM(COALESCE(bill_to_address_override_raw, 'FALSE'))) = 'TRUE' THEN TRUE
            ELSE FALSE
        END as bill_to_address_override,
        
        CASE 
            WHEN UPPER(TRIM(COALESCE(ship_to_address_override_raw, 'FALSE'))) = 'TRUE' THEN TRUE
            ELSE FALSE
        END as ship_to_address_override,
        
        -- Clean dates (keep timezone info)
        CASE 
            WHEN order_date_raw IS NOT NULL AND order_date_raw != '' 
            THEN CAST(order_date_raw AS TIMESTAMP)
            ELSE NULL
        END as order_date,
        
        CASE 
            WHEN requested_date_raw IS NOT NULL AND requested_date_raw != '' 
            THEN CAST(requested_date_raw AS TIMESTAMP)
            ELSE NULL
        END as requested_date,
        
        CASE 
            WHEN effective_date_raw IS NOT NULL AND effective_date_raw != '' 
            THEN CAST(effective_date_raw AS TIMESTAMP)
            ELSE NULL
        END as effective_date,
        
        CASE 
            WHEN created_date_raw IS NOT NULL AND created_date_raw != '' 
            THEN CAST(created_date_raw AS TIMESTAMP)
            ELSE NULL
        END as created_date,
        
        CASE 
            WHEN last_modified_raw IS NOT NULL AND last_modified_raw != '' 
            THEN CAST(last_modified_raw AS TIMESTAMP)
            ELSE NULL
        END as last_modified_timestamp,
        
        -- Extract simple dates for grouping
        CASE 
            WHEN order_date_raw IS NOT NULL AND order_date_raw != '' 
            THEN DATE(CAST(order_date_raw AS TIMESTAMP))
            ELSE NULL
        END as order_date_only,
        
        CASE 
            WHEN requested_date_raw IS NOT NULL AND requested_date_raw != '' 
            THEN DATE(CAST(requested_date_raw AS TIMESTAMP))
            ELSE NULL
        END as requested_date_only,
        
        source_links,
        extracted_at,
        source_system

    FROM {{ ref('sales_orders_raw') }}
),

order_categorization AS (
    SELECT
        *,
        
        -- Order classification (create order_category here)
        CASE 
            WHEN order_type = 'SO' THEN 'Sales Order'
            WHEN order_type = 'RC' THEN 'Return Credit'
            WHEN order_type = 'RO' THEN 'Return Order'
            ELSE COALESCE(order_type, 'Unknown')
        END as order_category,
        
        -- Order status analysis
        CASE 
            WHEN status = 'COMPLETED' THEN 'Completed'
            WHEN status = 'OPEN' THEN 'Open'
            WHEN status = 'ON HOLD' THEN 'On Hold'
            WHEN status = 'CANCELLED' THEN 'Cancelled'
            ELSE status
        END as order_status_clean,
        
        -- Calculate net amount (order total minus tax)
        (order_total - tax_total) as net_amount

    FROM order_cleaning
),

order_business_logic AS (
    SELECT
        *,
        
        -- Order processing metrics - NOW order_category exists
        CASE 
            WHEN order_category = 'Sales Order' AND order_status_clean = 'Completed' THEN 'Fulfilled Sales'
            WHEN order_category = 'Sales Order' AND order_status_clean != 'Completed' THEN 'Pending Sales'
            WHEN order_category = 'Return Credit' THEN 'Customer Return'
            ELSE 'Other'
        END as order_processing_status,
        
        -- Order value categories
        CASE 
            WHEN order_total >= 50000 THEN 'Very Large (50K+)'
            WHEN order_total >= 20000 THEN 'Large (20K-50K)'
            WHEN order_total >= 10000 THEN 'Medium (10K-20K)'
            WHEN order_total >= 5000 THEN 'Small (5K-10K)'
            WHEN order_total > 0 THEN 'Micro (<5K)'
            ELSE 'Zero/Credit'
        END as order_size_category,
        
        -- Delivery analysis
        CASE 
            WHEN requested_date_only IS NOT NULL AND order_date_only IS NOT NULL
            THEN (requested_date_only - order_date_only)
            ELSE NULL
        END as lead_time_days,
        
        CASE 
            WHEN will_call = TRUE THEN 'Will Call'
            WHEN ship_via IS NOT NULL AND ship_via != '' THEN 'Delivery'
            ELSE 'Unknown'
        END as fulfillment_method,
        
        -- Order timing analysis
        CASE 
            WHEN order_date_only IS NOT NULL 
            THEN EXTRACT(YEAR FROM order_date_only)
            ELSE NULL
        END as order_year,
        
        CASE 
            WHEN order_date_only IS NOT NULL 
            THEN EXTRACT(MONTH FROM order_date_only)
            ELSE NULL
        END as order_month,
        
        CASE 
            WHEN order_date_only IS NOT NULL 
            THEN EXTRACT(QUARTER FROM order_date_only)
            ELSE NULL
        END as order_quarter,
        
        CASE 
            WHEN order_date_only IS NOT NULL 
            THEN EXTRACT(DOW FROM order_date_only)
            ELSE NULL
        END as order_day_of_week,
        
        -- Quality and control flags
        CASE 
            WHEN customer_id IS NULL OR customer_id = '' THEN 'Missing Customer'
            WHEN order_date_only IS NULL THEN 'Missing Order Date'
            WHEN order_total = 0 AND order_category = 'Sales Order' THEN 'Zero Amount Order'
            WHEN ordered_qty = 0 AND order_category = 'Sales Order' THEN 'Zero Quantity Order'
            WHEN hold = TRUE THEN 'On Hold'
            WHEN credit_hold = TRUE THEN 'Credit Hold'
            ELSE 'Valid'
        END as data_quality_flag,
        
        -- Average unit price calculation
        CASE 
            WHEN ordered_qty > 0 
            THEN ROUND(net_amount / ordered_qty, 2)
            ELSE 0.00
        END as avg_unit_price,
        
        -- Tax rate calculation
        CASE 
            WHEN order_total > 0 
            THEN ROUND((tax_total / order_total) * 100, 2)
            ELSE 0.00
        END as tax_rate_percent

    FROM order_categorization
)

SELECT * 
FROM order_business_logic
ORDER BY order_date_only DESC, order_number