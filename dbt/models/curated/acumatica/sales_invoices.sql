-- Cleaned and standardized sales invoices data
{{ config(materialized='table') }}

WITH invoice_cleaning AS (
    SELECT 
        invoice_guid,
        invoice_number,
        customer_id,
        
        -- Clean text fields
        TRIM(COALESCE(customer_order, '')) as customer_order,
        UPPER(TRIM(COALESCE(invoice_type, ''))) as invoice_type,
        UPPER(TRIM(COALESCE(status, ''))) as status,
        TRIM(COALESCE(currency, 'USD')) as currency,
        TRIM(COALESCE(project, '')) as project,
        
        -- Clean description
        CASE 
            WHEN description IS NOT NULL AND description != '' THEN 
                REPLACE(REPLACE(TRIM(description), CHR(10), ' '), CHR(13), ' ')
            ELSE NULL
        END as description_cleaned,
        
        -- Clean numeric fields (convert TEXT to proper decimals)
        CASE 
            WHEN amount_raw IS NOT NULL AND amount_raw != '' AND amount_raw != 'NULL'
            THEN CAST(amount_raw AS DECIMAL(15,2))
            ELSE 0.00
        END as amount,
        
        CASE 
            WHEN balance_raw IS NOT NULL AND balance_raw != '' AND balance_raw != 'NULL'
            THEN CAST(balance_raw AS DECIMAL(15,2))
            ELSE 0.00
        END as balance,
        
        CASE 
            WHEN detail_total_raw IS NOT NULL AND detail_total_raw != '' AND detail_total_raw != 'NULL'
            THEN CAST(detail_total_raw AS DECIMAL(15,2))
            ELSE 0.00
        END as detail_total,
        
        CASE 
            WHEN tax_total_raw IS NOT NULL AND tax_total_raw != '' AND tax_total_raw != 'NULL'
            THEN CAST(tax_total_raw AS DECIMAL(15,2))
            ELSE 0.00
        END as tax_total,
        
        CASE 
            WHEN discount_total_raw IS NOT NULL AND discount_total_raw != '' AND discount_total_raw != 'NULL'
            THEN CAST(discount_total_raw AS DECIMAL(15,2))
            ELSE 0.00
        END as discount_total,
        
        CASE 
            WHEN freight_price_raw IS NOT NULL AND freight_price_raw != '' AND freight_price_raw != 'NULL'
            THEN CAST(freight_price_raw AS DECIMAL(15,2))
            ELSE 0.00
        END as freight_price,
        
        CASE 
            WHEN payment_total_raw IS NOT NULL AND payment_total_raw != '' AND payment_total_raw != 'NULL'
            THEN CAST(payment_total_raw AS DECIMAL(15,2))
            ELSE 0.00
        END as payment_total,
        
        CASE 
            WHEN cash_discount_raw IS NOT NULL AND cash_discount_raw != '' AND cash_discount_raw != 'NULL'
            THEN CAST(cash_discount_raw AS DECIMAL(15,2))
            ELSE 0.00
        END as cash_discount,
        
        -- Clean boolean fields  
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
        
        -- Clean dates (keep timezone info)
        CASE 
            WHEN invoice_date_raw IS NOT NULL AND invoice_date_raw != '' 
            THEN CAST(invoice_date_raw AS TIMESTAMP)
            ELSE NULL
        END as invoice_date,
        
        CASE 
            WHEN due_date_raw IS NOT NULL AND due_date_raw != '' 
            THEN CAST(due_date_raw AS TIMESTAMP)
            ELSE NULL
        END as due_date,
        
        CASE 
            WHEN last_modified_datetime_raw IS NOT NULL AND last_modified_datetime_raw != '' 
            THEN CAST(last_modified_datetime_raw AS TIMESTAMP)
            ELSE NULL
        END as last_modified_timestamp,
        
        -- Extract simple dates for grouping
        CASE 
            WHEN invoice_date_raw IS NOT NULL AND invoice_date_raw != '' 
            THEN DATE(CAST(invoice_date_raw AS TIMESTAMP))
            ELSE NULL
        END as invoice_date_only,
        
        CASE 
            WHEN due_date_raw IS NOT NULL AND due_date_raw != '' 
            THEN DATE(CAST(due_date_raw AS TIMESTAMP))
            ELSE NULL
        END as due_date_only,
        
        source_links,
        extracted_at,
        source_system

    FROM {{ ref('sales_invoices_raw') }}
),

invoice_categorization AS (
    SELECT
        *,
        
        -- Invoice classification (create invoice_category here)
        CASE 
            WHEN invoice_type = 'CREDIT MEMO' THEN 'Credit Memo'
            WHEN invoice_type = 'INVOICE' OR invoice_type = '' THEN 'Regular Invoice'
            ELSE invoice_type
        END as invoice_category,
        
        -- Payment status
        CASE 
            WHEN balance = 0 THEN 'Paid'
            WHEN status = 'CLOSED' AND balance > 0 THEN 'Closed with Balance'
            WHEN balance > 0 THEN 'Outstanding'
            ELSE 'Unknown'
        END as payment_status,
        
        -- Calculate net amount (amount minus tax)
        (amount - tax_total) as net_amount

    FROM invoice_cleaning
),

invoice_business_logic AS (
    SELECT
        *,
        
        -- Outstanding balance (for regular invoices) - NOW invoice_category exists
        CASE 
            WHEN invoice_category = 'Regular Invoice' THEN balance
            ELSE 0.00
        END as outstanding_balance,
        
        -- Days outstanding (from invoice date to today)
        CASE 
            WHEN invoice_date_only IS NOT NULL AND invoice_category = 'Regular Invoice' AND balance > 0
            THEN (CURRENT_DATE - invoice_date_only)
            ELSE NULL
        END as days_outstanding,
        
        -- Days until due (or overdue)
        CASE 
            WHEN due_date_only IS NOT NULL AND invoice_category = 'Regular Invoice' AND balance > 0
            THEN (due_date_only - CURRENT_DATE)
            ELSE NULL
        END as days_until_due,
        
        -- Aging buckets for AR analysis
        CASE 
            WHEN invoice_category != 'Regular Invoice' OR balance = 0 THEN 'Not Applicable'
            WHEN due_date_only IS NULL THEN 'No Due Date'
            WHEN due_date_only >= CURRENT_DATE THEN 'Current'
            WHEN (CURRENT_DATE - due_date_only) <= 30 THEN '1-30 Days Past Due'
            WHEN (CURRENT_DATE - due_date_only) <= 60 THEN '31-60 Days Past Due'
            WHEN (CURRENT_DATE - due_date_only) <= 90 THEN '61-90 Days Past Due'
            ELSE '90+ Days Past Due'
        END as aging_bucket,
        
        -- Invoice size categories
        CASE 
            WHEN amount >= 10000 THEN 'Large (10K+)'
            WHEN amount >= 5000 THEN 'Medium (5K-10K)'
            WHEN amount >= 1000 THEN 'Small (1K-5K)'
            WHEN amount > 0 THEN 'Micro (<1K)'
            ELSE 'Zero/Credit'
        END as invoice_size_category,
        
        -- Data quality flags
        CASE 
            WHEN customer_id IS NULL OR customer_id = '' THEN 'Missing Customer'
            WHEN invoice_date_only IS NULL THEN 'Missing Invoice Date'
            WHEN amount = 0 AND invoice_category = 'Regular Invoice' THEN 'Zero Amount Invoice'
            WHEN due_date_only IS NULL AND invoice_category = 'Regular Invoice' THEN 'Missing Due Date'
            ELSE 'Valid'
        END as data_quality_flag,
        
        -- Extract year/month for reporting
        CASE 
            WHEN invoice_date_only IS NOT NULL 
            THEN EXTRACT(YEAR FROM invoice_date_only)
            ELSE NULL
        END as invoice_year,
        
        CASE 
            WHEN invoice_date_only IS NOT NULL 
            THEN EXTRACT(MONTH FROM invoice_date_only)
            ELSE NULL
        END as invoice_month,
        
        CASE 
            WHEN invoice_date_only IS NOT NULL 
            THEN EXTRACT(QUARTER FROM invoice_date_only)
            ELSE NULL
        END as invoice_quarter,
        
        -- Calculate payment percentage
        CASE 
            WHEN amount > 0 
            THEN ROUND((payment_total / amount) * 100, 2)
            ELSE 0.00
        END as payment_percentage

    FROM invoice_categorization  -- Reference the intermediate CTE
)

SELECT * 
FROM invoice_business_logic
ORDER BY invoice_date_only DESC, invoice_number