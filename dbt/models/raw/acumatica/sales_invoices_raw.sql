-- Raw sales invoices data with basic filtering and renaming
{{ config(materialized='view') }}

SELECT 
    id as invoice_guid,
    CAST("ReferenceNbr" AS TEXT) as invoice_number,  -- Cast to text to handle mixed types
    CAST("CustomerID" AS TEXT) as customer_id,
    CAST("CustomerOrder" AS TEXT) as customer_order,
    CAST("Type" AS TEXT) as invoice_type,
    CAST("Status" AS TEXT) as status,
    CAST("Amount" AS TEXT) as amount_raw,
    CAST("Balance" AS TEXT) as balance_raw,
    CAST("DetailTotal" AS TEXT) as detail_total_raw,
    CAST("TaxTotal" AS TEXT) as tax_total_raw,
    CAST("DiscountTotal" AS TEXT) as discount_total_raw,
    CAST("FreightPrice" AS TEXT) as freight_price_raw,
    CAST("PaymentTotal" AS TEXT) as payment_total_raw,
    CAST("CashDiscount" AS TEXT) as cash_discount_raw,
    CAST("Currency" AS TEXT) as currency,
    CAST("Date" AS TEXT) as invoice_date_raw,
    CAST("DueDate" AS TEXT) as due_date_raw,
    CAST("LastModifiedDateTime" AS TEXT) as last_modified_datetime_raw,
    CAST("Description" AS TEXT) as description,
    CAST("Project" AS TEXT) as project,
    CAST("CreditHold" AS TEXT) as credit_hold_raw,
    CAST("Hold" AS TEXT) as hold_raw,
    CAST("IsTaxValid" AS TEXT) as is_tax_valid_raw,
    note,
    custom,
    "_links" as source_links,
    "_extracted_at" as extracted_at,
    "_source_system" as source_system,
    "_endpoint" as endpoint

FROM {{ source('acumatica_raw', 'raw_sales_invoices') }}
WHERE CAST("ReferenceNbr" AS TEXT) IS NOT NULL  -- Cast in WHERE clause too
  AND CAST("ReferenceNbr" AS TEXT) != ''