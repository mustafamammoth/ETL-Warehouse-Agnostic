-- Raw sales invoices data with basic filtering and renaming
{{ config(materialized='view') }}

SELECT 
    id as invoice_guid,
    "rowNumber"::TEXT as row_number,
    note::TEXT,
    "Amount"::TEXT as amount_raw,
    "Balance"::TEXT as balance_raw,
    "CashDiscount"::TEXT as cash_discount_raw,
    "CreditHold"::TEXT as credit_hold_raw,
    "Currency"::TEXT as currency_id,
    "CustomerID"::TEXT as customer_id,
    "CustomerOrder"::TEXT as customer_order,
    "Date"::TEXT as invoice_date_raw,
    "Description"::TEXT as description,
    "DetailTotal"::TEXT as detail_total_raw,
    "DiscountTotal"::TEXT as discount_total_raw,
    "DueDate"::TEXT as due_date_raw,
    "FreightPrice"::TEXT as freight_price_raw,
    "Hold"::TEXT as hold_raw,
    "IsTaxValid"::TEXT as is_tax_valid_raw,
    "LastModifiedDateTime"::TEXT as last_modified_datetime_raw,
    "PaymentTotal"::TEXT as payment_total_raw,
    "Project"::TEXT as project,
    "ReferenceNbr"::TEXT as reference_number,
    "Status"::TEXT as status,
    "TaxTotal"::TEXT as tax_total_raw,
    "Type"::TEXT as invoice_type,
    custom::TEXT,
    "_links"::TEXT as source_links,
    "_extracted_at" as extracted_at,
    "_source_system" as source_system,
    "_endpoint" as endpoint

FROM {{ source('acumatica_raw', 'raw_sales_invoices') }}
WHERE "ReferenceNbr" IS NOT NULL