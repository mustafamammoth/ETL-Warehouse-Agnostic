-- Raw sales orders data with basic filtering and renaming
{{ config(materialized='view') }}

SELECT 
    id as order_guid,
    "OrderNbr"::TEXT as order_number,
    "CustomerID"::TEXT as customer_id,
    "CustomerOrder"::TEXT as customer_order,
    "OrderType"::TEXT as order_type,
    "Status"::TEXT as status,
    "OrderTotal"::TEXT as order_total_raw,
    "TaxTotal"::TEXT as tax_total_raw,
    "ControlTotal"::TEXT as control_total_raw,
    "OrderedQty"::TEXT as ordered_qty_raw,
    "Date"::TEXT as order_date_raw,
    "RequestedOn"::TEXT as requested_date_raw,
    "EffectiveDate"::TEXT as effective_date_raw,
    "CreatedDate"::TEXT as created_date_raw,
    "LastModified"::TEXT as last_modified_raw,
    "Description"::TEXT as description,
    "Branch"::TEXT as branch,
    "LocationID"::TEXT as location_id,
    "CurrencyID"::TEXT as currency_id,
    "CurrencyRate"::TEXT as currency_rate_raw,
    "PaymentMethod"::TEXT as payment_method,
    "PaymentRef"::TEXT as payment_ref,
    "ShipVia"::TEXT as ship_via,
    "Approved"::TEXT as approved_raw,
    "CreditHold"::TEXT as credit_hold_raw,
    "Hold"::TEXT as hold_raw,
    "IsTaxValid"::TEXT as is_tax_valid_raw,
    "WillCall"::TEXT as will_call_raw,
    "BillToAddressOverride"::TEXT as bill_to_address_override_raw,
    "ShipToAddressOverride"::TEXT as ship_to_address_override_raw,
    note::TEXT,
    custom::TEXT,
    "_links"::TEXT as source_links,
    "_extracted_at" as extracted_at,
    "_source_system" as source_system,
    "_endpoint" as endpoint

FROM {{ source('acumatica_raw', 'raw_sales_orders') }}
WHERE "OrderNbr" IS NOT NULL