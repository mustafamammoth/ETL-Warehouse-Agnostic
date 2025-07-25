-- models/bronze/sales_orders_raw.sql
-- Raw sales orders data with basic filtering and renaming (bronze layer, append-only source)
{{ config(materialized='view') }}

SELECT 
    id as order_guid,
    "rowNumber"::TEXT as row_number,
    note::TEXT,
    "Approved"::TEXT as approved_raw,
    "BaseCurrencyID"::TEXT as base_currency_id,
    "BillToAddressOverride"::TEXT as bill_to_address_override_raw,
    "BillToAddressValidated"::TEXT as bill_to_address_validated_raw,
    "BillToContactOverride"::TEXT as bill_to_contact_override_raw,
    "Branch"::TEXT as branch,
    "ContactID"::TEXT as contact_id,
    "ControlTotal"::TEXT as control_total_raw,
    "CreatedDate"::TEXT as created_date_raw,
    "CreditHold"::TEXT as credit_hold_raw,
    "CurrencyID"::TEXT as currency_id,
    "CurrencyRate"::TEXT as currency_rate_raw,
    "CurrencyRateTypeID"::TEXT as currency_rate_type_id,
    "CustomerID"::TEXT as customer_id,
    "CustomerOrder"::TEXT as customer_order,
    "Date"::TEXT as order_date_raw,
    "Description"::TEXT as description,
    "DestinationWarehouseID"::TEXT as destination_warehouse_id,
    "DisableAutomaticTaxCalculation"::TEXT as disable_automatic_tax_calculation_raw,
    "EffectiveDate"::TEXT as effective_date_raw,
    "ExternalRef"::TEXT as external_ref,
    "Hold"::TEXT as hold_raw,
    "IsTaxValid"::TEXT as is_tax_valid_raw,
    "LastModified"::TEXT as last_modified_raw,
    "LocationID"::TEXT as location_id,
    "NoteID"::TEXT as note_id,
    "OrderedQty"::TEXT as ordered_qty_raw,
    "OrderNbr"::TEXT as order_number,
    "OrderTotal"::TEXT as order_total_raw,
    "OrderType"::TEXT as order_type,
    "PreferredWarehouseID"::TEXT as preferred_warehouse_id,
    "ReciprocalRate"::TEXT as reciprocal_rate_raw,
    "RequestedOn"::TEXT as requested_date_raw,
    "ShipToAddressOverride"::TEXT as ship_to_address_override_raw,
    "ShipToAddressValidated"::TEXT as ship_to_address_validated_raw,
    "ShipToContactOverride"::TEXT as ship_to_contact_override_raw,
    "ShipVia"::TEXT as ship_via,
    "Status"::TEXT as status,
    "WillCall"::TEXT as will_call_raw,
    custom::TEXT,
    "_links"::TEXT as source_links,
    "_extracted_at" as extracted_at,
    "_source_system" as source_system,
    "_endpoint" as endpoint
FROM {{ source('acumatica_raw', 'raw_sales_orders') }}
WHERE "OrderNbr" IS NOT NULL
