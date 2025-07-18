-- Cleaned and standardized sales orders data (ClickHouse compatible)
{{ config(materialized='table') }}

WITH order_cleaning AS (
    SELECT 
        order_guid,
        order_number,
        customer_id,
        
        -- Clean text fields
        trimBoth(coalesce(customer_order, '')) as customer_order,
        upper(trimBoth(coalesce(order_type, ''))) as order_type,
        upper(trimBoth(coalesce(status, ''))) as status,
        trimBoth(coalesce(currency_id, 'USD')) as currency_id,
        trimBoth(coalesce(branch, '')) as branch,
        trimBoth(coalesce(location_id, '')) as location_id,
        trimBoth(coalesce(ship_via, '')) as ship_via,
        trimBoth(coalesce(base_currency_id, '')) as base_currency_id,
        trimBoth(coalesce(currency_rate_type_id, '')) as currency_rate_type_id,
        trimBoth(coalesce(contact_id, '')) as contact_id,
        trimBoth(coalesce(external_ref, '')) as external_ref,
        trimBoth(coalesce(note_id, '')) as note_id,
        trimBoth(coalesce(destination_warehouse_id, '')) as destination_warehouse_id,
        trimBoth(coalesce(preferred_warehouse_id, '')) as preferred_warehouse_id,
        
        -- Clean description safely (if needed)
        trimBoth(coalesce(description, '')) as description_cleaned,
        
        -- Clean numeric fields (ClickHouse compatible - consistent decimal types)
        multiIf(
            (order_total_raw IS NOT NULL) AND (order_total_raw != '') AND (order_total_raw != 'NULL'),
            toDecimal64(order_total_raw, 2),
            toDecimal64('0.00', 2)
        ) as order_total,
        
        multiIf(
            (control_total_raw IS NOT NULL) AND (control_total_raw != '') AND (control_total_raw != 'NULL'),
            toDecimal64(control_total_raw, 2),
            toDecimal64('0.00', 2)
        ) as control_total,
        
        multiIf(
            (ordered_qty_raw IS NOT NULL) AND (ordered_qty_raw != '') AND (ordered_qty_raw != 'NULL'),
            toDecimal64(ordered_qty_raw, 2),
            toDecimal64('0.00', 2)
        ) as ordered_qty,
        
        multiIf(
            (currency_rate_raw IS NOT NULL) AND (currency_rate_raw != '') AND (currency_rate_raw != 'NULL'),
            toDecimal64(currency_rate_raw, 4),
            toDecimal64('1.0000', 4)
        ) as currency_rate,
        
        multiIf(
            (reciprocal_rate_raw IS NOT NULL) AND (reciprocal_rate_raw != '') AND (reciprocal_rate_raw != 'NULL'),
            toDecimal64(reciprocal_rate_raw, 4),
            toDecimal64('1.0000', 4)
        ) as reciprocal_rate,
        
        -- Clean boolean fields
        multiIf(upper(trimBoth(coalesce(approved_raw, 'FALSE'))) = 'TRUE', true, false) as approved,
        multiIf(upper(trimBoth(coalesce(credit_hold_raw, 'FALSE'))) = 'TRUE', true, false) as credit_hold,
        multiIf(upper(trimBoth(coalesce(hold_raw, 'FALSE'))) = 'TRUE', true, false) as hold,
        multiIf(upper(trimBoth(coalesce(is_tax_valid_raw, 'FALSE'))) = 'TRUE', true, false) as is_tax_valid,
        multiIf(upper(trimBoth(coalesce(will_call_raw, 'FALSE'))) = 'TRUE', true, false) as will_call,
        multiIf(upper(trimBoth(coalesce(bill_to_address_override_raw, 'FALSE'))) = 'TRUE', true, false) as bill_to_address_override,
        multiIf(upper(trimBoth(coalesce(ship_to_address_override_raw, 'FALSE'))) = 'TRUE', true, false) as ship_to_address_override,
        multiIf(upper(trimBoth(coalesce(bill_to_address_validated_raw, 'FALSE'))) = 'TRUE', true, false) as bill_to_address_validated,
        multiIf(upper(trimBoth(coalesce(bill_to_contact_override_raw, 'FALSE'))) = 'TRUE', true, false) as bill_to_contact_override,
        multiIf(upper(trimBoth(coalesce(ship_to_address_validated_raw, 'FALSE'))) = 'TRUE', true, false) as ship_to_address_validated,
        multiIf(upper(trimBoth(coalesce(ship_to_contact_override_raw, 'FALSE'))) = 'TRUE', true, false) as ship_to_contact_override,
        multiIf(upper(trimBoth(coalesce(disable_automatic_tax_calculation_raw, 'FALSE'))) = 'TRUE', true, false) as disable_automatic_tax_calculation,
        
        -- Clean dates (ClickHouse compatible)
        multiIf(
            (order_date_raw IS NOT NULL) AND (order_date_raw != ''),
            parseDateTime64BestEffort(order_date_raw),
            CAST(NULL AS Nullable(DateTime64))
        ) as order_date,
        
        multiIf(
            (requested_date_raw IS NOT NULL) AND (requested_date_raw != ''),
            parseDateTime64BestEffort(requested_date_raw),
            CAST(NULL AS Nullable(DateTime64))
        ) as requested_date,
        
        multiIf(
            (effective_date_raw IS NOT NULL) AND (effective_date_raw != ''),
            parseDateTime64BestEffort(effective_date_raw),
            CAST(NULL AS Nullable(DateTime64))
        ) as effective_date,
        
        multiIf(
            (created_date_raw IS NOT NULL) AND (created_date_raw != ''),
            parseDateTime64BestEffort(created_date_raw),
            CAST(NULL AS Nullable(DateTime64))
        ) as created_date,
        
        multiIf(
            (last_modified_raw IS NOT NULL) AND (last_modified_raw != ''),
            parseDateTime64BestEffort(last_modified_raw),
            CAST(NULL AS Nullable(DateTime64))
        ) as last_modified_timestamp,
        
        -- Extract simple dates for grouping
        multiIf(
            (order_date_raw IS NOT NULL) AND (order_date_raw != ''),
            toDate(parseDateTime64BestEffort(order_date_raw)),
            CAST(NULL AS Nullable(Date))
        ) as order_date_only,
        
        multiIf(
            (requested_date_raw IS NOT NULL) AND (requested_date_raw != ''),
            toDate(parseDateTime64BestEffort(requested_date_raw)),
            CAST(NULL AS Nullable(Date))
        ) as requested_date_only,
        
        -- Keep ALL bronze fields that exist (excluding problematic fields)
        row_number,
        description,
        source_links,
        extracted_at,
        source_system,
        endpoint,
        
        -- Raw fields for reference (all the _raw fields)
        order_total_raw,
        control_total_raw,
        ordered_qty_raw,
        currency_rate_raw,
        reciprocal_rate_raw,
        approved_raw,
        credit_hold_raw,
        hold_raw,
        is_tax_valid_raw,
        will_call_raw,
        bill_to_address_override_raw,
        ship_to_address_override_raw,
        bill_to_address_validated_raw,
        bill_to_contact_override_raw,
        ship_to_address_validated_raw,
        ship_to_contact_override_raw,
        disable_automatic_tax_calculation_raw,
        order_date_raw,
        requested_date_raw,
        effective_date_raw,
        created_date_raw,
        last_modified_raw

    FROM {{ ref('sales_orders_raw') }}
),

order_categorization AS (
    SELECT
        *,
        
        -- Order classification
        multiIf(
            order_type = 'SO', 'Sales Order',
            order_type = 'RC', 'Return Credit',
            order_type = 'RO', 'Return Order',
            coalesce(order_type, 'Unknown')
        ) as order_category,
        
        -- Order status analysis
        multiIf(
            status = 'COMPLETED', 'Completed',
            status = 'OPEN', 'Open',
            status = 'ON HOLD', 'On Hold',
            status = 'CANCELLED', 'Cancelled',
            status
        ) as order_status_clean,
        
        -- Calculate net amount (order total minus any calculated tax - note: no tax_total in orders)
        order_total as net_amount

    FROM order_cleaning
),

order_business_logic AS (
    SELECT
        *,
        
        -- Order processing metrics
        multiIf(
            order_category = 'Sales Order' AND order_status_clean = 'Completed', 'Fulfilled Sales',
            order_category = 'Sales Order' AND order_status_clean != 'Completed', 'Pending Sales',
            order_category = 'Return Credit', 'Customer Return',
            'Other'
        ) as order_processing_status,
        
        -- Order value categories
        multiIf(
            order_total >= toDecimal64('50000.00', 2), 'Very Large (50K+)',
            order_total >= toDecimal64('20000.00', 2), 'Large (20K-50K)',
            order_total >= toDecimal64('10000.00', 2), 'Medium (10K-20K)',
            order_total >= toDecimal64('5000.00', 2), 'Small (5K-10K)',
            order_total > toDecimal64('0.00', 2), 'Micro (<5K)',
            'Zero/Credit'
        ) as order_size_category,
        
        -- Delivery analysis
        multiIf(
            requested_date_only IS NOT NULL AND order_date_only IS NOT NULL,
            dateDiff('day', order_date_only, requested_date_only),
            CAST(NULL AS Nullable(Int32))
        ) as lead_time_days,
        
        multiIf(
            will_call = true, 'Will Call',
            ship_via IS NOT NULL AND ship_via != '', 'Delivery',
            'Unknown'
        ) as fulfillment_method,
        
        -- Order timing analysis (ClickHouse compatible)
        multiIf(
            order_date_only IS NOT NULL,
            toYear(order_date_only),
            CAST(NULL AS Nullable(UInt16))
        ) as order_year,
        
        multiIf(
            order_date_only IS NOT NULL,
            toMonth(order_date_only),
            CAST(NULL AS Nullable(UInt8))
        ) as order_month,
        
        multiIf(
            order_date_only IS NOT NULL,
            toQuarter(order_date_only),
            CAST(NULL AS Nullable(UInt8))
        ) as order_quarter,
        
        multiIf(
            order_date_only IS NOT NULL,
            toDayOfWeek(order_date_only),
            CAST(NULL AS Nullable(UInt8))
        ) as order_day_of_week,
        
        -- Quality and control flags
        multiIf(
            customer_id IS NULL OR customer_id = '', 'Missing Customer',
            order_date_only IS NULL, 'Missing Order Date',
            order_total = toDecimal64('0.00', 2) AND order_category = 'Sales Order', 'Zero Amount Order',
            ordered_qty = toDecimal64('0.00', 2) AND order_category = 'Sales Order', 'Zero Quantity Order',
            hold = true, 'On Hold',
            credit_hold = true, 'Credit Hold',
            'Valid'
        ) as data_quality_flag,
        
        -- Average unit price calculation
        multiIf(
            ordered_qty > toDecimal64('0.00', 2),
            round(net_amount / ordered_qty, 2),
            toDecimal64('0.00', 2)
        ) as avg_unit_price

    FROM order_categorization
)

SELECT * 
FROM order_business_logic
ORDER BY order_date_only DESC, order_number