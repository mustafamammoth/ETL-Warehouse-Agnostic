-- Cleaned and standardized customer data (silver layer) - ClickHouse compatible
{{ config(materialized='table') }}

WITH customer_cleaning AS (
    SELECT 
        customer_guid,
        customer_id,
        customer_name,
        
        -- Clean text fields
        trimBoth(coalesce(account_ref, '')) as account_ref,
        trimBoth(coalesce(customer_category, '')) as customer_category,
        upper(trimBoth(coalesce(customer_class, ''))) as customer_class,
        upper(trimBoth(coalesce(status, ''))) as status,
        trimBoth(coalesce(currency_id, 'USD')) as currency_id,
        trimBoth(coalesce(currency_rate_type, '')) as currency_rate_type,
        trimBoth(coalesce(location_name, '')) as location_name,
        trimBoth(coalesce(fob_point, '')) as fob_point,
        trimBoth(coalesce(parent_record, '')) as parent_record,
        trimBoth(coalesce(price_class_id, '')) as price_class_id,
        trimBoth(coalesce(shipping_branch, '')) as shipping_branch,
        trimBoth(coalesce(shipping_rule, '')) as shipping_rule,
        trimBoth(coalesce(shipping_terms, '')) as shipping_terms,
        trimBoth(coalesce(shipping_zone_id, '')) as shipping_zone_id,
        trimBoth(coalesce(ship_via, '')) as ship_via,
        trimBoth(coalesce(statement_cycle_id, '')) as statement_cycle_id,
        trimBoth(coalesce(statement_type, '')) as statement_type,
        trimBoth(coalesce(tax_registration_id, '')) as tax_registration_id,
        trimBoth(coalesce(tax_zone, '')) as tax_zone,
        trimBoth(coalesce(payment_terms, '')) as payment_terms,
        trimBoth(coalesce(warehouse_id, '')) as warehouse_id,
        
        -- Clean email field and extract primary email (ClickHouse compatible)
        email_raw AS emails_all,
        multiIf(
            email_raw LIKE '%;%', trimBoth(splitByChar(';', email_raw)[1]),
            trimBoth(email_raw)
        ) AS primary_email,
        
        -- Count total emails (ClickHouse compatible)
        multiIf(
            email_raw LIKE '%;%', length(splitByChar(';', email_raw)),
            (email_raw IS NOT NULL) AND (email_raw != ''), 1,
            0
        ) AS email_count,
        
        -- Clean numeric fields - Fixed type consistency
        multiIf(
            (credit_limit_raw IS NOT NULL) AND (credit_limit_raw != '') AND (credit_limit_raw != 'NULL'),
            toDecimal64(credit_limit_raw, 2),
            toDecimal64('0.00', 2)
        ) as credit_limit,
        
        multiIf(
            (write_off_limit_raw IS NOT NULL) AND (write_off_limit_raw != '') AND (write_off_limit_raw != 'NULL'),
            toDecimal64(write_off_limit_raw, 2),
            toDecimal64('0.00', 2)
        ) as write_off_limit,
        
        multiIf(
            (lead_time_days_raw IS NOT NULL) AND (lead_time_days_raw != '') AND (lead_time_days_raw != 'NULL'),
            toFloat32(lead_time_days_raw),
            toFloat32(0.0)
        ) as lead_time_days,
        
        multiIf(
            (order_priority_raw IS NOT NULL) AND (order_priority_raw != '') AND (order_priority_raw != 'NULL'),
            toInt32(order_priority_raw),
            toInt32(0)
        ) as order_priority,
        
        multiIf(
            (primary_contact_id_raw IS NOT NULL) AND (primary_contact_id_raw != '') AND (primary_contact_id_raw != 'NULL'),
            toFloat64(primary_contact_id_raw),
            CAST(NULL AS Nullable(Float64))
        ) as primary_contact_id,
        
        -- Clean boolean fields
        multiIf(upper(trimBoth(coalesce(apply_overdue_charges_raw, 'FALSE'))) = 'TRUE', true, false) as apply_overdue_charges,
        multiIf(upper(trimBoth(coalesce(auto_apply_payments_raw, 'FALSE'))) = 'TRUE', true, false) as auto_apply_payments,
        multiIf(upper(trimBoth(coalesce(billing_address_override_raw, 'FALSE'))) = 'TRUE', true, false) as billing_address_override,
        multiIf(upper(trimBoth(coalesce(billing_contact_override_raw, 'FALSE'))) = 'TRUE', true, false) as billing_contact_override,
        multiIf(upper(trimBoth(coalesce(enable_currency_override_raw, 'FALSE'))) = 'TRUE', true, false) as enable_currency_override,
        multiIf(upper(trimBoth(coalesce(enable_rate_override_raw, 'FALSE'))) = 'TRUE', true, false) as enable_rate_override,
        multiIf(upper(trimBoth(coalesce(enable_write_offs_raw, 'FALSE'))) = 'TRUE', true, false) as enable_write_offs,
        multiIf(upper(trimBoth(coalesce(multi_currency_statements_raw, 'FALSE'))) = 'TRUE', true, false) as multi_currency_statements,
        multiIf(upper(trimBoth(coalesce(print_dunning_letters_raw, 'FALSE'))) = 'TRUE', true, false) as print_dunning_letters,
        multiIf(upper(trimBoth(coalesce(print_invoices_raw, 'FALSE'))) = 'TRUE', true, false) as print_invoices,
        multiIf(upper(trimBoth(coalesce(print_statements_raw, 'FALSE'))) = 'TRUE', true, false) as print_statements,
        multiIf(upper(trimBoth(coalesce(residential_delivery_raw, 'FALSE'))) = 'TRUE', true, false) as residential_delivery,
        multiIf(upper(trimBoth(coalesce(saturday_delivery_raw, 'FALSE'))) = 'TRUE', true, false) as saturday_delivery,
        multiIf(upper(trimBoth(coalesce(send_dunning_letters_by_email_raw, 'FALSE'))) = 'TRUE', true, false) as send_dunning_letters_by_email,
        multiIf(upper(trimBoth(coalesce(send_invoices_by_email_raw, 'FALSE'))) = 'TRUE', true, false) as send_invoices_by_email,
        multiIf(upper(trimBoth(coalesce(send_statements_by_email_raw, 'FALSE'))) = 'TRUE', true, false) as send_statements_by_email,
        multiIf(upper(trimBoth(coalesce(shipping_address_override_raw, 'FALSE'))) = 'TRUE', true, false) as shipping_address_override,
        multiIf(upper(trimBoth(coalesce(shipping_contact_override_raw, 'FALSE'))) = 'TRUE', true, false) as shipping_contact_override,
        
        -- Clean dates (ClickHouse compatible)
        multiIf(
            (created_datetime_raw IS NOT NULL) AND (created_datetime_raw != ''),
            parseDateTime64BestEffort(created_datetime_raw),
            CAST(NULL AS Nullable(DateTime64))
        ) as created_timestamp,
        
        multiIf(
            (last_modified_datetime_raw IS NOT NULL) AND (last_modified_datetime_raw != ''),
            parseDateTime64BestEffort(last_modified_datetime_raw),
            CAST(NULL AS Nullable(DateTime64))
        ) as last_modified_timestamp,
        
        multiIf(
            (created_datetime_raw IS NOT NULL) AND (created_datetime_raw != ''),
            toDate(parseDateTime64BestEffort(created_datetime_raw)),
            CAST(NULL AS Nullable(Date))
        ) as created_date,
        
        multiIf(
            (last_modified_datetime_raw IS NOT NULL) AND (last_modified_datetime_raw != ''),
            toDate(parseDateTime64BestEffort(last_modified_datetime_raw)),
            CAST(NULL AS Nullable(Date))
        ) as last_modified_date,
        
        -- Keep all raw fields that exist
        row_number,
        b_account_id,
        note_id,
        note,
        apply_overdue_charges_raw,
        auto_apply_payments_raw,
        billing_address_override_raw,
        billing_contact_override_raw,
        enable_currency_override_raw,
        enable_rate_override_raw,
        enable_write_offs_raw,
        multi_currency_statements_raw,
        print_dunning_letters_raw,
        print_invoices_raw,
        print_statements_raw,
        residential_delivery_raw,
        saturday_delivery_raw,
        send_dunning_letters_by_email_raw,
        send_invoices_by_email_raw,
        send_statements_by_email_raw,
        shipping_address_override_raw,
        shipping_contact_override_raw,
        credit_limit_raw,
        write_off_limit_raw,
        lead_time_days_raw,
        order_priority_raw,
        primary_contact_id_raw,
        created_datetime_raw,
        last_modified_datetime_raw,
        source_links,
        extracted_at,
        source_system,
        endpoint

    FROM {{ ref('customers_raw') }}
),

customer_business_logic AS (
    SELECT
        *,
        
        -- Customer tiers
        multiIf(
            credit_limit >= toDecimal64('15000.00', 2), 'Enterprise',
            credit_limit >= toDecimal64('10000.00', 2), 'Premium',
            credit_limit >= toDecimal64('5000.00', 2), 'Standard',
            credit_limit > toDecimal64('0.00', 2), 'Basic',
            'No Credit'
        ) as customer_tier,
        
        -- Status categorization
        multiIf(
            status = 'ACTIVE', 'Active',
            status = 'INACTIVE', 'Inactive',
            status
        ) as status_category,
        
        -- Customer type analysis
        multiIf(
            customer_class = 'DISPENSARY', 'Dispensary',
            customer_class = 'DISTRIBUTOR', 'Distributor',
            customer_class = 'MANUFACTURER', 'Manufacturer',
            customer_class
        ) as customer_type,
        
        -- Email quality flags
        multiIf(
            primary_email IS NULL OR primary_email = '', 'No Email',
            primary_email NOT LIKE '%@%' OR primary_email NOT LIKE '%.%', 'Invalid Email',
            email_count > 1, 'Multiple Emails',
            'Single Valid Email'
        ) as email_quality,
        
        -- Customer age analysis (ClickHouse compatible)
        multiIf(
            created_date IS NOT NULL,
            dateDiff('day', created_date, today()),
            CAST(NULL AS Nullable(Int32))
        ) as customer_age_days,
        
        multiIf(
            last_modified_date IS NOT NULL,
            dateDiff('day', last_modified_date, today()),
            CAST(NULL AS Nullable(Int32))
        ) as days_since_last_update

    FROM customer_cleaning
)

SELECT * 
FROM customer_business_logic
ORDER BY customer_id