-- Cleaned and standardized vendor data (silver layer) - ClickHouse compatible
{{ config(materialized='table') }}

WITH vendor_cleaning AS (
    SELECT 
        vendor_guid,
        vendor_code,
        vendor_name,
        legal_name,
        
        -- Clean text fields
        trimBoth(coalesce(account_ref, '')) as account_ref,
        trimBoth(coalesce(ap_account, '')) as ap_account,
        trimBoth(coalesce(ap_subaccount, '')) as ap_subaccount,
        trimBoth(coalesce(cash_account, '')) as cash_account,
        trimBoth(coalesce(currency_id, 'USD')) as currency_id,
        trimBoth(coalesce(currency_rate_type, '')) as currency_rate_type,
        trimBoth(coalesce(f1099_box, '')) as f1099_box,
        trimBoth(coalesce(fatca, '')) as fatca,
        trimBoth(coalesce(fob_point, '')) as fob_point,
        trimBoth(coalesce(location_name, '')) as location_name,
        trimBoth(coalesce(parent_account, '')) as parent_account,
        trimBoth(coalesce(payment_by, '')) as payment_by,
        trimBoth(coalesce(payment_method, '')) as payment_method,
        trimBoth(coalesce(receipt_action, '')) as receipt_action,
        trimBoth(coalesce(receiving_branch, '')) as receiving_branch,
        trimBoth(coalesce(shipping_terms, '')) as shipping_terms,
        trimBoth(coalesce(ship_via, '')) as ship_via,
        upper(trimBoth(coalesce(status, ''))) as status,
        trimBoth(coalesce(tax_registration_id, '')) as tax_registration_id,
        trimBoth(coalesce(tax_zone, '')) as tax_zone,
        trimBoth(coalesce(payment_terms, '')) as payment_terms,
        upper(trimBoth(coalesce(vendor_class, ''))) as vendor_class,
        trimBoth(coalesce(warehouse, '')) as warehouse,
        
        -- Clean numeric fields (ClickHouse compatible - consistent decimal types)
        multiIf(
            (lead_time_days_raw IS NOT NULL) AND (lead_time_days_raw != '') AND (lead_time_days_raw != 'NULL'),
            toDecimal64(lead_time_days_raw, 1),
            toDecimal64('0.0', 1)
        ) as lead_time_days,
        
        multiIf(
            (max_receipt_raw IS NOT NULL) AND (max_receipt_raw != '') AND (max_receipt_raw != 'NULL'),
            toDecimal64(max_receipt_raw, 2),
            toDecimal64('0.00', 2)
        ) as max_receipt,
        
        multiIf(
            (min_receipt_raw IS NOT NULL) AND (min_receipt_raw != '') AND (min_receipt_raw != 'NULL'),
            toDecimal64(min_receipt_raw, 2),
            toDecimal64('0.00', 2)
        ) as min_receipt,
        
        multiIf(
            (payment_lead_time_days_raw IS NOT NULL) AND (payment_lead_time_days_raw != '') AND (payment_lead_time_days_raw != 'NULL'),
            toInt32(payment_lead_time_days_raw),
            toInt32(0)
        ) as payment_lead_time_days,
        
        multiIf(
            (threshold_receipt_raw IS NOT NULL) AND (threshold_receipt_raw != '') AND (threshold_receipt_raw != 'NULL'),
            toDecimal64(threshold_receipt_raw, 2),
            toDecimal64('0.00', 2)
        ) as threshold_receipt,
        
        -- Clean boolean fields
        multiIf(upper(trimBoth(coalesce(enable_currency_override_raw, 'FALSE'))) = 'TRUE', true, false) as enable_currency_override,
        multiIf(upper(trimBoth(coalesce(enable_rate_override_raw, 'FALSE'))) = 'TRUE', true, false) as enable_rate_override,
        multiIf(upper(trimBoth(coalesce(f1099_vendor_raw, 'FALSE'))) = 'TRUE', true, false) as is_1099_vendor,
        multiIf(upper(trimBoth(coalesce(foreign_entity_raw, 'FALSE'))) = 'TRUE', true, false) as is_foreign_entity,
        multiIf(upper(trimBoth(coalesce(landed_cost_vendor_raw, 'FALSE'))) = 'TRUE', true, false) as is_landed_cost_vendor,
        multiIf(upper(trimBoth(coalesce(pay_separately_raw, 'FALSE'))) = 'TRUE', true, false) as pay_separately,
        multiIf(upper(trimBoth(coalesce(print_orders_raw, 'FALSE'))) = 'TRUE', true, false) as print_orders,
        multiIf(upper(trimBoth(coalesce(remittance_address_override_raw, 'FALSE'))) = 'TRUE', true, false) as remittance_address_override,
        multiIf(upper(trimBoth(coalesce(remittance_contact_override_raw, 'FALSE'))) = 'TRUE', true, false) as remittance_contact_override,
        multiIf(upper(trimBoth(coalesce(send_orders_by_email_raw, 'FALSE'))) = 'TRUE', true, false) as send_orders_by_email,
        multiIf(upper(trimBoth(coalesce(shipping_address_override_raw, 'FALSE'))) = 'TRUE', true, false) as shipping_address_override,
        multiIf(upper(trimBoth(coalesce(shipping_contact_override_raw, 'FALSE'))) = 'TRUE', true, false) as shipping_contact_override,
        multiIf(upper(trimBoth(coalesce(vendor_is_labor_union_raw, 'FALSE'))) = 'TRUE', true, false) as vendor_is_labor_union,
        multiIf(upper(trimBoth(coalesce(vendor_is_tax_agency_raw, 'FALSE'))) = 'TRUE', true, false) as vendor_is_tax_agency,
        
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
        
        -- Clean notes safely (without char functions for now)
        trimBoth(coalesce(vendor_name, '')) as notes_cleaned,
        
        -- Keep all raw fields that exist (excluding problematic ones)
        row_number,
        source_links,
        extracted_at,
        source_system,
        endpoint,
        
        -- All the raw fields for reference
        lead_time_days_raw,
        max_receipt_raw,
        min_receipt_raw,
        payment_lead_time_days_raw,
        threshold_receipt_raw,
        enable_currency_override_raw,
        enable_rate_override_raw,
        f1099_vendor_raw,
        foreign_entity_raw,
        landed_cost_vendor_raw,
        pay_separately_raw,
        print_orders_raw,
        remittance_address_override_raw,
        remittance_contact_override_raw,
        send_orders_by_email_raw,
        shipping_address_override_raw,
        shipping_contact_override_raw,
        vendor_is_labor_union_raw,
        vendor_is_tax_agency_raw,
        created_datetime_raw,
        last_modified_datetime_raw

    FROM {{ ref('vendors_raw') }}
),

vendor_business_logic AS (
    SELECT
        *,
        
        -- Status categorization
        multiIf(
            status = 'ACTIVE', 'Active',
            status = 'INACTIVE', 'Inactive',
            status
        ) as status_category,
        
        -- Vendor type analysis
        multiIf(
            vendor_class = 'CANNABIS', 'Cannabis Vendor',
            vendor_class = 'SERVICE', 'Service Provider',
            vendor_class = 'SUPPLIES', 'Supplies Vendor',
            vendor_class
        ) as vendor_type,
        
        -- Payment terms categorization
        multiIf(
            payment_terms = 'COD', 'Cash on Delivery',
            payment_terms = 'NET30', 'Net 30 Days',
            payment_terms = 'NET15', 'Net 15 Days',
            payment_terms = 'DUE DATE', 'Due Date',
            payment_terms
        ) as payment_terms_category,
        
        -- Tax classification
        multiIf(
            tax_zone = 'CNNABISTAX', 'Cannabis Tax',
            tax_zone = 'EXEMPT', 'Tax Exempt',
            tax_zone
        ) as tax_classification,
        
        -- Complexity flags
        multiIf(
            is_1099_vendor = true AND is_foreign_entity = true, 'Complex (1099 + Foreign)',
            is_1099_vendor = true, 'Requires 1099',
            is_foreign_entity = true, 'Foreign Entity',
            'Standard'
        ) as vendor_complexity,
        
        -- Data quality flags
        multiIf(legal_name IS NULL OR legal_name = '', false, true) as has_legal_name,
        multiIf(tax_registration_id IS NULL OR tax_registration_id = '', false, true) as has_tax_id,
        
        -- Vendor age analysis (ClickHouse compatible)
        multiIf(
            created_date IS NOT NULL,
            dateDiff('day', created_date, today()),
            CAST(NULL AS Nullable(Int32))
        ) as vendor_age_days,
        
        multiIf(
            last_modified_date IS NOT NULL,
            dateDiff('day', last_modified_date, today()),
            CAST(NULL AS Nullable(Int32))
        ) as days_since_last_update,
        
        -- Data completeness score (0-100)
        (
            multiIf(vendor_name IS NOT NULL AND vendor_name != '', 20, 0) +
            multiIf(legal_name IS NOT NULL AND legal_name != '', 20, 0) +
            multiIf(tax_registration_id IS NOT NULL AND tax_registration_id != '', 20, 0) +
            multiIf(payment_terms IS NOT NULL AND payment_terms != '', 20, 0) +
            multiIf(vendor_class IS NOT NULL AND vendor_class != '', 20, 0)
        ) as data_completeness_score

    FROM vendor_cleaning
)

SELECT * 
FROM vendor_business_logic
ORDER BY vendor_code