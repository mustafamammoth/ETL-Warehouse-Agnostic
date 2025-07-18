-- models/silver/vendors.sql
-- Cleaned, deduplicated & standardized vendor data (silver layer) - ClickHouse compatible
{{ config(materialized='table') }}

WITH base AS (
    SELECT
        v.*,
        -- Version timestamp for dedup preference (LastModified > Created > extracted_at)
        multiIf(
            (last_modified_datetime_raw IS NOT NULL AND last_modified_datetime_raw != ''),
            parseDateTime64BestEffort(last_modified_datetime_raw),
            (created_datetime_raw IS NOT NULL AND created_datetime_raw != ''),
            parseDateTime64BestEffort(created_datetime_raw),
            parseDateTime64BestEffortOrNull(extracted_at)
        ) AS _version_ts
    FROM {{ ref('vendors_raw') }} v
),

dedup AS (
    -- Keep latest record per vendor (prefer vendor_guid, fallback to vendor_code)
    SELECT *
    FROM (
        SELECT
            *,
            row_number() OVER (
                PARTITION BY coalesce(nullIf(vendor_guid,''), nullIf(vendor_code,''))
                ORDER BY _version_ts DESC, extracted_at DESC
            ) AS rn
        FROM base
    )
    WHERE rn = 1
),

vendor_cleaning AS (
    SELECT 
        vendor_guid,
        vendor_code,
        vendor_name,
        legal_name,
        
        -- Clean text fields
        trimBoth(coalesce(account_ref, ''))                    AS account_ref,
        trimBoth(coalesce(ap_account, ''))                     AS ap_account,
        trimBoth(coalesce(ap_subaccount, ''))                  AS ap_subaccount,
        trimBoth(coalesce(cash_account, ''))                   AS cash_account,
        trimBoth(coalesce(currency_id, 'USD'))                 AS currency_id,
        trimBoth(coalesce(currency_rate_type, ''))             AS currency_rate_type,
        trimBoth(coalesce(f1099_box, ''))                      AS f1099_box,
        trimBoth(coalesce(fatca, ''))                          AS fatca,
        trimBoth(coalesce(fob_point, ''))                      AS fob_point,
        trimBoth(coalesce(location_name, ''))                  AS location_name,
        trimBoth(coalesce(parent_account, ''))                 AS parent_account,
        trimBoth(coalesce(payment_by, ''))                     AS payment_by,
        trimBoth(coalesce(payment_method, ''))                 AS payment_method,
        trimBoth(coalesce(receipt_action, ''))                 AS receipt_action,
        trimBoth(coalesce(receiving_branch, ''))               AS receiving_branch,
        trimBoth(coalesce(shipping_terms, ''))                 AS shipping_terms,
        trimBoth(coalesce(ship_via, ''))                       AS ship_via,
        upper(trimBoth(coalesce(status, '')))                  AS status,
        trimBoth(coalesce(tax_registration_id, ''))            AS tax_registration_id,
        trimBoth(coalesce(tax_zone, ''))                       AS tax_zone,
        trimBoth(coalesce(payment_terms, ''))                  AS payment_terms,
        upper(trimBoth(coalesce(vendor_class, '')))            AS vendor_class,
        trimBoth(coalesce(warehouse, ''))                      AS warehouse,
        
        -- Numeric fields
        multiIf(
            lead_time_days_raw IS NOT NULL AND lead_time_days_raw != '' AND lead_time_days_raw != 'NULL',
            toDecimal64(lead_time_days_raw, 1),
            toDecimal64('0.0', 1)
        )                                                     AS lead_time_days,
        multiIf(
            max_receipt_raw IS NOT NULL AND max_receipt_raw != '' AND max_receipt_raw != 'NULL',
            toDecimal64(max_receipt_raw, 2),
            toDecimal64('0.00', 2)
        )                                                     AS max_receipt,
        multiIf(
            min_receipt_raw IS NOT NULL AND min_receipt_raw != '' AND min_receipt_raw != 'NULL',
            toDecimal64(min_receipt_raw, 2),
            toDecimal64('0.00', 2)
        )                                                     AS min_receipt,
        multiIf(
            payment_lead_time_days_raw IS NOT NULL AND payment_lead_time_days_raw != '' AND payment_lead_time_days_raw != 'NULL',
            toInt32(payment_lead_time_days_raw),
            toInt32(0)
        )                                                     AS payment_lead_time_days,
        multiIf(
            threshold_receipt_raw IS NOT NULL AND threshold_receipt_raw != '' AND threshold_receipt_raw != 'NULL',
            toDecimal64(threshold_receipt_raw, 2),
            toDecimal64('0.00', 2)
        )                                                     AS threshold_receipt,
        
        -- Boolean fields
        multiIf(upper(trimBoth(coalesce(enable_currency_override_raw, 'FALSE'))) = 'TRUE', true, false) AS enable_currency_override,
        multiIf(upper(trimBoth(coalesce(enable_rate_override_raw, 'FALSE'))) = 'TRUE', true, false)     AS enable_rate_override,
        multiIf(upper(trimBoth(coalesce(f1099_vendor_raw, 'FALSE'))) = 'TRUE', true, false)             AS is_1099_vendor,
        multiIf(upper(trimBoth(coalesce(foreign_entity_raw, 'FALSE'))) = 'TRUE', true, false)           AS is_foreign_entity,
        multiIf(upper(trimBoth(coalesce(landed_cost_vendor_raw, 'FALSE'))) = 'TRUE', true, false)       AS is_landed_cost_vendor,
        multiIf(upper(trimBoth(coalesce(pay_separately_raw, 'FALSE'))) = 'TRUE', true, false)           AS pay_separately,
        multiIf(upper(trimBoth(coalesce(print_orders_raw, 'FALSE'))) = 'TRUE', true, false)             AS print_orders,
        multiIf(upper(trimBoth(coalesce(remittance_address_override_raw, 'FALSE'))) = 'TRUE', true, false) AS remittance_address_override,
        multiIf(upper(trimBoth(coalesce(remittance_contact_override_raw, 'FALSE'))) = 'TRUE', true, false) AS remittance_contact_override,
        multiIf(upper(trimBoth(coalesce(send_orders_by_email_raw, 'FALSE'))) = 'TRUE', true, false)     AS send_orders_by_email,
        multiIf(upper(trimBoth(coalesce(shipping_address_override_raw, 'FALSE'))) = 'TRUE', true, false) AS shipping_address_override,
        multiIf(upper(trimBoth(coalesce(shipping_contact_override_raw, 'FALSE'))) = 'TRUE', true, false) AS shipping_contact_override,
        multiIf(upper(trimBoth(coalesce(vendor_is_labor_union_raw, 'FALSE'))) = 'TRUE', true, false)    AS vendor_is_labor_union,
        multiIf(upper(trimBoth(coalesce(vendor_is_tax_agency_raw, 'FALSE'))) = 'TRUE', true, false)     AS vendor_is_tax_agency,
        
        -- Dates
        multiIf(
            created_datetime_raw IS NOT NULL AND created_datetime_raw != '',
            parseDateTime64BestEffort(created_datetime_raw),
            CAST(NULL AS Nullable(DateTime64))
        )                                                     AS created_timestamp,
        multiIf(
            last_modified_datetime_raw IS NOT NULL AND last_modified_datetime_raw != '',
            parseDateTime64BestEffort(last_modified_datetime_raw),
            CAST(NULL AS Nullable(DateTime64))
        )                                                     AS last_modified_timestamp,
        multiIf(
            created_datetime_raw IS NOT NULL AND created_datetime_raw != '',
            toDate(parseDateTime64BestEffort(created_datetime_raw)),
            CAST(NULL AS Nullable(Date))
        )                                                     AS created_date,
        multiIf(
            last_modified_datetime_raw IS NOT NULL AND last_modified_datetime_raw != '',
            toDate(parseDateTime64BestEffort(last_modified_datetime_raw)),
            CAST(NULL AS Nullable(Date))
        )                                                     AS last_modified_date,
        
        -- Notes cleaned (placeholder)
        trimBoth(coalesce(vendor_name, ''))                    AS notes_cleaned,
        
        -- Raw lineage
        row_number,
        source_links,
        extracted_at,
        source_system,
        endpoint,
        
        -- Raw reference fields
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
    FROM dedup
),

vendor_business_logic AS (
    SELECT
        *,
        -- Status categorization
        multiIf(
            status = 'ACTIVE', 'Active',
            status = 'INACTIVE', 'Inactive',
            status
        ) AS status_category,
        
        -- Vendor type categorization
        multiIf(
            vendor_class = 'CANNABIS', 'Cannabis Vendor',
            vendor_class = 'SERVICE', 'Service Provider',
            vendor_class = 'SUPPLIES', 'Supplies Vendor',
            vendor_class
        ) AS vendor_type,
        
        -- Payment terms categorization
        multiIf(
            payment_terms = 'COD', 'Cash on Delivery',
            payment_terms = 'NET30', 'Net 30 Days',
            payment_terms = 'NET15', 'Net 15 Days',
            payment_terms = 'DUE DATE', 'Due Date',
            payment_terms
        ) AS payment_terms_category,
        
        -- Tax classification
        multiIf(
            tax_zone = 'CNNABISTAX', 'Cannabis Tax',
            tax_zone = 'EXEMPT', 'Tax Exempt',
            tax_zone
        ) AS tax_classification,
        
        -- Complexity flags
        multiIf(
            is_1099_vendor = true AND is_foreign_entity = true, 'Complex (1099 + Foreign)',
            is_1099_vendor = true, 'Requires 1099',
            is_foreign_entity = true, 'Foreign Entity',
            'Standard'
        ) AS vendor_complexity,
        
        -- Data quality flags
        multiIf(legal_name IS NULL OR legal_name = '', false, true) AS has_legal_name,
        multiIf(tax_registration_id IS NULL OR tax_registration_id = '', false, true) AS has_tax_id,
        
        -- Age analytics
        multiIf(
            created_date IS NOT NULL,
            dateDiff('day', created_date, today()),
            CAST(NULL AS Nullable(Int32))
        ) AS vendor_age_days,
        multiIf(
            last_modified_date IS NOT NULL,
            dateDiff('day', last_modified_date, today()),
            CAST(NULL AS Nullable(Int32))
        ) AS days_since_last_update,
        
        -- Data completeness score (0–100)
        (
            multiIf(vendor_name IS NOT NULL AND vendor_name != '', 20, 0) +
            multiIf(legal_name IS NOT NULL AND legal_name != '', 20, 0) +
            multiIf(tax_registration_id IS NOT NULL AND tax_registration_id != '', 20, 0) +
            multiIf(payment_terms IS NOT NULL AND payment_terms != '', 20, 0) +
            multiIf(vendor_class IS NOT NULL AND vendor_class != '', 20, 0)
        ) AS data_completeness_score
    FROM vendor_cleaning
)

SELECT *
FROM vendor_business_logic
ORDER BY vendor_code
