-- customers.sql
-- Cleaned, standardized, *deduplicated* customer data (silver layer) - ClickHouse
-- Dedup logic keeps latest version per customer_guid/customer_id using last_modified_datetime_raw (fallback extracted_at)

{{ config(materialized='table') }}

WITH base AS (
    SELECT
        cr.*,
        -- Version timestamp: prefer last_modified, fallback extracted
        multiIf(
            (last_modified_datetime_raw IS NOT NULL) AND (last_modified_datetime_raw != ''),
            parseDateTime64BestEffort(last_modified_datetime_raw),
            parseDateTime64BestEffortOrNull(extracted_at)
        ) AS _version_ts
    FROM {{ ref('customers_raw') }} cr
),

dedup AS (
    SELECT *
    FROM (
        SELECT
            *,
            row_number() OVER (
                PARTITION BY coalesce(nullIf(customer_guid,''), nullIf(customer_id,''))
                ORDER BY _version_ts DESC, extracted_at DESC
            ) AS rn
        FROM base
    )
    WHERE rn = 1
),

customer_cleaning AS (
    SELECT 
        customer_guid,
        customer_id,
        customer_name,

        -- Clean text fields
        trimBoth(coalesce(account_ref, ''))             AS account_ref,
        trimBoth(coalesce(customer_category, ''))       AS customer_category,
        upper(trimBoth(coalesce(customer_class, '')))   AS customer_class,
        upper(trimBoth(coalesce(status, '')))           AS status,
        trimBoth(coalesce(currency_id, 'USD'))          AS currency_id,
        trimBoth(coalesce(currency_rate_type, ''))      AS currency_rate_type,
        trimBoth(coalesce(location_name, ''))           AS location_name,
        trimBoth(coalesce(fob_point, ''))               AS fob_point,
        trimBoth(coalesce(parent_record, ''))           AS parent_record,
        trimBoth(coalesce(price_class_id, ''))          AS price_class_id,
        trimBoth(coalesce(shipping_branch, ''))         AS shipping_branch,
        trimBoth(coalesce(shipping_rule, ''))           AS shipping_rule,
        trimBoth(coalesce(shipping_terms, ''))          AS shipping_terms,
        trimBoth(coalesce(shipping_zone_id, ''))        AS shipping_zone_id,
        trimBoth(coalesce(ship_via, ''))                AS ship_via,
        trimBoth(coalesce(statement_cycle_id, ''))      AS statement_cycle_id,
        trimBoth(coalesce(statement_type, ''))          AS statement_type,
        trimBoth(coalesce(tax_registration_id, ''))     AS tax_registration_id,
        trimBoth(coalesce(tax_zone, ''))                AS tax_zone,
        trimBoth(coalesce(payment_terms, ''))           AS payment_terms,
        trimBoth(coalesce(warehouse_id, ''))            AS warehouse_id,

        -- Email parsing
        email_raw                                       AS emails_all,
        multiIf(
            email_raw LIKE '%;%',
                trimBoth(arrayElement(splitByChar(';', email_raw), 1)),
            trimBoth(email_raw)
        ) AS primary_email,
        multiIf(
            email_raw LIKE '%;%', length(splitByChar(';', email_raw)),
            (email_raw IS NOT NULL) AND (email_raw != ''), 1,
            0
        ) AS email_count,

        -- Numeric fields
        multiIf(
            credit_limit_raw IS NOT NULL AND credit_limit_raw NOT IN ('','NULL'),
            toDecimal64(credit_limit_raw, 2),
            toDecimal64('0.00', 2)
        ) AS credit_limit,
        multiIf(
            write_off_limit_raw IS NOT NULL AND write_off_limit_raw NOT IN ('','NULL'),
            toDecimal64(write_off_limit_raw, 2),
            toDecimal64('0.00', 2)
        ) AS write_off_limit,
        multiIf(
            lead_time_days_raw IS NOT NULL AND lead_time_days_raw NOT IN ('','NULL'),
            toFloat32(lead_time_days_raw),
            toFloat32(0.0)
        ) AS lead_time_days,
        multiIf(
            order_priority_raw IS NOT NULL AND order_priority_raw NOT IN ('','NULL'),
            toInt32(order_priority_raw),
            toInt32(0)
        ) AS order_priority,
        multiIf(
            primary_contact_id_raw IS NOT NULL AND primary_contact_id_raw NOT IN ('','NULL'),
            toFloat64(primary_contact_id_raw),
            CAST(NULL AS Nullable(Float64))
        ) AS primary_contact_id,

        -- Boolean fields
        multiIf(upper(trimBoth(coalesce(apply_overdue_charges_raw, 'FALSE')))          = 'TRUE', true, false) AS apply_overdue_charges,
        multiIf(upper(trimBoth(coalesce(auto_apply_payments_raw, 'FALSE')))            = 'TRUE', true, false) AS auto_apply_payments,
        multiIf(upper(trimBoth(coalesce(billing_address_override_raw, 'FALSE')))       = 'TRUE', true, false) AS billing_address_override,
        multiIf(upper(trimBoth(coalesce(billing_contact_override_raw, 'FALSE')))       = 'TRUE', true, false) AS billing_contact_override,
        multiIf(upper(trimBoth(coalesce(enable_currency_override_raw, 'FALSE')))       = 'TRUE', true, false) AS enable_currency_override,
        multiIf(upper(trimBoth(coalesce(enable_rate_override_raw, 'FALSE')))           = 'TRUE', true, false) AS enable_rate_override,
        multiIf(upper(trimBoth(coalesce(enable_write_offs_raw, 'FALSE')))              = 'TRUE', true, false) AS enable_write_offs,
        multiIf(upper(trimBoth(coalesce(multi_currency_statements_raw, 'FALSE')))      = 'TRUE', true, false) AS multi_currency_statements,
        multiIf(upper(trimBoth(coalesce(print_dunning_letters_raw, 'FALSE')))          = 'TRUE', true, false) AS print_dunning_letters,
        multiIf(upper(trimBoth(coalesce(print_invoices_raw, 'FALSE')))                 = 'TRUE', true, false) AS print_invoices,
        multiIf(upper(trimBoth(coalesce(print_statements_raw, 'FALSE')))               = 'TRUE', true, false) AS print_statements,
        multiIf(upper(trimBoth(coalesce(residential_delivery_raw, 'FALSE')))           = 'TRUE', true, false) AS residential_delivery,
        multiIf(upper(trimBoth(coalesce(saturday_delivery_raw, 'FALSE')))              = 'TRUE', true, false) AS saturday_delivery,
        multiIf(upper(trimBoth(coalesce(send_dunning_letters_by_email_raw, 'FALSE')))  = 'TRUE', true, false) AS send_dunning_letters_by_email,
        multiIf(upper(trimBoth(coalesce(send_invoices_by_email_raw, 'FALSE')))         = 'TRUE', true, false) AS send_invoices_by_email,
        multiIf(upper(trimBoth(coalesce(send_statements_by_email_raw, 'FALSE')))       = 'TRUE', true, false) AS send_statements_by_email,
        multiIf(upper(trimBoth(coalesce(shipping_address_override_raw, 'FALSE')))      = 'TRUE', true, false) AS shipping_address_override,
        multiIf(upper(trimBoth(coalesce(shipping_contact_override_raw, 'FALSE')))      = 'TRUE', true, false) AS shipping_contact_override,

        -- Date / timestamp fields
        multiIf(
            created_datetime_raw IS NOT NULL AND created_datetime_raw != '',
            parseDateTime64BestEffort(created_datetime_raw),
            CAST(NULL AS Nullable(DateTime64))
        ) AS created_timestamp,
        multiIf(
            last_modified_datetime_raw IS NOT NULL AND last_modified_datetime_raw != '',
            parseDateTime64BestEffort(last_modified_datetime_raw),
            CAST(NULL AS Nullable(DateTime64))
        ) AS last_modified_timestamp,
        multiIf(
            created_datetime_raw IS NOT NULL AND created_datetime_raw != '',
            toDate(parseDateTime64BestEffort(created_datetime_raw)),
            CAST(NULL AS Nullable(Date))
        ) AS created_date,
        multiIf(
            last_modified_datetime_raw IS NOT NULL AND last_modified_datetime_raw != '',
            toDate(parseDateTime64BestEffort(last_modified_datetime_raw)),
            CAST(NULL AS Nullable(Date))
        ) AS last_modified_date,

        -- Raw passthrough (retained)
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
    FROM dedup
),

customer_business_logic AS (
    SELECT
        *,
        -- Customer tier classification
        multiIf(
            credit_limit >= toDecimal64('15000.00', 2), 'Enterprise',
            credit_limit >= toDecimal64('10000.00', 2), 'Premium',
            credit_limit >= toDecimal64('5000.00',  2), 'Standard',
            credit_limit >  toDecimal64('0.00',    2), 'Basic',
            'No Credit'
        ) AS customer_tier,
        -- Status category
        multiIf(
            status = 'ACTIVE',   'Active',
            status = 'INACTIVE', 'Inactive',
            status
        ) AS status_category,
        -- Customer type
        multiIf(
            customer_class = 'DISPENSARY',  'Dispensary',
            customer_class = 'DISTRIBUTOR', 'Distributor',
            customer_class = 'MANUFACTURER','Manufacturer',
            customer_class
        ) AS customer_type,
        -- Email quality
        multiIf(
            primary_email IS NULL OR primary_email = '',                'No Email',
            (primary_email NOT LIKE '%@%') OR (primary_email NOT LIKE '%.%'), 'Invalid Email',
            email_count > 1,                                           'Multiple Emails',
            'Single Valid Email'
        ) AS email_quality,
        -- Age metrics
        multiIf(
            created_date IS NOT NULL,
            dateDiff('day', created_date, today()),
            CAST(NULL AS Nullable(Int32))
        ) AS customer_age_days,
        multiIf(
            last_modified_date IS NOT NULL,
            dateDiff('day', last_modified_date, today()),
            CAST(NULL AS Nullable(Int32))
        ) AS days_since_last_update
    FROM customer_cleaning
)

SELECT *
FROM customer_business_logic
ORDER BY customer_id
