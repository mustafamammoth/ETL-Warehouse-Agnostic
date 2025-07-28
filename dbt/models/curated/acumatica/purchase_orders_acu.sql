-- models/silver/purchase_orders.sql
-- Cleaned, standardized, and *deduplicated* purchase orders (silver layer) - ClickHouse compatible
-- Dedup keeps the latest version per purchase_order_guid (fallback: order_number)
-- Business logic preserved from your provided version.

{{ config(materialized='table') }}

WITH base AS (
    SELECT
        por.*,
        -- Version timestamp: prefer last_modified_datetime_raw, fallback extracted_at
        multiIf(
            (last_modified_datetime_raw IS NOT NULL) AND (last_modified_datetime_raw != ''),
            parseDateTime64BestEffort(last_modified_datetime_raw),
            parseDateTime64BestEffortOrNull(extracted_at)
        ) AS _version_ts
    FROM {{ ref('purchase_orders_raw') }} por
),

dedup AS (
    SELECT *
    FROM (
        SELECT
            *,
            row_number() OVER (
                PARTITION BY coalesce(nullIf(purchase_order_guid,''), nullIf(order_number,''))
                ORDER BY _version_ts DESC, extracted_at DESC
            ) AS rn
        FROM base
    )
    WHERE rn = 1
),

purchase_order_cleaning AS (
    SELECT 
        purchase_order_guid,
        order_number,
        vendor_id,
        
        -- Clean text fields
        trimBoth(coalesce(vendor_ref, ''))                  AS vendor_ref,
        trimBoth(coalesce(base_currency_id, 'USD'))         AS base_currency_id,
        trimBoth(coalesce(branch, ''))                      AS branch,
        trimBoth(coalesce(currency_id, 'USD'))              AS currency_id,
        trimBoth(coalesce(currency_rate_type_id, ''))       AS currency_rate_type_id,
        trimBoth(coalesce(location, ''))                    AS location,
        trimBoth(coalesce(owner, ''))                       AS owner,
        upper(trimBoth(coalesce(status, '')))               AS status,
        trimBoth(coalesce(payment_terms, ''))               AS payment_terms,
        upper(trimBoth(coalesce(order_type, '')))           AS order_type,
        trimBoth(coalesce(vendor_tax_zone, ''))             AS vendor_tax_zone,
        
        -- Clean description safely
        trimBoth(coalesce(description, ''))                 AS description_cleaned,
        
        -- Clean numeric fields
        multiIf(
            (control_total_raw IS NOT NULL) AND (control_total_raw != '') AND (control_total_raw != 'NULL'),
            toDecimal64(control_total_raw, 2),
            toDecimal64('0.00', 2)
        ) AS control_total,
        
        multiIf(
            (currency_rate_raw IS NOT NULL) AND (currency_rate_raw != '') AND (currency_rate_raw != 'NULL'),
            toDecimal64(currency_rate_raw, 4),
            toDecimal64('1.0000', 4)
        ) AS currency_rate,
        
        multiIf(
            (currency_reciprocal_rate_raw IS NOT NULL) AND (currency_reciprocal_rate_raw != '') AND (currency_reciprocal_rate_raw != 'NULL'),
            toDecimal64(currency_reciprocal_rate_raw, 4),
            toDecimal64('1.0000', 4)
        ) AS currency_reciprocal_rate,
        
        multiIf(
            (line_total_raw IS NOT NULL) AND (line_total_raw != '') AND (line_total_raw != 'NULL'),
            toDecimal64(line_total_raw, 2),
            toDecimal64('0.00', 2)
        ) AS line_total,
        
        multiIf(
            (order_total_raw IS NOT NULL) AND (order_total_raw != '') AND (order_total_raw != 'NULL'),
            toDecimal64(order_total_raw, 2),
            toDecimal64('0.00', 2)
        ) AS order_total,
        
        multiIf(
            (tax_total_raw IS NOT NULL) AND (tax_total_raw != '') AND (tax_total_raw != 'NULL'),
            toDecimal64(tax_total_raw, 2),
            toDecimal64('0.00', 2)
        ) AS tax_total,
        
        -- Booleans
        multiIf(upper(trimBoth(coalesce(hold_raw, 'FALSE'))) = 'TRUE', true, false) AS hold,
        multiIf(upper(trimBoth(coalesce(is_tax_valid_raw, 'FALSE'))) = 'TRUE', true, false) AS is_tax_valid,
        
        -- Dates
        multiIf(
            (order_date_raw IS NOT NULL) AND (order_date_raw != ''),
            parseDateTime64BestEffort(order_date_raw),
            CAST(NULL AS Nullable(DateTime64))
        ) AS order_date,
        
        multiIf(
            (currency_effective_date_raw IS NOT NULL) AND (currency_effective_date_raw != ''),
            parseDateTime64BestEffort(currency_effective_date_raw),
            CAST(NULL AS Nullable(DateTime64))
        ) AS currency_effective_date,
        
        multiIf(
            (promised_date_raw IS NOT NULL) AND (promised_date_raw != ''),
            parseDateTime64BestEffort(promised_date_raw),
            CAST(NULL AS Nullable(DateTime64))
        ) AS promised_date,
        
        multiIf(
            (last_modified_datetime_raw IS NOT NULL) AND (last_modified_datetime_raw != ''),
            parseDateTime64BestEffort(last_modified_datetime_raw),
            CAST(NULL AS Nullable(DateTime64))
        ) AS last_modified_timestamp,
        
        -- Simple dates
        multiIf(
            (order_date_raw IS NOT NULL) AND (order_date_raw != ''),
            toDate(parseDateTime64BestEffort(order_date_raw)),
            CAST(NULL AS Nullable(Date))
        ) AS order_date_only,
        
        multiIf(
            (promised_date_raw IS NOT NULL) AND (promised_date_raw != ''),
            toDate(parseDateTime64BestEffort(promised_date_raw)),
            CAST(NULL AS Nullable(Date))
        ) AS promised_date_only,
        
        -- Raw fields retained
        row_number,
        note,
        custom,
        description,
        source_links,
        extracted_at,
        source_system,
        endpoint,
        
        control_total_raw,
        currency_rate_raw,
        currency_reciprocal_rate_raw,
        line_total_raw,
        order_total_raw,
        tax_total_raw,
        hold_raw,
        is_tax_valid_raw,
        order_date_raw,
        currency_effective_date_raw,
        promised_date_raw,
        last_modified_datetime_raw
    FROM dedup
),

purchase_order_business_logic AS (
    SELECT
        *,
        -- Order type categorization
        multiIf(
            order_type = 'NORMAL',   'Normal Purchase',
            order_type = 'DROPSHIP', 'Drop Ship',
            order_type = 'BLANKET',  'Blanket Order',
            order_type
        ) AS order_type_category,
        
        -- Status categorization
        multiIf(
            status = 'COMPLETED', 'Completed',
            status = 'CLOSED',    'Closed',
            status = 'OPEN',      'Open',
            status = 'ON HOLD',   'On Hold',
            status
        ) AS status_category,
        
        -- Order size categories
        multiIf(
            order_total >= toDecimal64('100000.00', 2), 'Very Large ($100K+)',
            order_total >= toDecimal64('50000.00', 2),  'Large ($50K-100K)',
            order_total >= toDecimal64('20000.00', 2),  'Medium ($20K-50K)',
            order_total >= toDecimal64('5000.00', 2),   'Small ($5K-20K)',
            order_total >  toDecimal64('0.00', 2),      'Micro (<$5K)',
            'Zero/Credit'
        ) AS order_size_category,
        
        -- Payment terms categorization
        multiIf(
            payment_terms = 'COD',   'Cash on Delivery',
            payment_terms = 'NET30', 'Net 30 Days',
            payment_terms = 'NET15', 'Net 15 Days',
            payment_terms
        ) AS payment_terms_category,
        
        -- Tax classification
        multiIf(
            vendor_tax_zone = 'EXEMPT',   'Tax Exempt',
            vendor_tax_zone = 'CANNABIS', 'Cannabis Tax',
            vendor_tax_zone
        ) AS tax_classification,
        
        -- Lead time
        multiIf(
            promised_date_only IS NOT NULL AND order_date_only IS NOT NULL,
            dateDiff('day', order_date_only, promised_date_only),
            CAST(NULL AS Nullable(Int32))
        ) AS promised_lead_time_days,
        
        -- Priority status
        multiIf(
            hold = true, 'On Hold',
            promised_date_only IS NOT NULL AND promised_date_only < today(), 'Overdue',
            promised_date_only IS NOT NULL AND dateDiff('day', today(), promised_date_only) <= 7, 'Due Soon',
            status = 'COMPLETED', 'Completed',
            'Normal'
        ) AS priority_status,
        
        -- Product category (simple keyword scan)
        multiIf(
            positionCaseInsensitive(description_cleaned, 'cart')       > 0, 'Carts',
            positionCaseInsensitive(description_cleaned, 'packaging')  > 0, 'Packaging',
            positionCaseInsensitive(description_cleaned, 'distillate') > 0, 'Distillate',
            positionCaseInsensitive(description_cleaned, 'flower')     > 0, 'Flower',
            'Other'
        ) AS product_category,
        
        -- Totals validation
        multiIf(
            abs(toFloat64(order_total) - (toFloat64(line_total) + toFloat64(tax_total))) <= 0.01,
            true,
            false
        ) AS totals_match,
        
        -- Time periods
        multiIf(order_date_only IS NOT NULL, toYear(order_date_only),    CAST(NULL AS Nullable(UInt16))) AS order_year,
        multiIf(order_date_only IS NOT NULL, toMonth(order_date_only),   CAST(NULL AS Nullable(UInt8)))  AS order_month,
        multiIf(order_date_only IS NOT NULL, toQuarter(order_date_only), CAST(NULL AS Nullable(UInt8)))  AS order_quarter,
        
        -- Data completeness score
        (
            multiIf(vendor_id     IS NOT NULL AND vendor_id     != '', 20, 0) +
            multiIf(order_total   >  toDecimal64('0.00', 2),          20, 0) +
            multiIf(order_date_only IS NOT NULL,                      20, 0) +
            multiIf(status        IS NOT NULL AND status        != '',20, 0) +
            multiIf(payment_terms IS NOT NULL AND payment_terms != '',20, 0)
        ) AS data_completeness_score
    FROM purchase_order_cleaning
)

SELECT *
FROM purchase_order_business_logic
ORDER BY order_date_only DESC, order_number
