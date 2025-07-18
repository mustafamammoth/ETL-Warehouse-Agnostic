-- models/silver/sales_invoices.sql
-- Cleaned, standardized, *deduplicated* sales invoices (silver layer) - ClickHouse compatible
-- Dedupe keeps latest by LastModifiedDateTime (fallback extracted_at).

{{ config(materialized='table') }}

WITH base AS (
    SELECT
        r.*,
        multiIf(
            (last_modified_datetime_raw IS NOT NULL) AND (last_modified_datetime_raw != ''),
            parseDateTime64BestEffort(last_modified_datetime_raw),
            parseDateTime64BestEffortOrNull(extracted_at)
        ) AS _version_ts
    FROM {{ ref('sales_invoices_raw') }} r
),

dedup AS (
    SELECT *
    FROM (
        SELECT
            *,
            row_number() OVER (
                PARTITION BY coalesce(nullIf(invoice_guid,''), nullIf(reference_number,''))
                ORDER BY _version_ts DESC, extracted_at DESC
            ) AS rn
        FROM base
    )
    WHERE rn = 1
),

invoice_cleaning AS (
    SELECT 
        invoice_guid,
        reference_number as invoice_number,
        customer_id,

        -- Clean text fields
        trimBoth(coalesce(customer_order, '')) as customer_order,
        upper(trimBoth(coalesce(invoice_type, ''))) as invoice_type,
        upper(trimBoth(coalesce(status, ''))) as status,
        trimBoth(coalesce(currency_id, 'USD')) as currency,
        trimBoth(coalesce(project, '')) as project,

        -- Clean description (remove newlines)
        multiIf(
            (description IS NOT NULL) AND (description != ''),
            replaceAll(replaceAll(trimBoth(description), char(10), ' '), char(13), ' '),
            CAST(NULL AS Nullable(String))
        ) as description_cleaned,

        -- Numeric fields
        multiIf(amount_raw IS NOT NULL AND amount_raw != '' AND amount_raw != 'NULL',
            toDecimal64(amount_raw, 2), toDecimal64('0.00', 2)) as amount,
        multiIf(balance_raw IS NOT NULL AND balance_raw != '' AND balance_raw != 'NULL',
            toDecimal64(balance_raw, 2), toDecimal64('0.00', 2)) as balance,
        multiIf(detail_total_raw IS NOT NULL AND detail_total_raw != '' AND detail_total_raw != 'NULL',
            toDecimal64(detail_total_raw, 2), toDecimal64('0.00', 2)) as detail_total,
        multiIf(tax_total_raw IS NOT NULL AND tax_total_raw != '' AND tax_total_raw != 'NULL',
            toDecimal64(tax_total_raw, 2), toDecimal64('0.00', 2)) as tax_total,
        multiIf(discount_total_raw IS NOT NULL AND discount_total_raw != '' AND discount_total_raw != 'NULL',
            toDecimal64(discount_total_raw, 2), toDecimal64('0.00', 2)) as discount_total,
        multiIf(freight_price_raw IS NOT NULL AND freight_price_raw != '' AND freight_price_raw != 'NULL',
            toDecimal64(freight_price_raw, 2), toDecimal64('0.00', 2)) as freight_price,
        multiIf(payment_total_raw IS NOT NULL AND payment_total_raw != '' AND payment_total_raw != 'NULL',
            toDecimal64(payment_total_raw, 2), toDecimal64('0.00', 2)) as payment_total,
        multiIf(cash_discount_raw IS NOT NULL AND cash_discount_raw != '' AND cash_discount_raw != 'NULL',
            toDecimal64(cash_discount_raw, 2), toDecimal64('0.00', 2)) as cash_discount,

        -- Boolean fields
        multiIf(upper(trimBoth(coalesce(credit_hold_raw, 'FALSE'))) = 'TRUE', true, false) as credit_hold,
        multiIf(upper(trimBoth(coalesce(hold_raw, 'FALSE'))) = 'TRUE', true, false) as hold,
        multiIf(upper(trimBoth(coalesce(is_tax_valid_raw, 'FALSE'))) = 'TRUE', true, false) as is_tax_valid,

        -- Date/Time fields
        multiIf(invoice_date_raw IS NOT NULL AND invoice_date_raw != '',
            parseDateTime64BestEffort(invoice_date_raw), CAST(NULL AS Nullable(DateTime64))) as invoice_date,
        multiIf(due_date_raw IS NOT NULL AND due_date_raw != '',
            parseDateTime64BestEffort(due_date_raw), CAST(NULL AS Nullable(DateTime64))) as due_date,
        multiIf(last_modified_datetime_raw IS NOT NULL AND last_modified_datetime_raw != '',
            parseDateTime64BestEffort(last_modified_datetime_raw), CAST(NULL AS Nullable(DateTime64))) as last_modified_timestamp,

        multiIf(invoice_date_raw IS NOT NULL AND invoice_date_raw != '',
            toDate(parseDateTime64BestEffort(invoice_date_raw)), CAST(NULL AS Nullable(Date))) as invoice_date_only,
        multiIf(due_date_raw IS NOT NULL AND due_date_raw != '',
            toDate(parseDateTime64BestEffort(due_date_raw)), CAST(NULL AS Nullable(Date))) as due_date_only,

        -- Lineage
        row_number,
        source_links,
        extracted_at,
        source_system,

        -- Raw references
        amount_raw,
        balance_raw,
        detail_total_raw,
        tax_total_raw,
        discount_total_raw,
        freight_price_raw,
        payment_total_raw,
        cash_discount_raw,
        credit_hold_raw,
        hold_raw,
        is_tax_valid_raw,
        invoice_date_raw,
        due_date_raw,
        last_modified_datetime_raw
    FROM dedup
),

invoice_categorization AS (
    SELECT
        *,
        -- Invoice classification
        multiIf(
            invoice_type = 'CREDIT MEMO', 'Credit Memo',
            invoice_type = 'INVOICE' OR invoice_type = '', 'Regular Invoice',
            invoice_type
        ) as invoice_category,
        -- Payment status
        multiIf(
            balance = toDecimal64('0.00', 2), 'Paid',
            status = 'CLOSED' AND balance > toDecimal64('0.00', 2), 'Closed with Balance',
            balance > toDecimal64('0.00', 2), 'Outstanding',
            'Unknown'
        ) as payment_status,
        -- Net amount
        amount - tax_total as net_amount
    FROM invoice_cleaning
),

invoice_business_logic AS (
    SELECT
        *,
        -- Outstanding balance only for regular invoices
        multiIf(
            invoice_category = 'Regular Invoice', balance,
            toDecimal64('0.00', 2)
        ) as outstanding_balance,

        -- Days outstanding
        multiIf(
            invoice_date_only IS NOT NULL
              AND invoice_category = 'Regular Invoice'
              AND balance > toDecimal64('0.00', 2),
            dateDiff('day', invoice_date_only, today()),
            CAST(NULL AS Nullable(Int32))
        ) as days_outstanding,

        -- Days until due (negative if overdue)
        multiIf(
            due_date_only IS NOT NULL
              AND invoice_category = 'Regular Invoice'
              AND balance > toDecimal64('0.00', 2),
            dateDiff('day', today(), due_date_only),
            CAST(NULL AS Nullable(Int32))
        ) as days_until_due,

        -- Aging buckets
        multiIf(
            invoice_category != 'Regular Invoice' OR balance = toDecimal64('0.00', 2), 'Not Applicable',
            due_date_only IS NULL, 'No Due Date',
            due_date_only >= today(), 'Current',
            dateDiff('day', due_date_only, today()) <= 30, '1-30 Days Past Due',
            dateDiff('day', due_date_only, today()) <= 60, '31-60 Days Past Due',
            dateDiff('day', due_date_only, today()) <= 90, '61-90 Days Past Due',
            '90+ Days Past Due'
        ) as aging_bucket,

        -- Size categories
        multiIf(
            amount >= toDecimal64('10000.00', 2), 'Large (10K+)',
            amount >= toDecimal64('5000.00', 2), 'Medium (5K-10K)',
            amount >= toDecimal64('1000.00', 2), 'Small (1K-5K)',
            amount > toDecimal64('0.00', 2), 'Micro (<1K)',
            'Zero/Credit'
        ) as invoice_size_category,

        -- Data quality flags
        multiIf(
            customer_id IS NULL OR customer_id = '', 'Missing Customer',
            invoice_date_only IS NULL, 'Missing Invoice Date',
            amount = toDecimal64('0.00', 2) AND invoice_category = 'Regular Invoice', 'Zero Amount Invoice',
            due_date_only IS NULL AND invoice_category = 'Regular Invoice', 'Missing Due Date',
            'Valid'
        ) as data_quality_flag,

        -- Temporal attributes
        multiIf(invoice_date_only IS NOT NULL, toYear(invoice_date_only), CAST(NULL AS Nullable(UInt16))) as invoice_year,
        multiIf(invoice_date_only IS NOT NULL, toMonth(invoice_date_only), CAST(NULL AS Nullable(UInt8))) as invoice_month,
        multiIf(invoice_date_only IS NOT NULL, toQuarter(invoice_date_only), CAST(NULL AS Nullable(UInt8))) as invoice_quarter,

        -- Payment percentage
        multiIf(
            amount > toDecimal64('0.00', 2),
            round((payment_total / amount) * 100, 2),
            toDecimal64('0.00', 2)
        ) as payment_percentage
    FROM invoice_categorization
)

SELECT *
FROM invoice_business_logic
ORDER BY invoice_date_only DESC, invoice_number
