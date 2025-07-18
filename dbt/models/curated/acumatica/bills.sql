-- models/silver/bills.sql
-- Cleansed + deduplicated bills (silver layer) ClickHouse compatible
{{ config(materialized='table') }}

WITH base AS (
    SELECT
        b.*,
        -- Parse timestamps for ordering (fallback extracted_at)
        multiIf(
            (last_modified_datetime_raw IS NOT NULL) AND (last_modified_datetime_raw != ''),
            parseDateTime64BestEffort(last_modified_datetime_raw),
            parseDateTime64BestEffortOrNull(extracted_at)
        ) AS _version_ts
    FROM {{ ref('bills_raw') }} b
),

dedup AS (
    -- Keep the latest version per (bill_guid or reference_number)
    SELECT *
    FROM (
        SELECT
            *,
            row_number() OVER (
                PARTITION BY
                    coalesce(nullIf(bill_guid,''), nullIf(reference_number,''))
                ORDER BY _version_ts DESC, extracted_at DESC
            ) AS rn
        FROM base
    ) WHERE rn = 1
),

bill_cleaning AS (
    SELECT 
        bill_guid,
        reference_number as bill_number,
        vendor_code,
        trimBoth(coalesce(vendor_ref, '')) as vendor_ref,
        upper(trimBoth(coalesce(bill_type, ''))) as bill_type,
        upper(trimBoth(coalesce(status, ''))) as status,
        trimBoth(coalesce(currency_id, 'USD')) as currency_id,
        trimBoth(coalesce(branch_id, '')) as branch_id,
        trimBoth(coalesce(cash_account, '')) as cash_account,
        trimBoth(coalesce(location_id, '')) as location_id,
        trimBoth(coalesce(post_period, '')) as post_period,
        trimBoth(coalesce(payment_terms, '')) as payment_terms,
        trimBoth(coalesce(description, '')) as description_cleaned,

        multiIf(amount_raw IS NOT NULL AND amount_raw != '' AND amount_raw != 'NULL',
            toDecimal64(amount_raw, 2), toDecimal64('0.00', 2)) as amount,
        multiIf(balance_raw IS NOT NULL AND balance_raw != '' AND balance_raw != 'NULL',
            toDecimal64(balance_raw, 2), toDecimal64('0.00', 2)) as balance,
        multiIf(tax_total_raw IS NOT NULL AND tax_total_raw != '' AND tax_total_raw != 'NULL',
            toDecimal64(tax_total_raw, 2), toDecimal64('0.00', 2)) as tax_total,

        multiIf(upper(trimBoth(coalesce(approved_for_payment_raw, 'FALSE'))) = 'TRUE', true, false) as approved_for_payment,
        multiIf(upper(trimBoth(coalesce(hold_raw, 'FALSE'))) = 'TRUE', true, false) as hold,
        multiIf(upper(trimBoth(coalesce(is_tax_valid_raw, 'FALSE'))) = 'TRUE', true, false) as is_tax_valid,

        multiIf(bill_date_raw IS NOT NULL AND bill_date_raw != '',
            parseDateTime64BestEffort(bill_date_raw), CAST(NULL AS Nullable(DateTime64))) as bill_date,
        multiIf(due_date_raw IS NOT NULL AND due_date_raw != '',
            parseDateTime64BestEffort(due_date_raw), CAST(NULL AS Nullable(DateTime64))) as due_date,
        multiIf(last_modified_datetime_raw IS NOT NULL AND last_modified_datetime_raw != '',
            parseDateTime64BestEffort(last_modified_datetime_raw), CAST(NULL AS Nullable(DateTime64))) as last_modified_timestamp,

        multiIf(bill_date_raw IS NOT NULL AND bill_date_raw != '',
            toDate(parseDateTime64BestEffort(bill_date_raw)), CAST(NULL AS Nullable(Date))) as bill_date_only,
        multiIf(due_date_raw IS NOT NULL AND due_date_raw != '',
            toDate(parseDateTime64BestEffort(due_date_raw)), CAST(NULL AS Nullable(Date))) as due_date_only,

        row_number,
        source_links,
        extracted_at,
        source_system,
        endpoint,

        amount_raw,
        balance_raw,
        tax_total_raw,
        approved_for_payment_raw,
        hold_raw,
        is_tax_valid_raw,
        bill_date_raw,
        due_date_raw,
        last_modified_datetime_raw
    FROM dedup
),

bill_business_logic AS (
    SELECT
        *,
        multiIf(
            bill_type = 'CREDIT ADJ.', 'Credit Adjustment',
            bill_type = 'BILL' OR bill_type = '', 'Regular Bill',
            bill_type
        ) as bill_category,
        multiIf(
            status = 'CLOSED', 'Closed',
            status = 'OPEN', 'Open',
            status = 'ON HOLD', 'On Hold',
            status
        ) as status_category,
        multiIf(
            balance = toDecimal64('0.00', 2), 'Paid',
            status = 'CLOSED' AND balance > toDecimal64('0.00', 2), 'Closed with Balance',
            balance > toDecimal64('0.00', 2), 'Outstanding',
            'Unknown'
        ) as payment_status,
        multiIf(
            amount >= toDecimal64('10000.00', 2), 'Very Large ($10K+)',
            amount >= toDecimal64('5000.00', 2), 'Large ($5K-10K)',
            amount >= toDecimal64('1000.00', 2), 'Medium ($1K-5K)',
            amount > toDecimal64('0.00', 2), 'Small (<$1K)',
            'Zero/Credit'
        ) as amount_category,
        multiIf(
            bill_date_only IS NOT NULL AND balance > toDecimal64('0.00', 2),
            dateDiff('day', bill_date_only, today()),
            CAST(NULL AS Nullable(Int32))
        ) as days_outstanding,
        multiIf(
            due_date_only IS NOT NULL AND balance > toDecimal64('0.00', 2),
            dateDiff('day', today(), due_date_only),
            CAST(NULL AS Nullable(Int32))
        ) as days_until_due,
        multiIf(
            balance = toDecimal64('0.00', 2), 'Paid',
            due_date_only IS NULL, 'No Due Date',
            due_date_only >= today(), 'Current',
            dateDiff('day', due_date_only, today()) <= 30, '1-30 Days Past Due',
            dateDiff('day', due_date_only, today()) <= 60, '31-60 Days Past Due',
            dateDiff('day', due_date_only, today()) <= 90, '61-90 Days Past Due',
            '90+ Days Past Due'
        ) as aging_bucket,
        multiIf(
            balance = toDecimal64('0.00', 2), 'Paid',
            hold = true, 'On Hold',
            due_date_only IS NOT NULL AND dateDiff('day', due_date_only, today()) > 30, 'Past Due',
            due_date_only IS NOT NULL AND dateDiff('day', today(), due_date_only) <= 7, 'Due Soon',
            due_date_only IS NOT NULL AND dateDiff('day', due_date_only, today()) > 90, 'Overdue 30+ Days',
            'Current'
        ) as priority_status,
        multiIf(
            payment_terms = 'COD', 'Cash on Delivery',
            payment_terms = 'NET30', 'Net 30 Days',
            payment_terms = 'NET14', 'Net 14 Days',
            payment_terms = 'NET15', 'Net 15 Days',
            payment_terms
        ) as payment_terms_category,
        multiIf(bill_date_only IS NOT NULL, toYear(bill_date_only), CAST(NULL AS Nullable(UInt16))) as bill_year,
        multiIf(bill_date_only IS NOT NULL, toMonth(bill_date_only), CAST(NULL AS Nullable(UInt8))) as bill_month,
        multiIf(bill_date_only IS NOT NULL, toQuarter(bill_date_only), CAST(NULL AS Nullable(UInt8))) as bill_quarter
    FROM bill_cleaning
)

SELECT *
FROM bill_business_logic
ORDER BY bill_date_only DESC, bill_number
