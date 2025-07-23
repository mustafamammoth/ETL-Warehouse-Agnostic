-- models/silver/acumatica/bills.sql
-- Cleansed + deduplicated bills (silver layer) - ClickHouse compatible with incremental support
{{ config(
    materialized='incremental',
    unique_key='bill_guid',
    on_schema_change='sync_all_columns',
    meta={
        'description': 'Cleaned and deduplicated bills data with business logic applied'
    }
) }}

WITH source_data AS (
    SELECT *
    FROM {{ ref('bills_raw') }}
    {% if is_incremental() %}
        -- Incremental filter based on extraction time or last modified time
        WHERE 
            extracted_at > (SELECT max(extracted_at) FROM {{ this }})
            OR (
                last_modified_datetime_raw IS NOT NULL 
                AND last_modified_datetime_raw != ''
                AND parseDateTime64BestEffortOrNull(last_modified_datetime_raw) > (
                    SELECT max(last_modified_timestamp) FROM {{ this }} WHERE last_modified_timestamp IS NOT NULL
                )
            )
    {% endif %}
),

parsed_timestamps AS (
    SELECT
        *,
        -- Parse timestamps with fallback logic
        multiIf(
            last_modified_datetime_raw IS NOT NULL AND last_modified_datetime_raw != '',
            parseDateTime64BestEffortOrNull(last_modified_datetime_raw),
            parseDateTime64BestEffortOrNull(extracted_at)
        ) AS _version_ts,
        
        -- Parse business dates
        multiIf(
            bill_date_raw IS NOT NULL AND bill_date_raw != '',
            parseDateTime64BestEffortOrNull(bill_date_raw),
            CAST(NULL AS Nullable(DateTime64))
        ) as bill_date,
        
        multiIf(
            due_date_raw IS NOT NULL AND due_date_raw != '',
            parseDateTime64BestEffortOrNull(due_date_raw),
            CAST(NULL AS Nullable(DateTime64))
        ) as due_date,
        
        multiIf(
            last_modified_datetime_raw IS NOT NULL AND last_modified_datetime_raw != '',
            parseDateTime64BestEffortOrNull(last_modified_datetime_raw),
            CAST(NULL AS Nullable(DateTime64))
        ) as last_modified_timestamp
    FROM source_data
),

deduplicated AS (
    -- Keep the latest version per bill identifier
    SELECT *
    FROM (
        SELECT
            *,
            row_number() OVER (
                PARTITION BY 
                    coalesce(nullif(bill_guid, ''), nullif(reference_number, ''))
                ORDER BY 
                    _version_ts DESC NULLS LAST,
                    extracted_at DESC NULLS LAST
            ) AS rn
        FROM parsed_timestamps
        WHERE coalesce(nullif(bill_guid, ''), nullif(reference_number, '')) IS NOT NULL
    ) 
    WHERE rn = 1
),

data_cleaning AS (
    SELECT 
        -- Primary keys
        bill_guid,
        reference_number as bill_number,
        
        -- Vendor information
        vendor_code,
        trimBoth(coalesce(vendor_ref, '')) as vendor_ref,
        
        -- Categorical fields with standardization
        upper(trimBoth(coalesce(bill_type, ''))) as bill_type,
        upper(trimBoth(coalesce(status, ''))) as status,
        upper(trimBoth(coalesce(currency_id, 'USD'))) as currency_id,
        
        -- Reference fields
        trimBoth(coalesce(branch_id, '')) as branch_id,
        trimBoth(coalesce(cash_account, '')) as cash_account,
        trimBoth(coalesce(location_id, '')) as location_id,
        trimBoth(coalesce(post_period, '')) as post_period,
        trimBoth(coalesce(payment_terms, '')) as payment_terms,
        trimBoth(coalesce(description, '')) as description_cleaned,

        -- Numeric fields with safe conversion
        multiIf(
            amount_raw IS NOT NULL AND amount_raw != '' AND amount_raw != 'NULL' AND amount_raw != 'nan',
            toDecimal64OrNull(amount_raw, 2),
            toDecimal64('0.00', 2)
        ) as amount,
        
        multiIf(
            balance_raw IS NOT NULL AND balance_raw != '' AND balance_raw != 'NULL' AND balance_raw != 'nan',
            toDecimal64OrNull(balance_raw, 2),
            toDecimal64('0.00', 2)
        ) as balance,
        
        multiIf(
            tax_total_raw IS NOT NULL AND tax_total_raw != '' AND tax_total_raw != 'NULL' AND tax_total_raw != 'nan',
            toDecimal64OrNull(tax_total_raw, 2),
            toDecimal64('0.00', 2)
        ) as tax_total,

        -- Boolean fields with safe conversion
        multiIf(
            upper(trimBoth(coalesce(approved_for_payment_raw, 'FALSE'))) IN ('TRUE', '1', 'YES'), 
            true, 
            false
        ) as approved_for_payment,
        
        multiIf(
            upper(trimBoth(coalesce(hold_raw, 'FALSE'))) IN ('TRUE', '1', 'YES'), 
            true, 
            false
        ) as hold,
        
        multiIf(
            upper(trimBoth(coalesce(is_tax_valid_raw, 'FALSE'))) IN ('TRUE', '1', 'YES'), 
            true, 
            false
        ) as is_tax_valid,

        -- Parsed timestamp fields
        bill_date,
        due_date,
        last_modified_timestamp,
        
        -- Date-only versions for easier analysis
        multiIf(
            bill_date IS NOT NULL,
            toDate(bill_date),
            CAST(NULL AS Nullable(Date))
        ) as bill_date_only,
        
        multiIf(
            due_date IS NOT NULL,
            toDate(due_date),
            CAST(NULL AS Nullable(Date))
        ) as due_date_only,

        -- Metadata fields
        row_number,
        source_links,
        extracted_at,
        source_system,
        endpoint,

        -- Raw fields for debugging/audit
        amount_raw,
        balance_raw,
        tax_total_raw,
        approved_for_payment_raw,
        hold_raw,
        is_tax_valid_raw,
        bill_date_raw,
        due_date_raw,
        last_modified_datetime_raw
    FROM deduplicated
),

business_logic AS (
    SELECT
        *,
        
        -- Bill category standardization
        multiIf(
            bill_type IN ('CREDIT ADJ.', 'CREDIT_ADJ', 'CREDIT'), 'Credit Adjustment',
            bill_type IN ('BILL', 'INVOICE', '') OR bill_type IS NULL, 'Regular Bill',
            bill_type IN ('DEBIT ADJ.', 'DEBIT_ADJ'), 'Debit Adjustment',
            bill_type
        ) as bill_category,
        
        -- Status standardization
        multiIf(
            status IN ('CLOSED', 'COMPLETE'), 'Closed',
            status IN ('OPEN', 'PENDING'), 'Open',
            status IN ('ON HOLD', 'HOLD'), 'On Hold',
            status IN ('CANCELLED', 'CANCELED'), 'Cancelled',
            status
        ) as status_category,
        
        -- Payment status logic
        multiIf(
            balance = toDecimal64('0.00', 2), 'Paid',
            status = 'CLOSED' AND balance > toDecimal64('0.00', 2), 'Closed with Balance',
            balance > toDecimal64('0.00', 2), 'Outstanding',
            'Unknown'
        ) as payment_status,
        
        -- Amount categorization
        multiIf(
            amount >= toDecimal64('10000.00', 2), 'Very Large ($10K+)',
            amount >= toDecimal64('5000.00', 2), 'Large ($5K-10K)',
            amount >= toDecimal64('1000.00', 2), 'Medium ($1K-5K)',
            amount > toDecimal64('0.00', 2), 'Small (<$1K)',
            'Zero/Credit'
        ) as amount_category,
        
        -- Aging calculations
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
        
        -- Aging bucket classification
        multiIf(
            balance = toDecimal64('0.00', 2), 'Paid',
            due_date_only IS NULL, 'No Due Date',
            due_date_only >= today(), 'Current',
            dateDiff('day', due_date_only, today()) <= 30, '1-30 Days Past Due',
            dateDiff('day', due_date_only, today()) <= 60, '31-60 Days Past Due',
            dateDiff('day', due_date_only, today()) <= 90, '61-90 Days Past Due',
            '90+ Days Past Due'
        ) as aging_bucket,
        
        -- Priority status for workflow management
        multiIf(
            balance = toDecimal64('0.00', 2), 'Paid',
            hold = true, 'On Hold',
            due_date_only IS NOT NULL AND dateDiff('day', due_date_only, today()) > 90, 'Critical Overdue',
            due_date_only IS NOT NULL AND dateDiff('day', due_date_only, today()) > 30, 'Past Due',
            due_date_only IS NOT NULL AND dateDiff('day', today(), due_date_only) <= 7, 'Due Soon',
            due_date_only IS NOT NULL AND dateDiff('day', today(), due_date_only) <= 0, 'Due Today',
            'Current'
        ) as priority_status,
        
        -- Payment terms standardization
        multiIf(
            payment_terms IN ('COD', 'CASH'), 'Cash on Delivery',
            payment_terms LIKE '%NET30%' OR payment_terms = 'NET30', 'Net 30 Days',
            payment_terms LIKE '%NET15%' OR payment_terms = 'NET15', 'Net 15 Days',
            payment_terms LIKE '%NET14%' OR payment_terms = 'NET14', 'Net 14 Days',
            payment_terms LIKE '%NET7%' OR payment_terms = 'NET7', 'Net 7 Days',
            payment_terms != '', payment_terms,
            'Not Specified'
        ) as payment_terms_category,
        
        -- Date-based dimensions for reporting
        multiIf(
            bill_date_only IS NOT NULL, 
            toYear(bill_date_only), 
            CAST(NULL AS Nullable(UInt16))
        ) as bill_year,
        
        multiIf(
            bill_date_only IS NOT NULL, 
            toMonth(bill_date_only), 
            CAST(NULL AS Nullable(UInt8))
        ) as bill_month,
        
        multiIf(
            bill_date_only IS NOT NULL, 
            toQuarter(bill_date_only), 
            CAST(NULL AS Nullable(UInt8))
        ) as bill_quarter,
        
        -- Financial year (assuming April-March, adjust as needed)
        multiIf(
            bill_date_only IS NOT NULL,
            multiIf(
                toMonth(bill_date_only) >= 4,
                toYear(bill_date_only),
                toYear(bill_date_only) - 1
            ),
            CAST(NULL AS Nullable(UInt16))
        ) as fiscal_year,
        
        -- Risk indicators
        multiIf(
            balance > toDecimal64('0.00', 2) AND due_date_only IS NOT NULL AND dateDiff('day', due_date_only, today()) > 90, 'High Risk',
            balance > toDecimal64('5000.00', 2) AND due_date_only IS NOT NULL AND dateDiff('day', due_date_only, today()) > 30, 'Medium Risk',
            balance > toDecimal64('0.00', 2) AND due_date_only IS NOT NULL AND dateDiff('day', due_date_only, today()) > 0, 'Low Risk',
            'No Risk'
        ) as collection_risk,
        
        -- Data quality flags
        multiIf(
            bill_date_only IS NULL, 'Missing Bill Date',
            due_date_only IS NULL, 'Missing Due Date',
            amount <= toDecimal64('0.00', 2), 'Zero/Negative Amount',
            vendor_code = '', 'Missing Vendor',
            'OK'
        ) as data_quality_flag
        
    FROM data_cleaning
)

SELECT *
FROM business_logic
WHERE 
    -- Filter out obvious data quality issues
    coalesce(bill_guid, bill_number) IS NOT NULL
    AND coalesce(bill_guid, bill_number) != ''
ORDER BY 
    last_modified_timestamp DESC NULLS LAST,
    bill_date_only DESC NULLS LAST, 
    bill_number