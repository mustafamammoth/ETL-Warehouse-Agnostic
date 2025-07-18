-- models/silver/stock_items.sql
-- Cleaned, deduplicated, and standardized stock items (silver layer) - ClickHouse compatible
-- DEDUPE: Keep the latest version per stock item (inventory_id / stock_item_guid) using last_modified_raw then extracted_at.

{{ config(materialized='table') }}

WITH base AS (
    SELECT
        r.*,
        -- Version timestamp: prefer last_modified_raw, else extracted_at
        multiIf(
            (last_modified_raw IS NOT NULL) AND (last_modified_raw != ''),
            parseDateTime64BestEffort(last_modified_raw),
            parseDateTime64BestEffortOrNull(extracted_at)
        ) AS _version_ts
    FROM {{ ref('stock_items_raw') }} r
),

dedup AS (
    SELECT *
    FROM (
        SELECT
            *,
            row_number() OVER (
                PARTITION BY coalesce(nullIf(stock_item_guid, ''), nullIf(inventory_id, ''))
                ORDER BY _version_ts DESC, extracted_at DESC
            ) AS rn
        FROM base
    ) 
    WHERE rn = 1
),

stock_item_cleaning AS (
    SELECT 
        stock_item_guid,
        inventory_id,
        description,

        -- Clean text fields
        trimBoth(coalesce(abc_code, '')) as abc_code,
        upper(trimBoth(coalesce(availability, ''))) as availability,
        trimBoth(coalesce(base_uom, '')) as base_uom,
        trimBoth(coalesce(cogs_account, '')) as cogs_account,
        trimBoth(coalesce(cogs_subaccount, '')) as cogs_subaccount,
        trimBoth(coalesce(content, '')) as content,
        trimBoth(coalesce(country_of_origin, '')) as country_of_origin,
        trimBoth(coalesce(default_issue_location_id, '')) as default_issue_location_id,
        trimBoth(coalesce(default_receipt_location_id, '')) as default_receipt_location_id,
        trimBoth(coalesce(default_warehouse_id, '')) as default_warehouse_id,
        trimBoth(coalesce(discount_account, '')) as discount_account,
        trimBoth(coalesce(discount_subaccount, '')) as discount_subaccount,
        trimBoth(coalesce(image_url, '')) as image_url,
        trimBoth(coalesce(inventory_account, '')) as inventory_account,
        trimBoth(coalesce(inventory_subaccount, '')) as inventory_subaccount,
        upper(trimBoth(coalesce(item_class, ''))) as item_class,
        upper(trimBoth(coalesce(item_status, ''))) as item_status,
        upper(trimBoth(coalesce(item_type, ''))) as item_type,
        trimBoth(coalesce(landed_cost_variance_account, '')) as landed_cost_variance_account,
        trimBoth(coalesce(landed_cost_variance_subaccount, '')) as landed_cost_variance_subaccount,
        trimBoth(coalesce(lot_serial_class, '')) as lot_serial_class,
        upper(trimBoth(coalesce(not_available, ''))) as not_available,
        trimBoth(coalesce(note_id, '')) as note_id,
        trimBoth(coalesce(po_accrual_account, '')) as po_accrual_account,
        trimBoth(coalesce(po_accrual_subaccount, '')) as po_accrual_subaccount,
        trimBoth(coalesce(posting_class, '')) as posting_class,
        trimBoth(coalesce(price_class, '')) as price_class,
        trimBoth(coalesce(price_manager, '')) as price_manager,
        trimBoth(coalesce(price_workgroup, '')) as price_workgroup,
        trimBoth(coalesce(product_manager, '')) as product_manager,
        trimBoth(coalesce(product_workgroup, '')) as product_workgroup,
        trimBoth(coalesce(purchase_price_variance_account, '')) as purchase_price_variance_account,
        trimBoth(coalesce(purchase_price_variance_subaccount, '')) as purchase_price_variance_subaccount,
        trimBoth(coalesce(purchase_uom, '')) as purchase_uom,
        trimBoth(coalesce(reason_code_subaccount, '')) as reason_code_subaccount,
        trimBoth(coalesce(sales_account, '')) as sales_account,
        trimBoth(coalesce(sales_subaccount, '')) as sales_subaccount,
        trimBoth(coalesce(sales_uom, '')) as sales_uom,
        trimBoth(coalesce(standard_cost_revaluation_account, '')) as standard_cost_revaluation_account,
        trimBoth(coalesce(standard_cost_revaluation_subaccount, '')) as standard_cost_revaluation_subaccount,
        trimBoth(coalesce(standard_cost_variance_account, '')) as standard_cost_variance_account,
        trimBoth(coalesce(standard_cost_variance_subaccount, '')) as standard_cost_variance_subaccount,
        trimBoth(coalesce(tariff_code, '')) as tariff_code,
        upper(trimBoth(coalesce(tax_category, ''))) as tax_category,
        upper(trimBoth(coalesce(valuation_method, ''))) as valuation_method,
        upper(trimBoth(coalesce(visibility, ''))) as visibility,
        trimBoth(coalesce(volume_uom, '')) as volume_uom,
        trimBoth(coalesce(weight_uom, '')) as weight_uom,

        -- Numeric fields
        multiIf(auto_incremental_value_raw IS NOT NULL AND auto_incremental_value_raw != '' AND auto_incremental_value_raw != 'NULL',
            toFloat64(auto_incremental_value_raw), CAST(NULL AS Nullable(Float64))) as auto_incremental_value,

        multiIf(current_std_cost_raw IS NOT NULL AND current_std_cost_raw != '' AND current_std_cost_raw != 'NULL',
            toDecimal64(current_std_cost_raw, 4), toDecimal64('0.0000', 4)) as current_std_cost,

        multiIf(cury_specific_msrp_raw IS NOT NULL AND cury_specific_msrp_raw != '' AND cury_specific_msrp_raw != 'NULL',
            toDecimal64(cury_specific_msrp_raw, 2), toDecimal64('0.00', 2)) as cury_specific_msrp,

        multiIf(cury_specific_price_raw IS NOT NULL AND cury_specific_price_raw != '' AND cury_specific_price_raw != 'NULL',
            toDecimal64(cury_specific_price_raw, 2), toDecimal64('0.00', 2)) as cury_specific_price,

        multiIf(default_price_raw IS NOT NULL AND default_price_raw != '' AND default_price_raw != 'NULL',
            toDecimal64(default_price_raw, 2), toDecimal64('0.00', 2)) as default_price,

        multiIf(dimension_volume_raw IS NOT NULL AND dimension_volume_raw != '' AND dimension_volume_raw != 'NULL',
            toDecimal64(dimension_volume_raw, 4), toDecimal64('0.0000', 4)) as dimension_volume,

        multiIf(dimension_weight_raw IS NOT NULL AND dimension_weight_raw != '' AND dimension_weight_raw != 'NULL',
            toDecimal64(dimension_weight_raw, 4), toDecimal64('0.0000', 4)) as dimension_weight,

        multiIf(last_std_cost_raw IS NOT NULL AND last_std_cost_raw != '' AND last_std_cost_raw != 'NULL',
            toDecimal64(last_std_cost_raw, 4), toDecimal64('0.0000', 4)) as last_std_cost,

        multiIf(markup_raw IS NOT NULL AND markup_raw != '' AND markup_raw != 'NULL',
            toDecimal64(markup_raw, 4), toDecimal64('0.0000', 4)) as markup,

        multiIf(min_markup_raw IS NOT NULL AND min_markup_raw != '' AND min_markup_raw != 'NULL',
            toDecimal64(min_markup_raw, 4), toDecimal64('0.0000', 4)) as min_markup,

        multiIf(msrp_raw IS NOT NULL AND msrp_raw != '' AND msrp_raw != 'NULL',
            toDecimal64(msrp_raw, 2), toDecimal64('0.00', 2)) as msrp,

        multiIf(pending_std_cost_raw IS NOT NULL AND pending_std_cost_raw != '' AND pending_std_cost_raw != 'NULL',
            toDecimal64(pending_std_cost_raw, 4), toDecimal64('0.0000', 4)) as pending_std_cost,

        -- Boolean fields
        multiIf(upper(trimBoth(coalesce(is_a_kit_raw, 'FALSE'))) = 'TRUE', true, false) as is_a_kit,
        multiIf(upper(trimBoth(coalesce(subject_to_commission_raw, 'FALSE'))) = 'TRUE', true, false) as subject_to_commission,

        -- Timestamps
        multiIf(last_modified_raw IS NOT NULL AND last_modified_raw != '',
            parseDateTime64BestEffort(last_modified_raw), CAST(NULL AS Nullable(DateTime64))) as last_modified_timestamp,
        multiIf(last_modified_raw IS NOT NULL AND last_modified_raw != '',
            toDate(parseDateTime64BestEffort(last_modified_raw)), CAST(NULL AS Nullable(Date))) as last_modified_date,

        -- Clean description
        trimBoth(coalesce(description, '')) as description_cleaned,

        -- Raw provenance
        row_number,
        source_links,
        extracted_at,
        source_system,
        endpoint,

        -- Raw refs retained
        auto_incremental_value_raw,
        current_std_cost_raw,
        cury_specific_msrp_raw,
        cury_specific_price_raw,
        default_price_raw,
        dimension_volume_raw,
        dimension_weight_raw,
        last_std_cost_raw,
        markup_raw,
        min_markup_raw,
        msrp_raw,
        pending_std_cost_raw,
        is_a_kit_raw,
        subject_to_commission_raw,
        last_modified_raw
    FROM dedup
),

stock_item_business_logic AS (
    SELECT
        *,
        -- Status
        multiIf(
            item_status = 'ACTIVE', 'Active',
            item_status = 'INACTIVE', 'Inactive',
            item_status = 'TO DISCONTINUE', 'To Discontinue',
            item_status
        ) as item_status_clean,

        -- Type
        multiIf(
            item_type = 'FINISHED GOOD', 'Finished Good',
            item_type = 'STOCK ITEM', 'Stock Item',
            item_type = 'NON-STOCK ITEM', 'Non-Stock Item',
            item_type = 'SERVICE', 'Service',
            item_type
        ) as item_type_clean,

        -- Product category classification
        multiIf(
            positionCaseInsensitive(description_cleaned, 'battery') > 0 OR positionCaseInsensitive(inventory_id, 'BT') > 0, 'Battery',
            positionCaseInsensitive(description_cleaned, 'cart') > 0 OR positionCaseInsensitive(description_cleaned, 'pod') > 0, 'Cartridge/Pod',
            positionCaseInsensitive(description_cleaned, 'rosin') > 0, 'Live Rosin',
            positionCaseInsensitive(description_cleaned, 'distillate') > 0, 'Distillate',
            positionCaseInsensitive(description_cleaned, 'flower') > 0, 'Flower',
            positionCaseInsensitive(description_cleaned, 'edible') > 0 OR positionCaseInsensitive(description_cleaned, 'gumm') > 0, 'Edibles',
            positionCaseInsensitive(description_cleaned, 'preroll') > 0, 'Pre-Roll',
            positionCaseInsensitive(description_cleaned, 'concentrate') > 0, 'Concentrate',
            positionCaseInsensitive(description_cleaned, 'packaging') > 0 OR positionCaseInsensitive(description_cleaned, 'bag') > 0 OR positionCaseInsensitive(description_cleaned, 'box') > 0, 'Packaging',
            positionCaseInsensitive(description_cleaned, 'marketing') > 0 OR positionCaseInsensitive(description_cleaned, 'hat') > 0 OR positionCaseInsensitive(description_cleaned, 'shirt') > 0, 'Marketing/Merchandise',
            'Other'
        ) as product_category,

        -- Price tiers
        multiIf(
            default_price >= toDecimal64('100.00', 2), 'Premium ($100+)',
            default_price >= toDecimal64('50.00', 2), 'High ($50-100)',
            default_price >= toDecimal64('25.00', 2), 'Medium ($25-50)',
            default_price >= toDecimal64('10.00', 2), 'Low ($10-25)',
            default_price > toDecimal64('0.00', 2), 'Budget (<$10)',
            'No Price Set'
        ) as price_tier,

        -- Margin metrics
        multiIf(
            default_price > toDecimal64('0.00', 2) AND current_std_cost > toDecimal64('0.00', 4),
            round(((toFloat64(default_price) - toFloat64(current_std_cost)) / toFloat64(default_price)) * 100, 2),
            CAST(NULL AS Nullable(Float64))
        ) as margin_percent,
        multiIf(
            default_price > toDecimal64('0.00', 2) AND current_std_cost > toDecimal64('0.00', 4),
            toFloat64(default_price) - toFloat64(current_std_cost),
            CAST(NULL AS Nullable(Float64))
        ) as margin_amount,

        -- Tracking
        multiIf(
            lot_serial_class = 'NOTTRACKED', 'Not Tracked',
            lot_serial_class = 'SERIALIZED', 'Serialized',
            lot_serial_class = 'LOT', 'Lot Tracked',
            lot_serial_class
        ) as tracking_method,

        -- Tax classification
        multiIf(
            tax_category = 'EXEMPT', 'Tax Exempt',
            tax_category = 'TAXABLE', 'Taxable',
            tax_category = 'CANNABIS', 'Cannabis Tax',
            tax_category
        ) as tax_classification,

        -- Visibility
        multiIf(
            visibility = 'X', 'Visible',
            visibility = 'HIDDEN', 'Hidden',
            visibility
        ) as visibility_status,

        -- Kit vs single
        multiIf(is_a_kit = true, 'Kit Product', 'Single Product') as kit_status,

        -- Cost variance (pct)
        multiIf(
            current_std_cost > toDecimal64('0.00', 4) AND last_std_cost > toDecimal64('0.00', 4),
            round(((toFloat64(current_std_cost) - toFloat64(last_std_cost)) / toFloat64(last_std_cost)) * 100, 2),
            CAST(NULL AS Nullable(Float64))
        ) as cost_variance_percent,

        -- Completeness flags
        multiIf(default_price > toDecimal64('0.00', 2), true, false) as has_default_price,
        multiIf(msrp > toDecimal64('0.00', 2), true, false) as has_msrp,
        multiIf(current_std_cost > toDecimal64('0.00', 4), true, false) as has_standard_cost,

        -- Brand heuristic
        multiIf(
            startsWith(inventory_id, '710'), '710 Labs',
            startsWith(inventory_id, 'HH'), 'Heavy Hitters',
            startsWith(inventory_id, 'SEQ'), 'Sequoia',
            startsWith(inventory_id, 'MAM'), 'Mammoth',
            'Other'
        ) as brand,

        -- Age metrics
        multiIf(
            last_modified_date IS NOT NULL,
            dateDiff('day', last_modified_date, today()),
            CAST(NULL AS Nullable(Int32))
        ) as days_since_last_update,

        multiIf(
            last_modified_date IS NOT NULL,
            toYear(last_modified_date),
            CAST(NULL AS Nullable(UInt16))
        ) as last_modified_year,
        multiIf(
            last_modified_date IS NOT NULL,
            toMonth(last_modified_date),
            CAST(NULL AS Nullable(UInt8))
        ) as last_modified_month,

        -- Completeness score
        (
            multiIf(inventory_id IS NOT NULL AND inventory_id != '', 15, 0) +
            multiIf(description_cleaned IS NOT NULL AND description_cleaned != '', 15, 0) +
            multiIf(default_price > toDecimal64('0.00', 2), 15, 0) +
            multiIf(current_std_cost > toDecimal64('0.00', 4), 15, 0) +
            multiIf(item_class IS NOT NULL AND item_class != '', 10, 0) +
            multiIf(item_status IS NOT NULL AND item_status != '', 10, 0) +
            multiIf(base_uom IS NOT NULL AND base_uom != '', 10, 0) +
            multiIf(tax_category IS NOT NULL AND tax_category != '', 10, 0)
        ) as data_completeness_score
    FROM stock_item_cleaning
)

SELECT *
FROM stock_item_business_logic
ORDER BY inventory_id
