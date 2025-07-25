{{ config(
    materialized='table',
    schema=var('silver_schema', 'repsly_silver'),
    engine='MergeTree()',
    order_by='(purchase_order_id, processed_at)',
    partition_by='toYYYYMM(processed_at)',
    meta={
        'description': 'Cleaned & enhanced purchase orders data from Repsly',
        'business_key': 'purchase_order_id',
        'update_strategy': 'latest_wins'
    }
) }}

-- Silver layer: Cleaned, typed, and deduplicated data
WITH latest_records AS (
    SELECT *,
        row_number() OVER (
            PARTITION BY purchase_order_id
            ORDER BY 
                dbt_loaded_at DESC,
                record_hash DESC
        ) AS row_num
    FROM {{ ref('purchase_orders_raw') }}
    WHERE purchase_order_id != '' AND purchase_order_id != 'NULL'
),

with_timestamps AS (
    SELECT *,
        -- Parse extracted_at (stored as string in bronze)
        parseDateTimeBestEffortOrNull(extracted_at) AS extraction_datetime,
        
        -- dbt_loaded_at is already DateTime from now() in bronze
        dbt_loaded_at AS bronze_loaded_datetime,
        
        -- Current processing time
        now() AS processed_at

    FROM latest_records
    WHERE row_num = 1
),

typed_data AS (
    SELECT
        -- Business identifiers
        purchase_order_id,

        -- Use our reliable timestamps
        extraction_datetime AS data_extracted_at,
        bronze_loaded_datetime AS bronze_processed_at,
        processed_at,

        -- Transaction and document metadata
        NULLIF(trimBoth(transaction_type), '') AS transaction_type,
        
        CASE 
            WHEN document_type_id != '' AND document_type_id != 'NULL' AND document_type_id IS NOT NULL
            THEN toInt32OrNull(document_type_id)
            ELSE NULL
        END AS document_type_id,
        
        NULLIF(trimBoth(document_type_name), '') AS document_type_name,
        NULLIF(trimBoth(document_status), '') AS document_status,
        
        CASE 
            WHEN document_status_id != '' AND document_status_id != 'NULL' AND document_status_id IS NOT NULL
            THEN toInt32OrNull(document_status_id)
            ELSE NULL
        END AS document_status_id,
        
        NULLIF(trimBoth(document_item_attribute_caption), '') AS document_item_attribute_caption,

        -- Parse Microsoft JSON date format for creation time
        CASE 
            WHEN date_and_time_raw LIKE '/Date(%' THEN
                fromUnixTimestamp(
                    toInt64(toInt64OrNull(substring(date_and_time_raw, 7, position(date_and_time_raw, ')') - 7)) / 1000)
                )
            ELSE NULL
        END AS created_datetime,

        -- Parse document date
        CASE 
            WHEN document_date_raw LIKE '/Date(%' THEN
                toDate(fromUnixTimestamp(
                    toInt64(toInt64OrNull(substring(document_date_raw, 7, position(document_date_raw, ')') - 7)) / 1000)
                ))
            ELSE NULL
        END AS document_date,

        -- Parse due date
        CASE 
            WHEN due_date_raw LIKE '/Date(%' THEN
                toDate(fromUnixTimestamp(
                    toInt64(toInt64OrNull(substring(due_date_raw, 7, position(due_date_raw, ')') - 7)) / 1000)
                ))
            ELSE NULL
        END AS due_date,

        -- Document information
        NULLIF(trimBoth(document_no), '') AS document_no,

        -- Client information
        NULLIF(trimBoth(client_code), '') AS client_code,
        NULLIF(trimBoth(client_name), '') AS client_name,

        -- Representative information
        NULLIF(trimBoth(representative_code), '') AS representative_code,
        NULLIF(trimBoth(representative_name), '') AS representative_name,

        -- Item details with proper typing
        CASE 
            WHEN item_line_no_raw != '' AND item_line_no_raw != 'NULL' AND item_line_no_raw IS NOT NULL
            THEN toInt32OrNull(item_line_no_raw)
            ELSE NULL
        END AS item_line_no,

        NULLIF(trimBoth(item_product_code), '') AS item_product_code,
        NULLIF(trimBoth(item_product_name), '') AS item_product_name,

        CASE 
            WHEN item_unit_amount_raw != '' AND item_unit_amount_raw != 'NULL' AND item_unit_amount_raw IS NOT NULL
            THEN toFloat64OrNull(item_unit_amount_raw)
            ELSE NULL
        END AS item_unit_amount,

        CASE 
            WHEN item_unit_price_raw != '' AND item_unit_price_raw != 'NULL' AND item_unit_price_raw IS NOT NULL
            THEN toFloat64OrNull(item_unit_price_raw)
            ELSE NULL
        END AS item_unit_price,

        NULLIF(trimBoth(item_package_type_code), '') AS item_package_type_code,
        NULLIF(trimBoth(item_package_type_name), '') AS item_package_type_name,

        CASE 
            WHEN item_package_type_conversion_raw != '' AND item_package_type_conversion_raw != 'NULL' AND item_package_type_conversion_raw IS NOT NULL
            THEN toInt32OrNull(item_package_type_conversion_raw)
            ELSE NULL
        END AS item_package_type_conversion,

        CASE 
            WHEN item_quantity_raw != '' AND item_quantity_raw != 'NULL' AND item_quantity_raw IS NOT NULL
            THEN toInt32OrNull(item_quantity_raw)
            ELSE NULL
        END AS item_quantity,

        CASE 
            WHEN item_amount_raw != '' AND item_amount_raw != 'NULL' AND item_amount_raw IS NOT NULL
            THEN toFloat64OrNull(item_amount_raw)
            ELSE NULL
        END AS item_amount,

        CASE 
            WHEN item_discount_amount_raw != '' AND item_discount_amount_raw != 'NULL' AND item_discount_amount_raw IS NOT NULL
            THEN toFloat64OrNull(item_discount_amount_raw)
            ELSE NULL
        END AS item_discount_amount,

        CASE 
            WHEN item_discount_percent_raw != '' AND item_discount_percent_raw != 'NULL' AND item_discount_percent_raw IS NOT NULL
            THEN toFloat64OrNull(item_discount_percent_raw)
            ELSE NULL
        END AS item_discount_percent,

        CASE 
            WHEN item_tax_amount_raw != '' AND item_tax_amount_raw != 'NULL' AND item_tax_amount_raw IS NOT NULL
            THEN toFloat64OrNull(item_tax_amount_raw)
            ELSE NULL
        END AS item_tax_amount,

        CASE 
            WHEN item_tax_percent_raw != '' AND item_tax_percent_raw != 'NULL' AND item_tax_percent_raw IS NOT NULL
            THEN toFloat64OrNull(item_tax_percent_raw)
            ELSE NULL
        END AS item_tax_percent,

        CASE 
            WHEN item_total_amount_raw != '' AND item_total_amount_raw != 'NULL' AND item_total_amount_raw IS NOT NULL
            THEN toFloat64OrNull(item_total_amount_raw)
            ELSE NULL
        END AS item_total_amount,

        NULLIF(trimBoth(item_note), '') AS item_note,
        NULLIF(trimBoth(item_document_item_attribute_name), '') AS item_document_item_attribute_name,
        NULLIF(trimBoth(item_document_item_attribute_id), '') AS item_document_item_attribute_id,

        -- Additional fields
        NULLIF(trimBoth(signature_url), '') AS signature_url,
        
        CASE 
            WHEN note IS NOT NULL AND note != '' AND note != 'NULL'
            THEN replaceRegexpAll(trimBoth(note), '[\\r\\n]+', ' ')
            ELSE NULL
        END AS notes_cleaned,

        -- Boolean taxable field
        multiIf(
            lower(taxable_raw) IN ('true','1','t','yes'), 1,
            lower(taxable_raw) IN ('false','0','f','no'), 0,
            NULL
        ) AS is_taxable,

        CASE 
            WHEN visit_id_raw != '' AND visit_id_raw != 'NULL' AND visit_id_raw IS NOT NULL
            THEN toInt32OrNull(visit_id_raw)
            ELSE NULL
        END AS visit_id,

        -- Address information
        NULLIF(trimBoth(street_address), '') AS street_address,
        NULLIF(trimBoth(zip_code), '') AS zip_code,
        NULLIF(trimBoth(zip_ext), '') AS zip_ext,
        NULLIF(trimBoth(city), '') AS city,
        NULLIF(trimBoth(state), '') AS state,
        NULLIF(trimBoth(country), '') AS country,
        NULLIF(trimBoth(country_code), '') AS country_code,

        -- Custom attributes
        NULLIF(trimBoth(custom_attribute_info_id), '') AS custom_attribute_info_id,
        NULLIF(trimBoth(custom_attribute_title), '') AS custom_attribute_title,
        NULLIF(trimBoth(custom_attribute_type), '') AS custom_attribute_type,
        NULLIF(trimBoth(custom_attribute_value), '') AS custom_attribute_value,

        -- Additional metadata
        NULLIF(trimBoth(original_document_number), '') AS original_document_number,

        -- System metadata
        source_system,
        endpoint,
        record_hash

    FROM with_timestamps
),

enhanced AS (
    SELECT
        *,
        
        -- Derived address field
        CASE 
            WHEN street_address IS NOT NULL OR city IS NOT NULL OR state IS NOT NULL OR country IS NOT NULL THEN
                arrayStringConcat(
                    arrayFilter(x -> x != '', [
                        COALESCE(street_address, ''),
                        COALESCE(city, ''),
                        CASE 
                            WHEN state IS NOT NULL AND zip_code IS NOT NULL 
                            THEN concat(state, ' ', zip_code, COALESCE(concat('-', zip_ext), ''))
                            WHEN state IS NOT NULL 
                            THEN state
                            WHEN zip_code IS NOT NULL 
                            THEN concat(zip_code, COALESCE(concat('-', zip_ext), ''))
                            ELSE ''
                        END,
                        COALESCE(country, '')
                    ]),
                    ', '
                )
            ELSE NULL
        END AS full_address,

        -- Date analysis
        CASE WHEN document_date IS NOT NULL THEN toDayOfWeek(document_date) ELSE NULL END AS document_day_of_week,
        CASE WHEN document_date IS NOT NULL THEN toYear(document_date) ELSE NULL END AS document_year,
        CASE WHEN document_date IS NOT NULL THEN toMonth(document_date) ELSE NULL END AS document_month,

        -- Due date analysis
        CASE 
            WHEN document_date IS NOT NULL AND due_date IS NOT NULL
            THEN dateDiff('day', document_date, due_date)
            ELSE NULL
        END AS days_until_due,

        -- Financial calculations
        CASE 
            WHEN item_amount IS NOT NULL AND item_discount_amount IS NOT NULL AND item_amount > 0
            THEN round((item_discount_amount * 100.0) / item_amount, 2)
            ELSE NULL
        END AS calculated_discount_percentage,

        CASE 
            WHEN item_amount IS NOT NULL AND item_tax_amount IS NOT NULL AND item_amount > 0
            THEN round((item_tax_amount * 100.0) / item_amount, 2)
            ELSE NULL
        END AS calculated_tax_percentage,

        -- Data quality flags
        CASE WHEN document_date IS NOT NULL THEN 1 ELSE 0 END AS has_document_date,
        CASE WHEN due_date IS NOT NULL THEN 1 ELSE 0 END AS has_due_date,
        CASE WHEN client_code IS NOT NULL THEN 1 ELSE 0 END AS has_client,
        CASE WHEN representative_code IS NOT NULL THEN 1 ELSE 0 END AS has_representative,
        CASE WHEN item_product_code IS NOT NULL THEN 1 ELSE 0 END AS has_product,
        CASE WHEN item_total_amount IS NOT NULL AND item_total_amount > 0 THEN 1 ELSE 0 END AS has_amount,
        CASE WHEN signature_url IS NOT NULL THEN 1 ELSE 0 END AS has_signature,
        CASE WHEN visit_id IS NOT NULL THEN 1 ELSE 0 END AS has_visit_link

    FROM typed_data
),

business_logic AS (
    SELECT
        *,
        
        -- Document status categories
        CASE 
            WHEN document_status IS NULL THEN 'No Status'
            WHEN lower(document_status) LIKE '%draft%' OR lower(document_status) LIKE '%pending%' THEN 'Draft/Pending'
            WHEN lower(document_status) LIKE '%approved%' OR lower(document_status) LIKE '%confirmed%' THEN 'Approved'
            WHEN lower(document_status) LIKE '%completed%' OR lower(document_status) LIKE '%delivered%' THEN 'Completed'
            WHEN lower(document_status) LIKE '%cancelled%' OR lower(document_status) LIKE '%rejected%' THEN 'Cancelled'
            ELSE 'Other'
        END AS document_status_category,

        -- Day of week name
        CASE document_day_of_week
            WHEN 1 THEN 'Monday'
            WHEN 2 THEN 'Tuesday'
            WHEN 3 THEN 'Wednesday'
            WHEN 4 THEN 'Thursday'
            WHEN 5 THEN 'Friday'
            WHEN 6 THEN 'Saturday'
            WHEN 7 THEN 'Sunday'
            ELSE NULL
        END AS document_day_name,

        -- Work day type
        CASE 
            WHEN document_day_of_week IN (1,2,3,4,5) THEN 'Weekday'
            WHEN document_day_of_week IN (6,7) THEN 'Weekend'
            ELSE NULL
        END AS document_day_type,

        -- Due date urgency
        CASE 
            WHEN days_until_due IS NULL THEN 'No Due Date'
            WHEN days_until_due < 0 THEN 'Overdue'
            WHEN days_until_due = 0 THEN 'Due Today'
            WHEN days_until_due <= 7 THEN 'Due This Week'
            WHEN days_until_due <= 30 THEN 'Due This Month'
            ELSE 'Due Later'
        END AS due_date_urgency,

        -- Order value categories
        CASE 
            WHEN item_total_amount IS NULL THEN 'No Amount'
            WHEN item_total_amount = 0 THEN 'Zero Value'
            WHEN item_total_amount < 100 THEN 'Small (<$100)'
            WHEN item_total_amount < 1000 THEN 'Medium ($100-$1K)'
            WHEN item_total_amount < 10000 THEN 'Large ($1K-$10K)'
            ELSE 'Very Large (>$10K)'
        END AS order_value_category,

        -- Quantity categories
        CASE 
            WHEN item_quantity IS NULL THEN 'No Quantity Data'
            WHEN item_quantity = 0 THEN 'Zero Quantity'
            WHEN item_quantity = 1 THEN 'Single Item'
            WHEN item_quantity <= 10 THEN 'Small Order (2-10)'
            WHEN item_quantity <= 50 THEN 'Medium Order (11-50)'
            ELSE 'Large Order (50+)'
        END AS quantity_category,

        -- Discount categories
        CASE 
            WHEN item_discount_amount IS NULL OR item_discount_amount = 0 THEN 'No Discount'
            WHEN item_discount_amount < 10 THEN 'Small Discount (<$10)'
            WHEN item_discount_amount < 100 THEN 'Medium Discount ($10-$100)'
            ELSE 'Large Discount (>$100)'
        END AS discount_category,

        -- Data completeness score (0-100)
        (
            CASE WHEN purchase_order_id IS NOT NULL THEN 10 ELSE 0 END +
            CASE WHEN document_date IS NOT NULL THEN 15 ELSE 0 END +
            CASE WHEN client_code IS NOT NULL THEN 15 ELSE 0 END +
            CASE WHEN representative_code IS NOT NULL THEN 10 ELSE 0 END +
            CASE WHEN item_product_code IS NOT NULL THEN 15 ELSE 0 END +
            CASE WHEN item_total_amount IS NOT NULL AND item_total_amount > 0 THEN 15 ELSE 0 END +
            CASE WHEN document_status IS NOT NULL THEN 10 ELSE 0 END +
            CASE WHEN full_address IS NOT NULL THEN 5 ELSE 0 END +
            CASE WHEN signature_url IS NOT NULL THEN 5 ELSE 0 END
        ) AS data_completeness_score

    FROM enhanced
)

SELECT *
FROM business_logic
ORDER BY purchase_order_id, processed_at