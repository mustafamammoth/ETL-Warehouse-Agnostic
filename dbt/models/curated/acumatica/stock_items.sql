-- Cleaned and standardized stock items data
{{ config(materialized='table') }}

WITH item_cleaning AS (
    SELECT 
        item_guid,
        inventory_id,
        
        -- Clean text fields
        TRIM(COALESCE(description, '')) as description,
        UPPER(TRIM(COALESCE(item_class, ''))) as item_class,
        UPPER(TRIM(COALESCE(item_type, ''))) as item_type,
        UPPER(TRIM(COALESCE(item_status, ''))) as item_status,
        UPPER(TRIM(COALESCE(base_uom, ''))) as base_uom,
        UPPER(TRIM(COALESCE(purchase_uom, ''))) as purchase_uom,
        UPPER(TRIM(COALESCE(sales_uom, ''))) as sales_uom,
        UPPER(TRIM(COALESCE(volume_uom, ''))) as volume_uom,
        UPPER(TRIM(COALESCE(weight_uom, ''))) as weight_uom,
        TRIM(COALESCE(default_warehouse_id, '')) as default_warehouse_id,
        TRIM(COALESCE(default_issue_location_id, '')) as default_issue_location_id,
        TRIM(COALESCE(default_receipt_location_id, '')) as default_receipt_location_id,
        TRIM(COALESCE(posting_class, '')) as posting_class,
        TRIM(COALESCE(lot_serial_class, '')) as lot_serial_class,
        TRIM(COALESCE(price_class, '')) as price_class,
        UPPER(TRIM(COALESCE(valuation_method, ''))) as valuation_method,
        UPPER(TRIM(COALESCE(tax_category, ''))) as tax_category,
        UPPER(TRIM(COALESCE(country_of_origin, ''))) as country_of_origin,
        TRIM(COALESCE(tariff_code, '')) as tariff_code,
        UPPER(TRIM(COALESCE(abc_code, ''))) as abc_code,
        UPPER(TRIM(COALESCE(availability, ''))) as availability,
        UPPER(TRIM(COALESCE(visibility, ''))) as visibility,
        TRIM(COALESCE(content, '')) as content,
        TRIM(COALESCE(image_url, '')) as image_url,
        
        -- Clean notes
        CASE 
            WHEN note IS NOT NULL AND note != '' THEN 
                REPLACE(REPLACE(TRIM(note), CHR(10), ' '), CHR(13), ' ')
            ELSE NULL
        END as notes_cleaned,
        
        -- Clean pricing fields (convert TEXT to proper decimals)
        CASE 
            WHEN default_price_raw IS NOT NULL AND default_price_raw != '' AND default_price_raw != 'NULL'
            THEN CAST(default_price_raw AS DECIMAL(15,2))
            ELSE 0.00
        END as default_price,
        
        CASE 
            WHEN current_std_cost_raw IS NOT NULL AND current_std_cost_raw != '' AND current_std_cost_raw != 'NULL'
            THEN CAST(current_std_cost_raw AS DECIMAL(15,2))
            ELSE 0.00
        END as current_std_cost,
        
        CASE 
            WHEN last_std_cost_raw IS NOT NULL AND last_std_cost_raw != '' AND last_std_cost_raw != 'NULL'
            THEN CAST(last_std_cost_raw AS DECIMAL(15,2))
            ELSE 0.00
        END as last_std_cost,
        
        CASE 
            WHEN pending_std_cost_raw IS NOT NULL AND pending_std_cost_raw != '' AND pending_std_cost_raw != 'NULL'
            THEN CAST(pending_std_cost_raw AS DECIMAL(15,2))
            ELSE 0.00
        END as pending_std_cost,
        
        CASE 
            WHEN msrp_raw IS NOT NULL AND msrp_raw != '' AND msrp_raw != 'NULL'
            THEN CAST(msrp_raw AS DECIMAL(15,2))
            ELSE 0.00
        END as msrp,
        
        CASE 
            WHEN specific_msrp_raw IS NOT NULL AND specific_msrp_raw != '' AND specific_msrp_raw != 'NULL'
            THEN CAST(specific_msrp_raw AS DECIMAL(15,2))
            ELSE 0.00
        END as specific_msrp,
        
        CASE 
            WHEN specific_price_raw IS NOT NULL AND specific_price_raw != '' AND specific_price_raw != 'NULL'
            THEN CAST(specific_price_raw AS DECIMAL(15,2))
            ELSE 0.00
        END as specific_price,
        
        CASE 
            WHEN markup_raw IS NOT NULL AND markup_raw != '' AND markup_raw != 'NULL'
            THEN CAST(markup_raw AS DECIMAL(10,4))
            ELSE 0.0000
        END as markup,
        
        CASE 
            WHEN min_markup_raw IS NOT NULL AND min_markup_raw != '' AND min_markup_raw != 'NULL'
            THEN CAST(min_markup_raw AS DECIMAL(10,4))
            ELSE 0.0000
        END as min_markup,
        
        -- Clean dimension fields
        CASE 
            WHEN dimension_volume_raw IS NOT NULL AND dimension_volume_raw != '' AND dimension_volume_raw != 'NULL'
            THEN CAST(dimension_volume_raw AS DECIMAL(15,4))
            ELSE 0.0000
        END as dimension_volume,
        
        CASE 
            WHEN dimension_weight_raw IS NOT NULL AND dimension_weight_raw != '' AND dimension_weight_raw != 'NULL'
            THEN CAST(dimension_weight_raw AS DECIMAL(15,4))
            ELSE 0.0000
        END as dimension_weight,
        
        -- Clean boolean fields
        CASE 
            WHEN UPPER(TRIM(COALESCE(is_a_kit_raw, 'FALSE'))) = 'TRUE' THEN TRUE
            ELSE FALSE
        END as is_a_kit,
        
        CASE 
            WHEN UPPER(TRIM(COALESCE(subject_to_commission_raw, 'FALSE'))) = 'TRUE' THEN TRUE
            ELSE FALSE
        END as subject_to_commission,
        
        CASE 
            WHEN UPPER(TRIM(COALESCE(not_available_raw, 'FALSE'))) = 'TRUE' THEN TRUE
            ELSE FALSE
        END as not_available,
        
        -- Clean dates
        CASE 
            WHEN last_modified_raw IS NOT NULL AND last_modified_raw != '' 
            THEN CAST(last_modified_raw AS TIMESTAMP)
            ELSE NULL
        END as last_modified_timestamp,
        
        CASE 
            WHEN last_modified_raw IS NOT NULL AND last_modified_raw != '' 
            THEN DATE(CAST(last_modified_raw AS TIMESTAMP))
            ELSE NULL
        END as last_modified_date,
        
        source_links,
        extracted_at,
        source_system

    FROM {{ ref('stock_items_raw') }}
),

item_categorization AS (
    SELECT
        *,
        
        -- Product categorization based on InventoryID patterns
        CASE 
            WHEN inventory_id LIKE '710000BT%' THEN 'Battery'
            WHEN inventory_id LIKE '710001LR%' THEN 'Live Rosin'
            WHEN inventory_id LIKE '710000HA%' THEN 'Apparel'
            WHEN inventory_id LIKE '710%' THEN '710 Brand'
            ELSE 'Other'
        END as product_category,
        
        -- Extract product details from description
        CASE 
            WHEN description LIKE '%1g%' OR description LIKE '%1 g%' THEN '1g'
            WHEN description LIKE '%0.5g%' OR description LIKE '%.5g%' THEN '0.5g'
            WHEN description LIKE '%2g%' OR description LIKE '%2 g%' THEN '2g'
            ELSE 'Other Size'
        END as product_size,
        
        -- Brand extraction
        CASE 
            WHEN description LIKE '710:%' THEN '710'
            WHEN description LIKE 'Marketing:%' THEN 'Marketing'
            ELSE 'Unknown Brand'
        END as product_brand,
        
        -- Status standardization
        CASE 
            WHEN item_status = 'ACTIVE' THEN 'Active'
            WHEN item_status = 'INACTIVE' THEN 'Inactive'
            WHEN item_status = 'DISCONTINUED' THEN 'Discontinued'
            ELSE item_status
        END as item_status_clean,
        
        -- Inventory management classification (moved here from item_business_logic)
        CASE 
            WHEN lot_serial_class = 'NOTTRACKED' THEN 'Standard Item'
            WHEN lot_serial_class = 'LRE' THEN 'Lot Tracked'
            WHEN lot_serial_class IS NOT NULL AND lot_serial_class != '' THEN 'Serial Tracked'
            ELSE 'Unknown Tracking'
        END as tracking_method,
        
        -- Margin analysis (price vs cost) - moved here so they can be referenced later
        CASE 
            WHEN current_std_cost > 0 AND default_price > 0
            THEN ROUND(((default_price - current_std_cost) / current_std_cost) * 100, 2)
            ELSE 0.00
        END as margin_percent,
        
        CASE 
            WHEN current_std_cost > 0 AND default_price > 0
            THEN (default_price - current_std_cost)
            ELSE 0.00
        END as margin_amount

    FROM item_cleaning
),

item_business_logic AS (
    SELECT
        *,
        
        -- Product pricing analysis
        CASE 
            WHEN default_price = 0 THEN 'No Price Set'
            WHEN default_price < 10 THEN 'Low Price (<$10)'
            WHEN default_price < 25 THEN 'Medium Price ($10-25)'
            WHEN default_price < 50 THEN 'High Price ($25-50)'
            ELSE 'Premium Price ($50+)'
        END as price_tier,
        
        -- Product complexity
        CASE 
            WHEN is_a_kit = TRUE THEN 'Kit/Bundle'
            WHEN product_category = 'Live Rosin' THEN 'Complex Product'
            WHEN product_category = 'Battery' THEN 'Simple Product'
            ELSE 'Standard Product'
        END as product_complexity,
        
        -- Availability status
        CASE 
            WHEN item_status_clean = 'Active' AND availability = 'X' AND not_available = FALSE THEN 'Available'
            WHEN item_status_clean = 'Active' AND not_available = TRUE THEN 'Temporarily Unavailable'
            WHEN item_status_clean = 'Inactive' THEN 'Inactive'
            WHEN item_status_clean = 'Discontinued' THEN 'Discontinued'
            ELSE 'Unknown Status'
        END as availability_status,
        
        -- Cannabis compliance indicators (now using tracking_method which exists in this CTE)
        CASE 
            WHEN tracking_method = 'Lot Tracked' AND product_category = 'Live Rosin' THEN 'High Compliance'
            WHEN tracking_method = 'Lot Tracked' THEN 'Compliance Required'
            WHEN tracking_method = 'Standard Item' THEN 'Standard Compliance'
            ELSE 'Unknown Compliance'
        END as compliance_level,
        
        -- Data quality flags
        CASE 
            WHEN description = '' OR description IS NULL THEN 'Missing Description'
            WHEN default_price = 0 AND item_status_clean = 'Active' THEN 'Missing Price'
            WHEN current_std_cost = 0 AND item_status_clean = 'Active' THEN 'Missing Cost'
            WHEN base_uom = '' OR base_uom IS NULL THEN 'Missing UOM'
            ELSE 'Valid'
        END as data_quality_flag,
        
        -- Extract year/month for analysis
        CASE 
            WHEN last_modified_date IS NOT NULL 
            THEN EXTRACT(YEAR FROM last_modified_date)
            ELSE NULL
        END as last_modified_year,
        
        CASE 
            WHEN last_modified_date IS NOT NULL 
            THEN EXTRACT(MONTH FROM last_modified_date)
            ELSE NULL
        END as last_modified_month,
        
        -- Price validation flags
        CASE 
            WHEN default_price < current_std_cost THEN 'Price Below Cost'
            WHEN margin_percent > 1000 THEN 'Very High Margin'
            WHEN margin_percent < 10 AND default_price > 0 THEN 'Low Margin'
            ELSE 'Normal Pricing'
        END as pricing_flag

    FROM item_categorization
)

SELECT * 
FROM item_business_logic
ORDER BY product_category, inventory_id