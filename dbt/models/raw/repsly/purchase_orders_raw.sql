{{ config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='append_new_columns',
    meta={
        'description': 'Raw purchase orders data from Repsly API - append-only, all fields as strings',
        'data_source': 'repsly_api',
        'update_frequency': 'incremental'
    }
) }}

-- Bronze layer: Pure append-only storage
-- All fields stored as strings to avoid type conflicts
SELECT 
    CAST(COALESCE("PurchaseOrderID", '') AS String) AS purchase_order_id,
    
    -- Transaction and document metadata
    CAST(COALESCE("TransactionType", '') AS String) AS transaction_type,
    CAST(COALESCE("DocumentTypeID", '') AS String) AS document_type_id,
    CAST(COALESCE("DocumentTypeName", '') AS String) AS document_type_name,
    CAST(COALESCE("DocumentStatus", '') AS String) AS document_status,
    CAST(COALESCE("DocumentStatusID", '') AS String) AS document_status_id,
    CAST(COALESCE("DocumentItemAttributeCaption", '') AS String) AS document_item_attribute_caption,
    
    -- Date and time fields (Microsoft JSON format)
    CAST(COALESCE("DateAndTime", '') AS String) AS date_and_time_raw,
    CAST(COALESCE("DocumentDate", '') AS String) AS document_date_raw,
    CAST(COALESCE("DueDate", '') AS String) AS due_date_raw,
    
    -- Document information
    CAST(COALESCE("DocumentNo", '') AS String) AS document_no,
    
    -- Client information
    CAST(COALESCE("ClientCode", '') AS String) AS client_code,
    CAST(COALESCE("ClientName", '') AS String) AS client_name,
    
    -- Representative information
    CAST(COALESCE("RepresentativeCode", '') AS String) AS representative_code,
    CAST(COALESCE("RepresentativeName", '') AS String) AS representative_name,
    
    -- Item details (nested structure)
    CAST(COALESCE("Item_LineNo", '') AS String) AS item_line_no_raw,
    CAST(COALESCE("Item_ProductCode", '') AS String) AS item_product_code,
    CAST(COALESCE("Item_ProductName", '') AS String) AS item_product_name,
    CAST(COALESCE("Item_UnitAmount", '') AS String) AS item_unit_amount_raw,
    CAST(COALESCE("Item_UnitPrice", '') AS String) AS item_unit_price_raw,
    CAST(COALESCE("Item_PackageTypeCode", '') AS String) AS item_package_type_code,
    CAST(COALESCE("Item_PackageTypeName", '') AS String) AS item_package_type_name,
    CAST(COALESCE("Item_PackageTypeConversion", '') AS String) AS item_package_type_conversion_raw,
    CAST(COALESCE("Item_Quantity", '') AS String) AS item_quantity_raw,
    CAST(COALESCE("Item_Amount", '') AS String) AS item_amount_raw,
    CAST(COALESCE("Item_DiscountAmount", '') AS String) AS item_discount_amount_raw,
    CAST(COALESCE("Item_DiscountPercent", '') AS String) AS item_discount_percent_raw,
    CAST(COALESCE("Item_TaxAmount", '') AS String) AS item_tax_amount_raw,
    CAST(COALESCE("Item_TaxPercent", '') AS String) AS item_tax_percent_raw,
    CAST(COALESCE("Item_TotalAmount", '') AS String) AS item_total_amount_raw,
    CAST(COALESCE("Item_Note", '') AS String) AS item_note,
    CAST(COALESCE("Item_DocumentItemAttributeName", '') AS String) AS item_document_item_attribute_name,
    CAST(COALESCE("Item_DocumentItemAttributeID", '') AS String) AS item_document_item_attribute_id,
    
    -- Additional fields
    CAST(COALESCE("SignatureURL", '') AS String) AS signature_url,
    CAST(COALESCE("Note", '') AS String) AS note,
    CAST(COALESCE("Taxable", '') AS String) AS taxable_raw,
    CAST(COALESCE("VisitID", '') AS String) AS visit_id_raw,
    
    -- Address information
    CAST(COALESCE("StreetAddress", '') AS String) AS street_address,
    CAST(COALESCE("ZIP", '') AS String) AS zip_code,
    CAST(COALESCE("ZIPExt", '') AS String) AS zip_ext,
    CAST(COALESCE("City", '') AS String) AS city,
    CAST(COALESCE("State", '') AS String) AS state,
    CAST(COALESCE("Country", '') AS String) AS country,
    CAST(COALESCE("CountryCode", '') AS String) AS country_code,
    
    -- Custom attributes (nested structure)
    CAST(COALESCE("DocumentCustomAttributes_CustomAttributeInfoID", '') AS String) AS custom_attribute_info_id,
    CAST(COALESCE("DocumentCustomAttributes_Title", '') AS String) AS custom_attribute_title,
    CAST(COALESCE("DocumentCustomAttributes_Type", '') AS String) AS custom_attribute_type,
    CAST(COALESCE("DocumentCustomAttributes_Value", '') AS String) AS custom_attribute_value,
    
    -- Additional metadata
    CAST(COALESCE("OriginalDocumentNumber", '') AS String) AS original_document_number,

    -- System metadata
    CAST("_extracted_at" AS String) AS extracted_at,
    CAST("_source_system" AS String) AS source_system,
    CAST("_endpoint" AS String) AS endpoint,
    now() AS dbt_loaded_at,
    
    cityHash64(
        concat(
            COALESCE("PurchaseOrderID", ''),
            COALESCE("DateAndTime", ''),
            "_extracted_at"
        )
    ) AS record_hash

FROM {{ source('bronze_repsly', 'raw_purchase_orders') }}

{% if is_incremental() %}
WHERE parseDateTimeBestEffort("_extracted_at") > 
    (SELECT COALESCE(max(parseDateTimeBestEffort(extracted_at)), toDateTime('1900-01-01'))
     FROM {{ this }})
{% endif %}