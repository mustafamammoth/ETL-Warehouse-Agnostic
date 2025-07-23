-- RAW/BRONZE layer: Keep data exactly as received from API (all text fields)
{{ config(
    materialized='incremental',
    unique_key='client_id',
    incremental_strategy='merge',
    merge_exclude_columns=['dbt_updated_at'],
    on_schema_change='append_new_columns',
    meta={'description': 'Raw clients data from Repsly API - preserved exactly as received'}
) }}

WITH source_data AS (
    SELECT 
        -- Primary identifier (keep as text)
        COALESCE("ClientID", '')                          AS client_id,

        -- All API fields preserved as text
        COALESCE("ClientCode", '')                        AS client_code,
        COALESCE("ClientName", '')                        AS client_name,
        COALESCE("StreetAddress", '')                     AS street_address,
        COALESCE("ZIP", '')                               AS zip_code,
        COALESCE("ZIPExt", '')                            AS zip_ext,
        COALESCE("City", '')                              AS city,
        COALESCE("State", '')                             AS state,
        COALESCE("Country", '')                           AS country,
        COALESCE("Email", '')                             AS email,
        COALESCE("Phone", '')                             AS phone,
        COALESCE("Mobile", '')                            AS mobile,
        COALESCE("Territory", '')                         AS territory,
        COALESCE("Longitude", '')                         AS longitude_raw,
        COALESCE("Latitude", '')                          AS latitude_raw,
        COALESCE("TimeStamp", '')                         AS timestamp_raw,
        COALESCE("ModifiedDate", '')                      AS modified_date_raw,
        COALESCE("Active", '')                            AS active_raw,
        COALESCE("RepresentativeCode", '')                AS representative_code,
        COALESCE("RepresentativeName", '')                AS representative_name,
        COALESCE("ClientType", '')                        AS client_type,
        COALESCE("ClientSubType", '')                     AS client_sub_type,
        COALESCE("Notes", '')                             AS notes_raw,
        COALESCE("Website", '')                           AS website,
        COALESCE("CompanyNumber", '')                     AS company_number,
        COALESCE("TaxNumber", '')                         AS tax_number,
        COALESCE("Fax", '')                               AS fax,
        COALESCE("ContactPerson", '')                     AS contact_person,
        COALESCE("LastVisitDate", '')                     AS last_visit_date_raw,
        COALESCE("NextVisitDate", '')                     AS next_visit_date_raw,
        COALESCE("CreatedDate", '')                       AS created_date_raw,
        COALESCE("Tags", '')                              AS tags_raw,
        COALESCE("CustomFields", '')                      AS custom_fields_raw,
        COALESCE("ClientStatus", '')                      AS client_status,
        COALESCE("PaymentTerms", '')                      AS payment_terms,
        COALESCE("CreditLimit", '')                       AS credit_limit_raw,
        COALESCE("Currency", '')                          AS currency,
        COALESCE("PriceList", '')                         AS price_list,
        COALESCE("Discount", '')                          AS discount_raw,

        -- System metadata (text)
        "_extracted_at"                                   AS extracted_at,
        "_source_system"                                  AS source_system,
        "_endpoint"                                       AS endpoint,

        -- dbt processing metadata
        parseDateTimeBestEffort("_extracted_at")          AS dbt_extracted_at,
        now()                                             AS dbt_updated_at,

        -- Change detection hash
        cityHash64(
            concat(
                COALESCE("ClientID", ''),
                COALESCE("ClientCode", ''),
                COALESCE("ClientName", ''),
                COALESCE("ModifiedDate", ''),
                COALESCE("TimeStamp", '')
            )
        )                                                  AS record_hash
    FROM {{ source('repsly_raw', 'raw_clients') }}
    WHERE "ClientID" IS NOT NULL AND "ClientID" != '' AND "ClientID" != 'NULL'
),

deduplicated AS (
    SELECT *,
           row_number() OVER (
               PARTITION BY client_id
               ORDER BY dbt_extracted_at DESC, record_hash DESC
           ) AS rn
    FROM source_data
)

SELECT 
    client_id,
    client_code,
    client_name,

    street_address,
    zip_code,
    zip_ext,
    city,
    state,
    country,

    email,
    phone,
    mobile,
    fax,
    website,
    contact_person,

    territory,
    representative_code,
    representative_name,
    client_type,
    client_sub_type,
    client_status,

    longitude_raw,
    latitude_raw,

    timestamp_raw,
    modified_date_raw,
    last_visit_date_raw,
    next_visit_date_raw,
    created_date_raw,

    active_raw,
    payment_terms,
    credit_limit_raw,
    currency,
    price_list,
    discount_raw,

    notes_raw,
    tags_raw,
    custom_fields_raw,

    company_number,
    tax_number,

    extracted_at,
    source_system,
    endpoint,
    dbt_extracted_at,
    dbt_updated_at,
    record_hash
FROM deduplicated
WHERE rn = 1

{% if is_incremental() %}
  AND dbt_extracted_at >
      (SELECT COALESCE(max(dbt_extracted_at), toDateTime('1900-01-01'))
       FROM {{ this }})
{% endif %}
