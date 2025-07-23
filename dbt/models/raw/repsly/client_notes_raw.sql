-- Raw client notes data - BRONZE LAYER (preserve all raw data)
{{ config(
    materialized='incremental',
    schema=var('raw_schema', 'repsly_bronze'),
    unique_key='client_note_id',
    incremental_strategy='append',
    on_schema_change='append_new_columns',
    engine='MergeTree()',
    order_by='(dbt_extracted_at, client_note_id)'
) }}

WITH source_data AS (
    SELECT 
        COALESCE("ClientNoteID", '')                                 AS client_note_id,
        COALESCE("TimeStamp", '')                                    AS timestamp_raw,
        COALESCE("DateAndTime", '')                                  AS date_and_time_raw,
        COALESCE("RepresentativeCode", '')                           AS representative_code,
        COALESCE("RepresentativeName", '')                           AS representative_name,
        COALESCE("ClientCode", '')                                   AS client_code,
        COALESCE("ClientName", '')                                   AS client_name,
        COALESCE("StreetAddress", '')                                AS street_address,
        COALESCE("ZIP", '')                                          AS zip_code,
        COALESCE("ZIPExt", '')                                       AS zip_ext,
        COALESCE("City", '')                                         AS city,
        COALESCE("State", '')                                        AS state,
        COALESCE("Country", '')                                      AS country,
        COALESCE("Email", '')                                        AS email,
        COALESCE("Phone", '')                                        AS phone,
        COALESCE("Mobile", '')                                       AS mobile,
        COALESCE("Territory", '')                                    AS territory,
        COALESCE("Longitude", '')                                    AS longitude_raw,
        COALESCE("Latitude", '')                                     AS latitude_raw,
        "Note"                                                       AS note_raw,
        COALESCE("VisitID", '')                                      AS visit_id,
        "_extracted_at"                                              AS extracted_at,
        "_source_system"                                             AS source_system,
        "_endpoint"                                                  AS endpoint,
        parseDateTimeBestEffort("_extracted_at")                     AS dbt_extracted_at,
        now()                                                        AS dbt_updated_at,
        cityHash64(
            concat(
                COALESCE("ClientNoteID", ''),
                COALESCE("ClientCode", ''),
                COALESCE("DateAndTime", ''),
                left(COALESCE("Note", ''), 100)
            )
        )                                                            AS record_hash
    FROM {{ source('repsly_raw', 'raw_client_notes') }}
    WHERE "ClientNoteID" IS NOT NULL 
      AND "ClientNoteID" != ''
      AND "ClientNoteID" != 'NULL'
),

deduplicated_data AS (
    SELECT *,
        row_number() OVER (
            PARTITION BY client_note_id 
            ORDER BY dbt_extracted_at DESC, record_hash DESC
        ) AS rn
    FROM source_data
)

SELECT 
    client_note_id,
    timestamp_raw,
    date_and_time_raw,
    representative_code,
    representative_name,
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
    territory,
    longitude_raw,
    latitude_raw,
    note_raw,
    visit_id,
    extracted_at,
    source_system,
    endpoint,
    dbt_extracted_at,
    dbt_updated_at,
    record_hash
FROM deduplicated_data
WHERE rn = 1

{% if is_incremental() %}
  AND dbt_extracted_at > (
        SELECT coalesce(max(dbt_extracted_at), toDateTime('1900-01-01'))
        FROM {{ this }}
      )
{% endif %}

ORDER BY client_note_id, dbt_extracted_at DESC
