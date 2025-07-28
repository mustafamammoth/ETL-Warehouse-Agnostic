{{ config(
    materialized        = 'incremental',
    incremental_strategy = 'append',
    on_schema_change    = 'append_new_columns',
    meta = {
        'description'     : 'Raw representatives data from Repsly API – append‑only, all fields as strings',
        'data_source'     : 'repsly_api',
        'update_frequency': 'incremental'
    }
) }}

-- Bronze layer: pure append‑only storage
-- All fields stored as strings to avoid type conflicts
SELECT
    -- ① use Code as the surrogate primary key
    CAST(COALESCE(Code, '') AS String)               AS representative_id,

    -- keep original columns
    CAST(COALESCE(Code,        '') AS String)        AS code,
    CAST(COALESCE(Name,        '') AS String)        AS name,
    CAST(COALESCE(Note,        '') AS String)        AS note,
    CAST(COALESCE(Password,    '') AS String)        AS password_raw,
    CAST(COALESCE(Email,       '') AS String)        AS email,
    CAST(COALESCE(Phone,       '') AS String)        AS phone,
    CAST(COALESCE(Mobile,      '') AS String)        AS mobile,
    CAST(COALESCE(Territories, '') AS String)        AS territories_json,
    CAST(COALESCE(Active,      '') AS String)        AS active_raw,
    CAST(COALESCE(Attributes,  '') AS String)        AS attributes_json,
    CAST(COALESCE(Address1,    '') AS String)        AS address1,
    CAST(COALESCE(Address2,    '') AS String)        AS address2,
    CAST(COALESCE(City,        '') AS String)        AS city,
    CAST(COALESCE(State,       '') AS String)        AS state,
    CAST(COALESCE(ZipCode,     '') AS String)        AS zip_code,
    CAST(COALESCE(ZipCodeExt,  '') AS String)        AS zip_code_ext,
    CAST(COALESCE(Country,     '') AS String)        AS country,
    CAST(COALESCE(CountryCode, '') AS String)        AS country_code,

    -- system metadata
    CAST(_extracted_at        AS String)             AS extracted_at,
    CAST(_source_system       AS String)             AS source_system,
    CAST(_endpoint            AS String)             AS endpoint,
    now()                                            AS dbt_loaded_at,

    -- ② hash now built on Code (our surrogate ID) instead of the missing RepresentativeID
    cityHash64(
        concat(
            COALESCE(Code, ''),
            COALESCE(Name, ''),
            _extracted_at
        )
    ) AS record_hash

FROM {{ source('bronze_repsly', 'raw_representatives') }}

{% if is_incremental() %}
WHERE parseDateTimeBestEffort(_extracted_at) >
      (
          SELECT COALESCE(
              max(parseDateTimeBestEffort(extracted_at)),
              toDateTime('1900-01-01')
          )
          FROM {{ this }}
      )
{% endif %}
