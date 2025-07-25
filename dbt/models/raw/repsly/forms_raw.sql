-- Raw forms data from Repsly API - append-only, all fields as strings
{{ config(
    materialized='incremental',
    unique_key='record_hash',
    on_schema_change='append_new_columns',
    incremental_strategy='append',
    meta={
        'data_source': 'repsly_api',
        'description': 'Raw forms data from Repsly API - append-only, all fields as strings',
        'update_frequency': 'incremental'
    }
) }}

SELECT
    -- Business fields (keeping original column names as strings)
    toString(FormID) as FormID,
    toString(FormName) as FormName,
    toString(ClientCode) as ClientCode,
    toString(ClientName) as ClientName,
    toString(DateAndTime) as DateAndTime,
    toString(RepresentativeCode) as RepresentativeCode,
    toString(RepresentativeName) as RepresentativeName,
    toString(StreetAddress) as StreetAddress,
    toString(ZIP) as ZIP,
    toString(City) as City,
    toString(State) as State,
    toString(Country) as Country,
    toString(Email) as Email,
    toString(Phone) as Phone,
    toString(Longitude) as Longitude,
    toString(Latitude) as Latitude,
    toString(SignatureURL) as SignatureURL,
    toString(VisitStart) as VisitStart,
    toString(VisitEnd) as VisitEnd,
    toString(Items) as Items,
    toString(VisitID) as VisitID,
    
    -- System metadata
    toString(_extracted_at) as extracted_at,
    toString(_source_system) as source_system,
    toString(_endpoint) as endpoint,
    
    -- dbt metadata
    now() as dbt_loaded_at,
    MD5(concat(
        COALESCE(toString(FormID), ''), '|',
        COALESCE(toString(DateAndTime), ''), '|',
        COALESCE(toString(RepresentativeCode), ''), '|',
        COALESCE(toString(_extracted_at), '')
    )) as record_hash

FROM {{ source('repsly_raw', 'raw_forms') }}

{% if is_incremental() %}
    WHERE _extracted_at > (SELECT MAX(extracted_at) FROM {{ this }})
{% endif %}