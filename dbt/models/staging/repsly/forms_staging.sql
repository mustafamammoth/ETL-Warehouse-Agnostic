-- Forms staging: Clean scalars, keep Items as JSON - works for ClickHouse
{{ config(
    materialized='table',
    schema=var('silver_schema', 'repsly_silver'),
    engine='MergeTree()',
    order_by='(bronze_processed_at, processed_at)',
    partition_by='toYYYYMM(bronze_processed_at)',
    meta={
        'description': 'Cleaned forms data with structured metadata and flexible JSON items',
        'update_strategy': 'full_refresh'
    }
) }}

SELECT
    -- Core IDs
    toInt64OrNull(FormID) as form_id,
    toInt64OrNull(VisitID) as visit_id,
    
    -- Form details
    trimBoth(FormName) as form_name,
    trimBoth(ClientCode) as client_code,
    trimBoth(ClientName) as client_name,
    trimBoth(RepresentativeCode) as representative_code,
    trimBoth(RepresentativeName) as representative_name,
    
    -- Location
    trimBoth(StreetAddress) as street_address,
    trimBoth(City) as city,
    CASE 
        WHEN State = 'CA' THEN 'California'
        WHEN State = 'NY' THEN 'New York' 
        ELSE trimBoth(State) 
    END as state,
    trimBoth(Country) as country,
    trimBoth(ZIP) as zip_code,
    
    -- Contact
    trimBoth(Email) as email,
    trimBoth(Phone) as phone,
    
    -- GPS coordinates - ClickHouse compatible
    toFloat64OrNull(Longitude) as longitude,
    toFloat64OrNull(Latitude) as latitude,
    
    -- Signature
    trimBoth(SignatureURL) as signature_url,
    
    -- Parse Microsoft JSON dates: /Date(1665749886000+0000)/ 
    CASE 
        WHEN VisitStart LIKE '/Date(%' THEN
            toDateTime(toInt64(substring(VisitStart, 7, 13)) / 1000)
        ELSE NULL
    END as visit_start_ts,
    
    CASE 
        WHEN VisitEnd LIKE '/Date(%' THEN
            toDateTime(toInt64(substring(VisitEnd, 7, 13)) / 1000)
        ELSE NULL
    END as visit_end_ts,
    
    CASE 
        WHEN DateAndTime LIKE '/Date(%' THEN
            toDateTime(toInt64(substring(DateAndTime, 7, 13)) / 1000)
        ELSE NULL
    END as form_submitted_ts,
    
    -- Visit duration in minutes
    CASE 
        WHEN VisitStart LIKE '/Date(%' AND VisitEnd LIKE '/Date(%' THEN
            (toInt64(substring(VisitEnd, 7, 13)) - toInt64(substring(VisitStart, 7, 13))) / 60000
        ELSE NULL
    END as minutes_on_site,
    
    -- Keep Items as raw JSON for explosion (this is the flexible part!)
    Items as items_raw,
    
    -- System metadata
    parseDateTimeBestEffortOrNull(extracted_at) as data_extracted_at,
    dbt_loaded_at as bronze_processed_at,
    now() as processed_at,
    source_system,
    endpoint,
    record_hash

FROM {{ ref('forms_raw') }}