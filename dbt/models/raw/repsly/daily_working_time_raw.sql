{{ config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='append_new_columns',
    meta={
        'description': 'Raw daily working time data from Repsly API - append-only, all fields as strings',
        'data_source': 'repsly_api',
        'update_frequency': 'incremental'
    }
) }}

-- Bronze layer: Pure append-only storage
-- All fields stored as strings to avoid type conflicts
SELECT 
    CAST(COALESCE("DailyWorkingTimeID", '') AS String) AS daily_working_time_id,
    
    -- Date and time fields (Microsoft JSON format)
    CAST(COALESCE("Date", '') AS String) AS date_raw,
    CAST(COALESCE("DateAndTimeStart", '') AS String) AS date_and_time_start_raw,
    CAST(COALESCE("DateAndTimeEnd", '') AS String) AS date_and_time_end_raw,
    
    -- Duration and metrics
    CAST(COALESCE("Length", '') AS String) AS length_raw,
    
    -- Mileage information
    CAST(COALESCE("MileageStart", '') AS String) AS mileage_start_raw,
    CAST(COALESCE("MileageEnd", '') AS String) AS mileage_end_raw,
    CAST(COALESCE("MileageTotal", '') AS String) AS mileage_total_raw,
    
    -- GPS coordinates (start location)
    CAST(COALESCE("LatitudeStart", '') AS String) AS latitude_start_raw,
    CAST(COALESCE("LongitudeStart", '') AS String) AS longitude_start_raw,
    
    -- GPS coordinates (end location)
    CAST(COALESCE("LatitudeEnd", '') AS String) AS latitude_end_raw,
    CAST(COALESCE("LongitudeEnd", '') AS String) AS longitude_end_raw,
    
    -- Representative information
    CAST(COALESCE("RepresentativeCode", '') AS String) AS representative_code,
    CAST(COALESCE("RepresentativeName", '') AS String) AS representative_name,
    
    -- Notes and categorization
    CAST(COALESCE("Note", '') AS String) AS note,
    CAST(COALESCE("Tag", '') AS String) AS tag,
    
    -- Visit metrics (these might not be in all data)
    CAST(COALESCE("NoOfVisits", '') AS String) AS no_of_visits_raw,
    CAST(COALESCE("MinOfVisits", '') AS String) AS min_of_visits_raw,
    CAST(COALESCE("MaxOfVisits", '') AS String) AS max_of_visits_raw,
    CAST(COALESCE("MinMaxVisitsTime", '') AS String) AS min_max_visits_time_raw,
    
    -- Time breakdown (these might not be in all data)
    CAST(COALESCE("TimeAtClient", '') AS String) AS time_at_client_raw,
    CAST(COALESCE("TimeInTravel", '') AS String) AS time_in_travel_raw,
    CAST(COALESCE("TimeInPause", '') AS String) AS time_in_pause_raw,

    -- System metadata
    CAST("_extracted_at" AS String) AS extracted_at,
    CAST("_source_system" AS String) AS source_system,
    CAST("_endpoint" AS String) AS endpoint,
    now() AS dbt_loaded_at,
    
    cityHash64(
        concat(
            COALESCE("DailyWorkingTimeID", ''),
            COALESCE("Date", ''),
            "_extracted_at"
        )
    ) AS record_hash

FROM {{ source('bronze_repsly', 'raw_daily_working_time') }}


{% if is_incremental() %}
WHERE parseDateTimeBestEffort("_extracted_at") > 
    (SELECT COALESCE(max(parseDateTimeBestEffort(extracted_at)), toDateTime('1900-01-01'))
     FROM {{ this }})
{% endif %}