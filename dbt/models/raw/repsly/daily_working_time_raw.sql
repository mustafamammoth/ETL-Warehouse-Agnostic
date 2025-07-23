-- Raw daily working time data from Repsly API with incremental support
{{ config(
    materialized='incremental',
    schema=var('raw_schema', 'repsly_bronze'),
    unique_key='daily_working_time_id',
    incremental_strategy='append',
    on_schema_change='append_new_columns',
    engine='MergeTree()',
    order_by='(dbt_extracted_at, daily_working_time_id)'
) }}

WITH source_data AS (
    SELECT 
        -- Primary identifier
        "DailyWorkingTimeID"                                  AS daily_working_time_id,
        
        -- Date and time fields (Microsoft JSON format)
        "Date"                                                AS date_raw,
        "DateAndTimeStart"                                    AS date_and_time_start_raw,
        "DateAndTimeEnd"                                      AS date_and_time_end_raw,
        
        -- Duration and metrics
        "Length"                                              AS length_raw,
        
        -- Mileage information
        "MileageStart"                                        AS mileage_start_raw,
        "MileageEnd"                                          AS mileage_end_raw,
        "MileageTotal"                                        AS mileage_total_raw,
        
        -- GPS coordinates (start location)
        "LatitudeStart"                                       AS latitude_start_raw,
        "LongitudeStart"                                      AS longitude_start_raw,
        
        -- GPS coordinates (end location)
        "LatitudeEnd"                                         AS latitude_end_raw,
        "LongitudeEnd"                                        AS longitude_end_raw,
        
        -- Representative information
        "RepresentativeCode"                                  AS representative_code,
        "RepresentativeName"                                  AS representative_name,
        
        -- Notes and categorization
        "Note"                                                AS note,
        "Tag"                                                 AS tag,
        
        -- Visit metrics
        "NoOfVisits"                                          AS no_of_visits_raw,
        "MinOfVisits"                                         AS min_of_visits_raw,
        "MaxOfVisits"                                         AS max_of_visits_raw,
        "MinMaxVisitsTime"                                    AS min_max_visits_time_raw,
        
        -- Time breakdown
        "TimeAtClient"                                        AS time_at_client_raw,
        "TimeInTravel"                                        AS time_in_travel_raw,
        "TimeInPause"                                         AS time_in_pause_raw,
        
        -- Metadata fields (preserve all)
        "_extracted_at"                                       AS extracted_at,
        "_source_system"                                      AS source_system,
        "_endpoint"                                           AS endpoint,
        
        -- Incremental tracking fields - ClickHouse compatible
        parseDateTimeBestEffort("_extracted_at")              AS dbt_extracted_at,
        now()                                                 AS dbt_updated_at,
        
        -- Data quality indicators
        CASE WHEN "Date"               IS NOT NULL AND "Date"               != '' THEN 1 ELSE 0 END AS has_date,
        CASE WHEN "RepresentativeCode" IS NOT NULL AND "RepresentativeCode" != '' THEN 1 ELSE 0 END AS has_representative,
        CASE WHEN "Length"             IS NOT NULL AND "Length"             != '' AND "Length" != '0' THEN 1 ELSE 0 END AS has_duration

    FROM {{ source('repsly_raw', 'raw_daily_working_time') }}
    WHERE "DailyWorkingTimeID" IS NOT NULL
      AND "DailyWorkingTimeID" != ''
)

SELECT *
FROM source_data

{% if is_incremental() %}
WHERE dbt_extracted_at > (
    SELECT coalesce(max(dbt_extracted_at), toDateTime('1900-01-01 00:00:00'))
    FROM {{ this }}
)
{% endif %}
