-- Raw daily working time data with basic filtering and renaming (consistent TEXT approach)
{{ config(materialized='view') }}

SELECT 
    "DailyWorkingTimeID"::TEXT as daily_working_time_id,
    "Date"::TEXT as date_raw,
    "DateAndTimeStart"::TEXT as date_and_time_start_raw,
    "DateAndTimeEnd"::TEXT as date_and_time_end_raw,
    "Length"::TEXT as length_raw,
    "MileageStart"::TEXT as mileage_start_raw,
    "MileageEnd"::TEXT as mileage_end_raw,
    "MileageTotal"::TEXT as mileage_total_raw,
    "LatitudeStart"::TEXT as latitude_start_raw,
    "LongitudeStart"::TEXT as longitude_start_raw,
    "LatitudeEnd"::TEXT as latitude_end_raw,
    "LongitudeEnd"::TEXT as longitude_end_raw,
    "RepresentativeCode"::TEXT as representative_code,
    "RepresentativeName"::TEXT as representative_name,
    "Note"::TEXT as note,
    "Tag"::TEXT as tag,
    "NoOfVisits"::TEXT as no_of_visits_raw,
    "MinOfVisits"::TEXT as min_of_visits_raw,
    "MaxOfVisits"::TEXT as max_of_visits_raw,
    "MinMaxVisitsTime"::TEXT as min_max_visits_time_raw,
    "TimeAtClient"::TEXT as time_at_client_raw,
    "TimeInTravel"::TEXT as time_in_travel_raw,
    "TimeInPause"::TEXT as time_in_pause_raw,
    "_extracted_at" as extracted_at,
    "_source_system" as source_system,
    "_endpoint" as endpoint,
    "_testing_mode" as testing_mode

FROM {{ source('repsly_raw', 'raw_daily_working_time') }}
WHERE "DailyWorkingTimeID" IS NOT NULL