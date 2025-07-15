-- Raw visit schedule realizations data with basic filtering and renaming (consistent TEXT approach)
-- dbt/models/raw/repsly/visit_schedule_realizations_raw.sql

{{ config(materialized='view') }}

SELECT
    "ScheduleId"::TEXT as schedule_id,
    "ProjectId"::TEXT as project_id,
    "EmployeeId"::TEXT as employee_id,
    "EmployeeCode"::TEXT as employee_code,
    "PlaceId"::TEXT as place_id,
    "PlaceCode"::TEXT as place_code,
    "ModifiedUTC"::TEXT as modified_utc_raw,
    "TimeZone"::TEXT as time_zone,
    "ScheduleNote"::TEXT as schedule_note,
    "Status"::TEXT as status_raw,
    "DateTimeStart"::TEXT as date_time_start_raw,
    "DateTimeStartUTC"::TEXT as date_time_start_utc_raw,
    "DateTimeEnd"::TEXT as date_time_end_raw,
    "DateTimeEndUTC"::TEXT as date_time_end_utc_raw,
    "PlanDateTimeStart"::TEXT as plan_date_time_start_raw,
    "PlanDateTimeStartUTC"::TEXT as plan_date_time_start_utc_raw,
    "PlanDateTimeEnd"::TEXT as plan_date_time_end_raw,
    "PlanDateTimeEndUTC"::TEXT as plan_date_time_end_utc_raw,
    "Tasks"::TEXT as tasks_raw,
    "_extracted_at" as extracted_at,
    "_source_system" as source_system,
    "_endpoint" as endpoint

FROM {{ source('repsly_raw', 'raw_visit_schedule_realizations') }}
WHERE "EmployeeId" IS NOT NULL  -- Using EmployeeId as primary identifier since ScheduleId can be NULL