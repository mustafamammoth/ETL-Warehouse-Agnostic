-- Raw visit schedules extended data with basic filtering and renaming (consistent TEXT approach)
-- dbt/models/raw/repsly/visit_schedules_extended_raw.sql

{{ config(materialized='view') }}

SELECT
    "ScheduleCode"::TEXT as schedule_code,
    "ScheduledDate"::TEXT as scheduled_date_raw,
    "ScheduledTime"::TEXT as scheduled_time_raw,
    "UserID"::TEXT as user_id,
    "RepresentativeName"::TEXT as representative_name,
    "ClientCode"::TEXT as client_code,
    "ClientName"::TEXT as client_name,
    "StreetAddress"::TEXT as street_address,
    "ZIP"::TEXT as zip_code,
    "ZIPExt"::TEXT as zip_ext,
    "City"::TEXT as city,
    "State"::TEXT as state,
    "Country"::TEXT as country,
    "DueDate"::TEXT as due_date_raw,
    "VisitNote"::TEXT as visit_note,
    "ProjectName"::TEXT as project_name,
    "ScheduledDuration"::TEXT as scheduled_duration_raw,
    "RepeatEveryWeeks"::TEXT as repeat_every_weeks_raw,
    "RepeatDays"::TEXT as repeat_days_raw,
    "RepeatEndDate"::TEXT as repeat_end_date_raw,
    "AlertUsersIfMissed"::TEXT as alert_users_if_missed_raw,
    "AlertUsersIfLate"::TEXT as alert_users_if_late_raw,
    "AlertUsersIfDone"::TEXT as alert_users_if_done_raw,
    "ExternalID"::TEXT as external_id,
    "Tasks"::TEXT as tasks_raw,
    "_extracted_at" as extracted_at,
    "_source_system" as source_system,
    "_endpoint" as endpoint

FROM {{ source('repsly_raw', 'raw_visit_schedules_extended') }}
WHERE "ScheduleCode" IS NOT NULL