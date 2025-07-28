{{ config(
    materialized = 'table',
    schema       = var('silver_schema', 'repsly_silver'),
    engine       = 'MergeTree()',
    order_by     = '(photo_id, photo_date)',
    partition_by = 'toYYYYMM(photo_date)',
    on_schema_change = 'sync_all_columns',
    meta = {'description': 'Cleaned/categorized photos data'}
) }}

WITH base AS (
    SELECT
        toInt64(photo_id)                         AS photo_id,
        toInt64OrNull(visit_id)                   AS visit_id,
        trim(client_code)                         AS client_code,
        trim(client_name)                         AS client_name,
        trim(coalesce(representative_code, ''))   AS representative_code,
        trim(coalesce(representative_name, ''))   AS representative_name,
        photo_url,
        trim(coalesce(note_raw, ''))              AS note,
        date_and_time_raw,
        tag,
        extracted_at_dt                           AS extracted_at,
        source_system
    FROM {{ ref('photos_raw') }}
),

parsed as (
    SELECT
        *,
        /* Parse MS /Date(…)/ pattern - Fixed: Cast to String before toDateTimeOrNull */
        CASE
            WHEN startsWith(date_and_time_raw, '/Date(') AND endsWith(date_and_time_raw, ')/')
            THEN toDateTimeOrNull(toString(toInt64OrNull(extract(date_and_time_raw, '/Date\\((\\d+)')) / 1000))
        END                                                                         AS photo_timestamp_ms,
        /* Best effort for anything else */
        parseDateTimeBestEffortOrNull(date_and_time_raw)                            AS photo_timestamp_best
    FROM base
),

enriched AS (
    SELECT
        *,
        coalesce(photo_timestamp_ms, photo_timestamp_best)                          AS photo_timestamp,
        coalesce(toDate(coalesce(photo_timestamp_ms, photo_timestamp_best)), toDate('1900-01-01')) AS photo_date,
        trim(coalesce(tag, ''))                                                     AS tags_raw,

        positionCaseInsensitive(tags_raw, 'Competition') > 0                        AS is_competition_photo,
        positionCaseInsensitive(tags_raw, 'Display') > 0                            AS is_display_photo,
        positionCaseInsensitive(tags_raw, '@marketing') > 0                         AS is_marketing_photo,
        positionCaseInsensitive(tags_raw, 'Social Media Worthy') > 0                AS is_social_media_worthy,

        CASE 
            WHEN is_competition_photo AND is_display_photo THEN 'Competition & Display'
            WHEN is_competition_photo THEN 'Competition Analysis'
            WHEN is_display_photo THEN 'Display Documentation'
            WHEN is_marketing_photo THEN 'Marketing Content'
            WHEN is_social_media_worthy THEN 'Social Media Content'
            WHEN tags_raw = '' THEN 'General Documentation'
            ELSE 'Other'
        END                                                                          AS photo_category,

        (note != '')                                                                 AS has_note,
        match(photo_url, '^https://repsly\\.s3\\.amazonaws\\.com/.+')                AS is_valid_url,

        toHour(photo_timestamp)                                                      AS photo_hour,

        CASE
            WHEN photo_hour BETWEEN 6 AND 11  THEN 'Morning'
            WHEN photo_hour BETWEEN 12 AND 17 THEN 'Afternoon'
            WHEN photo_hour BETWEEN 18 AND 21 THEN 'Evening'
            ELSE 'Other'
        END                                                                          AS time_of_day,

        CASE
            WHEN is_social_media_worthy OR is_marketing_photo THEN 'High Priority'
            WHEN is_competition_photo THEN 'Medium Priority'
            ELSE 'Standard'
        END                                                                          AS business_priority
    FROM parsed
)

SELECT
    photo_id,
    visit_id,
    client_code,
    client_name,
    representative_code,
    representative_name,
    photo_url,
    note,
    date_and_time_raw,
    photo_timestamp,
    photo_date,
    tags_raw,
    is_competition_photo,
    is_display_photo,
    is_marketing_photo,
    is_social_media_worthy,
    photo_category,
    has_note,
    is_valid_url,
    photo_hour,
    time_of_day,
    business_priority,
    extracted_at,
    source_system,
    now() AS processed_at
FROM enriched
ORDER BY photo_timestamp DESC, photo_id