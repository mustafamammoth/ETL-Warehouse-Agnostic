-- models/curated/repsly/photos.sql
-- Cleaned/categorized photos (silver). Keeps all logic & columns.
-- Avoid nullable columns in engine keys.

{{ config(
    materialized = 'table',
    schema       = var('silver_schema', 'repsly_silver'),
    engine       = 'MergeTree()',
    order_by     = '(photo_id, photo_date_nn)',
    partition_by = 'toYYYYMM(photo_date_nn)',
    on_schema_change = 'sync_all_columns'
) }}

WITH base AS (
    SELECT
        /* Core identifiers */
        toInt64OrNull(photo_id)        AS photo_id,
        toInt64OrNull(visit_id)        AS visit_id,
        trimBoth(client_code)          AS client_code,
        trimBoth(client_name)          AS client_name,
        trimBoth(ifNull(representative_code, ''))   AS representative_code,
        trimBoth(ifNull(representative_name, ''))   AS representative_name,

        /* Photo details */
        photo_url,
        trimBoth(ifNull(note_raw, '')) AS note,

        /* Parse Microsoft JSON date: /Date(1665059516000+0000)/ or /Date(1665059516000)/ */
        CASE 
            WHEN startsWith(date_and_time_raw, '/Date(') AND endsWith(date_and_time_raw, ')/')
            THEN toDateTime(
                    toInt64OrNull(
                        extractAll(date_and_time_raw, '/Date\\((\\d+)')[1]
                    ) / 1000
                 )
            ELSE NULL
        END AS photo_timestamp,

        /* Raw tag string */
        trimBoth(ifNull(tag, ''))      AS tags_raw,

        extracted_at,
        source_system,
        endpoint
    FROM {{ ref('photos_raw') }}
),

derived AS (
    SELECT
        *,
        toDate(photo_timestamp) AS photo_date,

        /* boolean-ish flags (UInt8) */
        (positionCaseInsensitive(tags_raw, 'Competition')    > 0) AS is_competition_photo,
        (positionCaseInsensitive(tags_raw, 'Display')        > 0) AS is_display_photo,
        (positionCaseInsensitive(tags_raw, '@marketing')     > 0) AS is_marketing_photo,
        (positionCaseInsensitive(tags_raw, 'Social Media Worthy') > 0) AS is_social_media_worthy,

        /* category logic */
        CASE 
            WHEN positionCaseInsensitive(tags_raw, 'Competition') > 0 
                 AND positionCaseInsensitive(tags_raw, 'Display') > 0
                THEN 'Competition & Display'
            WHEN positionCaseInsensitive(tags_raw, 'Competition') > 0
                THEN 'Competition Analysis'
            WHEN positionCaseInsensitive(tags_raw, 'Display') > 0
                THEN 'Display Documentation'
            WHEN positionCaseInsensitive(tags_raw, '@marketing') > 0
                THEN 'Marketing Content'
            WHEN positionCaseInsensitive(tags_raw, 'Social Media Worthy') > 0
                THEN 'Social Media Content'
            WHEN tags_raw = '' THEN 'General Documentation'
            ELSE 'Other'
        END AS photo_category,

        (note != '')                                         AS has_note,
        (position(photo_url, 'https://repsly.s3.amazonaws.com/') = 1) AS is_valid_url,

        /* hour + time of day */
        toHour(photo_timestamp) AS photo_hour,
        CASE
            WHEN photo_hour BETWEEN 6  AND 11 THEN 'Morning'
            WHEN photo_hour BETWEEN 12 AND 17 THEN 'Afternoon'
            WHEN photo_hour BETWEEN 18 AND 21 THEN 'Evening'
            ELSE 'Other'
        END AS time_of_day,

        /* priority */
        CASE
            WHEN positionCaseInsensitive(tags_raw, 'Social Media Worthy') > 0
              OR positionCaseInsensitive(tags_raw, '@marketing') > 0
                THEN 'High Priority'
            WHEN positionCaseInsensitive(tags_raw, 'Competition') > 0
                THEN 'Medium Priority'
            ELSE 'Standard'
        END AS business_priority
    FROM base
),

final AS (
    SELECT
        *,
        /* non-nullable date for engine keys */
        CAST(ifNull(photo_date, toDate('1970-01-01')) AS Date) AS photo_date_nn,
        now() AS processed_at
    FROM derived
)

SELECT *
FROM final
ORDER BY photo_timestamp DESC, photo_id
