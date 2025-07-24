{{-
    config(
        materialized       = 'incremental',
        schema             = var('bronze_schema', 'repsly_bronze'),
        engine             = 'MergeTree()',
        order_by           = '(photo_id_nn, extracted_at_nn)',
        partition_by       = 'toYYYYMM(extracted_at_nn)',
        unique_key         = 'photo_id_nn',
        on_schema_change   = 'sync_all_columns'
    )
-}}

{#-- Detect if the target table already exists --#}
{%- set rel = adapter.get_relation(
        database=this.database,
        schema=this.schema,
        identifier=this.identifier
    )
-%}

WITH src AS (
    SELECT
        toString("PhotoID")            AS photo_id,
        toString("ClientCode")         AS client_code,
        toString("ClientName")         AS client_name,
        toString("Note")               AS note_raw,
        toString("DateAndTime")        AS date_and_time_raw,
        toString("PhotoURL")           AS photo_url,
        toString("RepresentativeCode") AS representative_code,
        toString("RepresentativeName") AS representative_name,
        toString("VisitID")            AS visit_id,
        toString("Tag")                AS tag,
        _extracted_at                  AS extracted_at_str,
        _source_system                 AS source_system,
        _endpoint                      AS endpoint
    FROM {{ source('repsly_raw', 'raw_photos') }}
    WHERE "PhotoID" IS NOT NULL
),
typed AS (
    SELECT
        *,
        coalesce(
            toDateTimeOrNull(extracted_at_str),
            parseDateTimeBestEffortOrNull(extracted_at_str)
        ) AS extracted_at_dt
    FROM src
),
final AS (
    SELECT
        photo_id,
        client_code,
        client_name,
        note_raw,
        date_and_time_raw,
        photo_url,
        representative_code,
        representative_name,
        visit_id,
        tag,
        extracted_at_str,
        extracted_at_dt,
        source_system,
        endpoint,
        ifNull(toInt64OrNull(photo_id), 0)                                 AS photo_id_nn,
        toDate(ifNull(extracted_at_dt, toDateTime('1970-01-01 00:00:00'))) AS extracted_at_nn
    FROM typed
)

{#-- Pre-calc the cutoff so we don't inline a subquery at the very end --#}
{%- if is_incremental() and rel is not none -%}
    {%- set cutoff_sql -%}
        (SELECT coalesce(max(extracted_at_dt), toDateTime('1970-01-01 00:00:00')) FROM {{ this }})
    {%- endset -%}
{%- endif -%}

SELECT
    photo_id,
    client_code,
    client_name,
    note_raw,
    date_and_time_raw,
    photo_url,
    representative_code,
    representative_name,
    visit_id,
    tag,
    extracted_at_str,
    extracted_at_dt,
    source_system,
    endpoint,
    photo_id_nn,
    extracted_at_nn
FROM final
WHERE 1 = 1
{% if is_incremental() and rel is not none %}
  AND extracted_at_dt > {{ cutoff_sql }}
{% endif %}
-- dummy clause keeps adapter’s LIMIT 0 happy
  AND 1 = 1
