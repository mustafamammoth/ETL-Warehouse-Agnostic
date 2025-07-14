-- Cleaned and standardized customer data (PostgreSQL compatible)
{{ config(materialized='table') }}

WITH customer_cleaning AS (
    SELECT 
        customer_guid,
        customer_id,

        -- Clean customer name
        TRIM(customer_name)                         AS customer_name,

        -- Keep original email field and extract primary only
        email_raw                                   AS emails_all,

        CASE 
            WHEN email_raw LIKE '%;%'               THEN TRIM(SPLIT_PART(email_raw, ';', 1))
            ELSE TRIM(email_raw)
        END                                         AS primary_email,

        -- Count total emails
        CASE 
            WHEN email_raw LIKE '%;%'               THEN ARRAY_LENGTH(STRING_TO_ARRAY(email_raw, ';'), 1)
            WHEN email_raw IS NOT NULL
                 AND email_raw <> ''                THEN 1
            ELSE 0
        END                                         AS email_count,

        -- Clean numeric fields
        COALESCE(credit_limit_raw, 0)               AS credit_limit,
        COALESCE(write_off_limit_raw, 0)            AS write_off_limit,

        -- Keep timezone info in dates
        CASE 
            WHEN created_datetime_raw IS NOT NULL
                 AND created_datetime_raw <> ''     THEN created_datetime_raw::TIMESTAMP
            ELSE NULL
        END                                         AS created_timestamp,

        CASE 
            WHEN last_modified_datetime_raw IS NOT NULL
                 AND last_modified_datetime_raw <> '' THEN last_modified_datetime_raw::TIMESTAMP
            ELSE NULL
        END                                         AS last_modified_timestamp,

        CASE 
            WHEN created_datetime_raw IS NOT NULL
                 AND created_datetime_raw <> ''     THEN (created_datetime_raw::TIMESTAMP)::DATE
            ELSE NULL
        END                                         AS created_date,

        CASE 
            WHEN last_modified_datetime_raw IS NOT NULL
                 AND last_modified_datetime_raw <> '' THEN (last_modified_datetime_raw::TIMESTAMP)::DATE
            ELSE NULL
        END                                         AS last_modified_date,

        -- Standardize status & other text columns
        UPPER(TRIM(COALESCE(status, '')))           AS status,
        UPPER(TRIM(COALESCE(customer_class, '')))   AS customer_class,
        TRIM(COALESCE(tax_zone, ''))                AS tax_zone,
        TRIM(COALESCE(payment_terms, ''))           AS payment_terms,
        TRIM(COALESCE(account_ref, ''))             AS account_ref,
        TRIM(COALESCE(shipping_zone, ''))           AS shipping_zone,

        -- Clean notes
        CASE 
            WHEN note IS NOT NULL AND note <> ''
                 THEN REPLACE(REPLACE(note, CHR(10), ' '), CHR(13), ' ')
            ELSE NULL
        END                                         AS notes_cleaned,

        _extracted_at,
        _source_system

    FROM {{ ref('customers_raw') }}
),

customer_business_logic AS (
    SELECT
        *,
        -- Customer tiers
        CASE 
            WHEN credit_limit >= 15000              THEN 'Enterprise'
            WHEN credit_limit >= 10000              THEN 'Premium'
            WHEN credit_limit >= 5000               THEN 'Standard'
            WHEN credit_limit >  0                  THEN 'Basic'
            ELSE 'No Credit'
        END                                         AS customer_tier,

        -- Risk assessment
        CASE 
            WHEN status = 'ACTIVE' AND credit_limit > 0   THEN 'Low Risk'
            WHEN status = 'ACTIVE' AND credit_limit = 0   THEN 'Medium Risk'
            WHEN status = 'INACTIVE'                      THEN 'High Risk'
            ELSE 'Unknown'
        END                                         AS risk_category,

        -- Customer age in days
        CASE 
            WHEN created_date IS NOT NULL
                 THEN (CURRENT_DATE - created_date)
            ELSE NULL
        END                                         AS customer_age_days,

        -- Days since last modification
        CASE 
            WHEN last_modified_date IS NOT NULL
                 THEN (CURRENT_DATE - last_modified_date)
            ELSE NULL
        END                                         AS days_since_last_update,

        -- Email quality flags
        CASE 
            WHEN primary_email IS NULL OR primary_email = ''      THEN 'No Email'
            WHEN primary_email NOT LIKE '%@%'  OR primary_email NOT LIKE '%.%' THEN 'Invalid Email'
            WHEN email_count > 1                                   THEN 'Multiple Emails'
            ELSE 'Single Valid Email'
        END                                         AS email_quality

    FROM customer_cleaning
)

SELECT *
FROM   customer_business_logic
ORDER  BY customer_id
