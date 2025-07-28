-- Create a simple test model: models/curated/repsly/test_schema.sql
{{ config(
    materialized='table'
) }}

SELECT 
    'test' as test_column,
    now() as created_at