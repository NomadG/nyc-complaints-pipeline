{{ config(
    materialized='table'
    ) }}


select
    income.zip_code
    ,income.median_income  
from {{ source('raw', 'median_income') }} income