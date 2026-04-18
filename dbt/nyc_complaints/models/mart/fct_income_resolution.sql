with complaints as (
SELECT 
    DATE_TRUNC(complaints.created_date, month) as created_month
    ,complaints.incident_zip AS zip_code 
    ,complaints.borough 
    ,complaints.city 
    ,COUNT(complaints.unique_key) AS total_complaints
    ,COUNT(CASE WHEN LOWER(complaints.status) = 'closed' THEN 1 END) AS closed_complaints
    ,SUM(complaints.resolution_time_days) AS total_resolution_time
    ,COUNT(CASE WHEN complaints.due_date IS NOT NULL THEN 1 END) AS total_due_date
    ,SUM(complaints.sla_days) AS total_sla_days
FROM {{ ref('stg_complaints') }} complaints 
GROUP BY 1,2,3,4
), 

income as (
SELECT
    zip_code
    ,median_income
FROM {{ ref('stg_median_income') }}
WHERE median_income > 0 -- Filter out records with zero or negative median income
)

SELECT 
    complaints.created_month
    ,complaints.zip_code
    ,complaints.borough
    ,complaints.city
    ,complaints.total_complaints
    ,complaints.closed_complaints
    ,complaints.total_resolution_time
    ,complaints.total_due_date
    ,complaints.total_sla_days
    ,income.median_income
FROM complaints
INNER JOIN income
ON complaints.zip_code = income.zip_code