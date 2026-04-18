SELECT
    DATE_TRUNC(complaints.created_date, month) AS created_month
    ,complaints.borough
    ,complaints.city
    ,complaints.agency
    ,complaints.agency_name
    ,COUNT(complaints.unique_key) AS total_cases
    ,COUNT(CASE WHEN LOWER(complaints.status) = 'closed' THEN 1 END) AS total_cases_closed
    ,SUM(CASE WHEN LOWER(complaints.status) = 'closed' THEN complaints.resolution_time_days ELSE 0 END) AS total_resolution_days
    ,SUM(complaints.SLA_days) AS total_sla_days
FROM {{ ref('stg_complaints') }} complaints
GROUP BY 1,2,3,4,5
