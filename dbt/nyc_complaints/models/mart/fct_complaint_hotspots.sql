WITH base AS (
    SELECT
        c.complaint_type
        ,c.city
        ,c.borough
        ,c.unique_key
        ,c.created_date
        ,DATE_TRUNC(c.created_date, month) AS created_month
        ,NULLIF(i.median_income, 0) AS median_income
    FROM {{ ref('stg_complaints') }} c
    INNER JOIN {{ ref('stg_median_income') }} i
        ON c.incident_zip = i.zip_code
    WHERE i.median_income > 0 -- Filter out records with zero or negative median income
),

top_5_types AS (
    SELECT complaint_type
    FROM base
    GROUP BY complaint_type
    ORDER BY COUNT(*) DESC
    LIMIT 5
),

hour_counts AS (
    SELECT
        complaint_type
        ,city
        ,borough
        ,created_month
        ,EXTRACT(HOUR FROM created_date) AS hour_of_day
        ,COUNT(*) AS hour_count
    FROM base
    GROUP BY 1,2,3,4,5
),

most_common_hours AS (
    SELECT
        complaint_type
        ,city
        ,borough
        ,created_month
        ,hour_of_day AS most_common_hour_of_day
    FROM hour_counts
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY complaint_type, city, borough, created_month
        ORDER BY hour_count DESC
    ) = 1
),

aggregated AS (
    SELECT
        b.complaint_type
        ,b.city
        ,b.borough
        ,b.created_month
        ,COUNT(b.unique_key) AS total_complaints
        ,AVG(b.median_income) AS avg_income_of_reporters
    FROM base b
    INNER JOIN top_5_types t ON b.complaint_type = t.complaint_type
    GROUP BY 1,2,3,4
)

SELECT
    a.created_month
    ,a.complaint_type
    ,a.city
    ,a.borough
    ,a.total_complaints
    ,a.avg_income_of_reporters
    ,h.most_common_hour_of_day
FROM aggregated a
LEFT JOIN most_common_hours h
    ON a.complaint_type = h.complaint_type
    AND a.city = h.city
    AND a.borough = h.borough
    AND a.created_month = h.created_month
