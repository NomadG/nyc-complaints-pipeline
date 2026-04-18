-- Refunds have a negative amount, so the total amount should always be >= 0.
-- Therefore return records where total_amount < 0 to make the test fail.
select
    zip_code,
    sum(median_income) as total_amount
from {{ ref('stg_median_income') }}
group by 1
having total_amount < 0