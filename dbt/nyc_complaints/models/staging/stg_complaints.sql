{{
    config(
        materialized='incremental'
        ,unique_key='unique_key'
        ,incremental_strategy='merge'
        ,partition_by={
            "field": "created_date",
            "data_type": "timestamp",
            "granularity": "day"
        }
        ,cluster_by=['borough', 'complaint_type', 'incident_zip']
    )
}}

select
    unique_key
    , lower(trim(status)) as status
    , closed_date
    , created_date
    , due_date
    , resolution_action_updated_date
    , SLA_days
    , resolution_time_days
    , address_type 
    , agency 
    , agency_name
    , bbl
    , borough
    , UPPER(trim(city)) AS city
    , community_board
    , complaint_type
    , council_district
    , descriptor
    , descriptor_2
    , incident_zip
    , location_type
    , police_precinct
    ,  _created_at AS created_at
    , _updated_at AS updated_at
    , ingest_date
    , ingest_timestamp

from {{ source('raw', 'nyc_complaints') }}


{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  -- (uses >= to include records whose timestamp occurred since the last run of this model)
  -- (If event_time is NULL or the table is truncated, the condition will always be true and load all records)
    where ingest_date >= (select max(ingest_date) from {{ this }})

{% endif %}
    AND created_date >= '2025-01-01' -- Filter out records before January 1, 2025

QUALIFY row_number() over (partition by unique_key order by _updated_at desc) = 1