{{ config(materialized='view') }}

select
    {{ dbt_utils.surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as vehicleid,
    dispatching_base_num,
    cast(PULocationID as integer) as  PULocationID,
    cast(DOLocationID as integer) as DOLocationID,
    
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    
    SR_Flag
    
from {{ source('staging','fhv_tripdata_table') }}
where EXTRACT(YEAR FROM pickup_datetime) IN (2019)

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}