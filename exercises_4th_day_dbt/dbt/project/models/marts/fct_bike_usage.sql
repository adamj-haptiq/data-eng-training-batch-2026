{{ config(materialized='table') }}

select
    b.bike_id,
    b.bike_model,
    sum(t.trip_cost) as total_cost,
    sum(t.trip_duration_seconds) / 60.0 as total_minutes
from {{ source('greenwheel', 'raw_trips') }} as t
inner join {{ ref('stg_bikes') }} as b
    on t.bike_id = b.bike_id
group by
    b.bike_id,
    b.bike_model
