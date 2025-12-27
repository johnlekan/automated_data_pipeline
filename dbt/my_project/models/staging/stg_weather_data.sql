{{config(
    materialized='table',
    unique_key='id'
)}}


with source as (
   select *
    from {{ source('dev', 'weather_data') }} 
),

duplicate as (
    select
        *,
        row_number() over (partition by time order by inserted_at desc) as rn
    from source
)

select 
    id,
    city,
    temperature,
    weather_description,
    wind_speed,
    time as weather_time_local,
    (inserted_at + (utc_offset || ' hour')::interval) as inserted_at_local
from duplicate
where rn = 1