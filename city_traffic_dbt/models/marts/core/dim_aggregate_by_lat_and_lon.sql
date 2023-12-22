with trajectory as (

    select * from {{ref('stg_trajectory')}}
),

aggregate_by_lon_and_lat as (

    select 
        t.lat,
        t.lon,
        t.time,
        count(*) as car_count
    from 
        trajectory as t 
    group by 
        lat,
        lon, 
        time
)

select * from aggregate_by_lon_and_lat