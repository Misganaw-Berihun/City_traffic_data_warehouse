with trajectory as(

    select * from {{ref('stg_trajectory')}}
),

aggregate_by_time as (

    select
        t.time,
        avg(t.speed) as avg_speed,
        avg(t.lat_acc) as avg_lat_acc,
        avg(t.lon_acc) as avg_lon_acc,
        count(distinct t.track_id) as num_cars

    from trajectory as t

    group by t.time
)

select * from aggregate_by_time