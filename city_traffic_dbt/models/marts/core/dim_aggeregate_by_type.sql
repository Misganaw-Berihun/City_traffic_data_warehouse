with vehicle as (
    
    select * from {{ref('stg_vehicle')}}

), 

trajectory as (

    select * from {{ref('stg_trajectory')}}

),

aggregate_by_user as (
    select
        v.type,
        avg(t.speed) as avg_speed,
        avg(t.lat_acc) as avg_lat_acc,
        avg(t.lon_acc) as avg_lon_acc

    from vehicle as v
    
    join trajectory as t 
    on v.track_id = t.track_id 

    group by v.type

)

select * from aggregate_by_user