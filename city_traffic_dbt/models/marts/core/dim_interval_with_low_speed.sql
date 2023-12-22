with trajectory as (
    select * from {{ref('stg_trajectory')}}
),
vehicle as (
    select * from {{ref('stg_vehicle')}}
),
lowSpeedInterval as (
    select 
        t.track_id,
        min(t.lat) as start_lat,
        min(t.lon) as start_lon,
        max(t.lat) as end_lat,
        max(t.lon) as end_lon,
        min(t.speed) as min_speed,
        max(t.speed) as max_speed,
        min(t.time) as start_time,
        max(t.time) as end_time
    from (
        select
            track_id,
            time,
            lat,
            lon,
            speed,
            (row_number() over (order by time) - 
            row_number() over (partition by track_id order by time)) as grp
        from 
            trajectory
        where
            speed <= 5
    ) as t
    group by 
        t.track_id,
        t.grp
)

select * from lowSpeedInterval
