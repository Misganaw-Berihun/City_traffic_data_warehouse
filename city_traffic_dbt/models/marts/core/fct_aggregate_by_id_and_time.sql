with trajectory as (

    select * from {{ref('stg_trajectory')}}
),

aggregate_by_id_and_time as (

    select 
        t.track_id,
        avg(t.speed) as avg_speed,
        t.time 
    from trajectory as t 
    group by t.time, t.track_id

)

select * from aggregate_by_id_and_time