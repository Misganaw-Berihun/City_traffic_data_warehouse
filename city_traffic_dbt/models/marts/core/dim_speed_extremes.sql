with aggregate_by_id_and_time as (

    select * from {{ref('fct_aggregate_by_id_and_time')}}
),

speedExtremes as (

    select 
        a.track_id,
        max(a.avg_speed) as max_speed,
        min(a.avg_speed) as min_speed

    from aggregate_by_id_and_time as a

    group by a.track_id

)

select * from speedExtremes