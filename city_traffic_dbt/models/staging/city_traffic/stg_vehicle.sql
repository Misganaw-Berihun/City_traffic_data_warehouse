with vehicle as (

    select 
       "track_id",
        "type",
        "traveled_distance",
        "avg_speed"
    from 
        vehicle
)

select * from vehicle