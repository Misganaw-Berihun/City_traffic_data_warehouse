with trajectory as (
    
    select 
        "track_id",
        "lat",
        "lon",
        "speed",
        "lon_acc",
        "lat_acc",
        "time"

    from 
        trajectory
)

select * from trajectory