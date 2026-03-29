with 
source as (
select 
    *
from {{ source("raw_data", "results_raw")}}
)

    -- see https://docs.fastf1.dev/api_reference/api_autogen/fastf1.core.SessionResults.html
select
    -- identification info
    cast(result_id as int64) as result_id,
    cast(year as int64) as year,
    cast(event_id as int64) as event_id_within_year,
    cast(DriverNumber as string) as driver_number,
    cast(event_name as string) as event_name,

    -- driver info (will create separate dim drivers table)
    cast(BroadcastName as string) as broadcast_name, 
    cast(DriverId as string) as driver_id,
    cast(Abbreviation as string) as driver_code,        -- possibly good for joining to laps
    cast(FirstName as string) as first_name,         
    cast(LastName as string) as last_name,          
    cast(FullName as string) as full_name,
    cast(HeadshotUrl as string) as headshot_url,    
    cast(CountryCode as string) as country_code,        -- not always available

    -- team info (will create separate dim teams table)
    cast(TeamName as string) as team_name,              -- possibly good for joining to laps
    cast(TeamColor as string) as team_color,         
    cast(TeamId as string) as team_id,               

    -- race result info
    cast(Position as int64) as finishing_position,      -- after penalties and DSQ if applicable, even if did not finish, data shows 1-20 values and null
    cast(ClassifiedPosition as string) as classified_position, -- integer (if finished, 1-20) or str (if not); could use dedicated dim table: 
    /*
    “R” (retired), “D” (disqualified), “E” (excluded), “W” (withdrawn), “F” (failed to qualify) or “N” (not classified).
    */
    --> will create adjused column in fct table with 999 for not finished values (maybe even brake down into 998, 997, ...)
    cast(GridPosition as int64) as grid_position,
    -- cast(Q1 as float64) as q1_time_seconds,          -- only availble for quali and sprint sessions
    -- cast(Q2 as float64) as q2_time_seconds,          -- only availble for quali and sprint sessions
    -- cast(Q3 as float64) as q3_time_seconds,          -- only availble for quali and sprint sessions
    cast(Time as float64) as gap_or_total_driver_race_time_seconds,
    -- this value is incorrectly decribed in api docs where it says its drivers total race time
    -- BUT in reality its interval --> total race time for winner + difference between given driver and winner time for the rest (!)
    cast(Status as string) as race_status,              -- if and how finished OR reason for DNF --> possibly could be braken down into more
    cast(Points as float64) as points,
    cast(Laps as int64) as completed_laps,

    -- metadata
    cast(loaded_at as timestamp) as loaded_at

from source
where result_id is not null