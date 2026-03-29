with 
source as (
select 
    *
from {{ source("raw_data", "laps_raw")}}
)

-- see https://docs.fastf1.dev/api_reference/timing_data.html
select
    --  identification info
    cast(lap_id as int64) as lap_id,
    cast(year as int64) as year,
    cast(event_id as int64) as event_id_within_year,
    cast(Driver as string) as driver_code,
    cast(DriverNumber as string) as driver_number,
    cast(Team as string) as team_name,                  -- good candidate for its own dimension table

    -- session info
    cast(Time as float64) as total_session_time_seconds,

    -- lap info
    cast(LapTime as float64) as lap_time_seconds,
    cast(LapStartTime as float64) as lap_start_time_seconds,
    cast(LapNumber as int64) as lap_number,
    cast(Position as int64) as lap_end_position,    -- NaN for FP1, FP2, FP3, Sprint Shootout, and Qualifying as well as for crash laps.
    cast(Stint as int64) as stint_nr,
    cast(PitOutTime as float64) as pit_out_time_seconds,
    cast(PitInTime as float64) as pit_in_time_seconds,
    cast(Sector1Time as float64) as sector_1_time_seconds,
    cast(Sector2Time as float64) as sector_2_time_seconds,
    cast(Sector3Time as float64) as sector_3_time_seconds,
    cast(Sector1SessionTime as float64) as time_of_setting_sector_1_time_seconds,
    cast(Sector2SessionTime as float64) as time_of_setting_sector_2_time_seconds,
    cast(Sector3SessionTime as float64) as time_of_setting_sector_3_time_seconds,

    -- speed info
    cast(SpeedI1 as float64) as trap_sector_1_speed,
    cast(SpeedI2 as float64) as trap_sector_2_speed,
    cast(SpeedFL as float64) as trap_finish_line_speed,
    cast(SpeedST as float64) as trap_high_speed_zone_speed, -- longest straight or selected point based on given circuit

    -- technical info
    cast(IsPersonalBest as bool) as is_personal_best,             -- counted only for valid laps
    cast(Compound as string) as compound,                         -- SOFT, MEDIUM, HARD, INTERMEDIATE, WET, TEST_UNKNOWN, UNKNOWN.
    cast(TyreLife as int64) as tyre_life_in_laps,                 -- incl. laps on other sessions if driven
    cast(FreshTyre as bool) as is_fresh_tyre,                     -- = tire was new put on this lap
    cast(TrackStatus as string) as track_status_concat,           -- lenght of up to 5 chars in data --> should create bool cols (e.g. is_SC)
    /*
      ‘1’: Track clear (beginning of session or to indicate the end
      of another status)
      ‘2’: Yellow flag (sectors are unknown)
      ‘3’: ??? Never seen so far, does not exist?
      ‘4’: Safety Car
      ‘5’: Red Flag
      ‘6’: Virtual Safety Car deployed
      ‘7’: Virtual Safety Car ending (As indicated on the drivers steering wheel, on tv and so on; status ‘1’ will mark the actual end)
    */
    -- cast(Deleted as bool) as is_deleted,             -- removing due to missing data (requires load of race control info); not needed
    -- cast(DeletedReason as string) as deleted_reason, -- removing due to missing data (requires load of race control info); not needed
    cast(FastF1Generated as bool) as is_fastf1_generated, -- interpolated and limited data
    cast(IsAccurate as bool) as is_accurate,              -- if lap is time synced with others
    cast(loaded_at as timestamp) as loaded_at

from source
where lap_id is not null