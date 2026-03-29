{{ config(
    materialized='table',
    partition_by={
      "field": "year",
      "data_type": "int64",
      "range": {
        "start": 2018,
        "end": 2036,
        "interval": 1
      }
    },
    cluster_by=["event_id_within_year", "driver_number"]
) }}

with laps as (
    select *
    from {{ ref('fct_laps') }}
),

pit_windows as (
    select
        laps.lap_id,
        laps.year,
        laps.event_id_within_year,
        laps.driver_number,
        laps.season_driver_team_key,
        laps.track_status_concat,
        laps.lap_number,
        laps.stint_nr,
        laps.lap_time_seconds,
        laps.pit_in_time_seconds,
        laps.pit_out_time_seconds,
        laps.compound,
        laps.tyre_life_in_laps,
        lag(laps.pit_in_time_seconds) over (
            partition by laps.year, laps.event_id_within_year, laps.driver_number
            order by laps.lap_number asc
        ) as previous_pit_in_time_seconds,
        right(
            lag(laps.track_status_concat) over (
                partition by laps.year, laps.event_id_within_year, laps.driver_number
                order by laps.lap_number asc
            ),
            1
        ) as previous_lap_final_track_status
    from laps
    where laps.pit_in_time_seconds is not null
       or laps.pit_out_time_seconds is not null
)

select
    pit_windows.lap_id,
    pit_windows.year,
    pit_windows.event_id_within_year,
    pit_windows.driver_number,
    pit_windows.season_driver_team_key,
    pit_windows.track_status_concat,
    pit_windows.lap_number,
    pit_windows.stint_nr,
    pit_windows.lap_time_seconds,
    pit_windows.pit_in_time_seconds,
    pit_windows.previous_pit_in_time_seconds,
    pit_windows.previous_lap_final_track_status,
    pit_windows.pit_out_time_seconds - pit_windows.previous_pit_in_time_seconds as pit_length_seconds,
    case
        when pit_windows.previous_lap_final_track_status != '5'
            and pit_windows.pit_out_time_seconds - pit_windows.previous_pit_in_time_seconds is not null
            and pit_windows.pit_out_time_seconds - pit_windows.previous_pit_in_time_seconds < 60
        then true
        else false
    end as is_valid_pit_efficiency_event,
    pit_windows.pit_out_time_seconds,
    pit_windows.compound,
    pit_windows.tyre_life_in_laps
from pit_windows
