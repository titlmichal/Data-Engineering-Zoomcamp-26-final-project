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
    from {{ ref('stg_laps') }}
),

season_driver_teams as (
    select *
    from {{ ref('dim_season_drivers_teams') }}
),

track_status as (
    select *
    from {{ ref('int_track_status') }}
)

select
    -- fact grain key
    laps.lap_id,

    -- foreign keys / business keys
    laps.year,
    laps.event_id_within_year,
    laps.driver_number,
    season_driver_teams.season_driver_team_key,
    laps.track_status_concat,

    -- lap identifiers
    laps.lap_number,
    laps.stint_nr,

    -- lap metrics
    laps.total_session_time_seconds,
    laps.lap_time_seconds,
    laps.lap_start_time_seconds,
    laps.pit_out_time_seconds,
    laps.pit_in_time_seconds,
    laps.sector_1_time_seconds,
    laps.sector_2_time_seconds,
    laps.sector_3_time_seconds,
    laps.time_of_setting_sector_1_time_seconds,
    laps.time_of_setting_sector_2_time_seconds,
    laps.time_of_setting_sector_3_time_seconds,

    -- speed metrics
    laps.trap_sector_1_speed,
    laps.trap_sector_2_speed,
    laps.trap_finish_line_speed,
    laps.trap_high_speed_zone_speed,

    -- racing context
    laps.lap_end_position,
    laps.compound,
    laps.tyre_life_in_laps,
    laps.is_fresh_tyre,
    laps.is_personal_best,
    laps.is_fastf1_generated,
    laps.is_accurate,

    -- decoded track status flags
    -- track_status.is_track_clear,
    track_status.is_only_clear_track,
    -- track_status.is_yellow_flag,
    -- track_status.is_unknown_status_3,
    -- track_status.is_safety_car,
    -- track_status.is_red_flag,
    -- track_status.is_virtual_safety_car,
    -- track_status.is_virtual_safety_car_ending,

    -- metadata
    laps.loaded_at

from laps
-- team info to be joined if needed when analyzing
left join season_driver_teams
    on laps.year = season_driver_teams.year
    and laps.driver_number = season_driver_teams.driver_number
    and laps.team_name = season_driver_teams.team_name

-- mainly for clear laps, possible to join the rest later
left join track_status
    on laps.track_status_concat = track_status.track_status_concat
