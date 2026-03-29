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

with results as (
    select *
    from {{ ref('stg_results') }}
),

season_driver_teams as (
    select *
    from {{ ref('dim_season_drivers_teams') }}
)

select

    -- identificators
    results.result_id,
    results.year,
    results.event_id_within_year,
    results.driver_number,
    results.event_name,
    season_driver_teams.season_driver_team_key,

    -- result info
    results.finishing_position,
    results.classified_position,
    results.grid_position,
    results.gap_or_total_driver_race_time_seconds,
    results.race_status,
    results.points,
    results.completed_laps,

    -- inferrer info about finishes etc
    case when results.finishing_position is not null then True else False end as has_finishing_position,
    safe_cast(results.classified_position as int64) is not null as has_classified_position_numeric,
    safe_cast(results.classified_position as int64) as classified_position_numeric,
    case when safe_cast(results.classified_position as int64) is null then results.classified_position end as not_classified_reason,
    case when results.race_status = 'Finished' then True else False end as has_finished_status,
    -- --> if has_classified_position_numeric = True and has_finished_status = True then can be used as good calculation
    -- bcs has_classified_position_numeric is light check = if they got some evaluated position but didnt have to finish to the end/could be lapped/crashed...
    -- has_finished_status is stronger check = if they actually finished w/o being lapped, DNF, ... etc
    -- plus no record with False strong check but True light check --> has_finished_status seems to be good

    -- metadata
    results.loaded_at

from results
-- team info to be joined if needed when analyzing
left join season_driver_teams
    on results.year = season_driver_teams.year
    and results.driver_number = season_driver_teams.driver_number
    and results.team_name = season_driver_teams.team_name