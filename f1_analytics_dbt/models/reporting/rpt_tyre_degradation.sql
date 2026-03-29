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
    cluster_by=["event_id_within_year", "driver_number", "compound"]
) }}

with laps as (
    select *
    from {{ ref('fct_laps') }}
),

results as (
    select distinct
        year,
        event_id_within_year,
        event_name
    from {{ ref('fct_results') }}
),

lagged_data as (
select
    laps.lap_id,
    laps.year,
    laps.event_id_within_year,
    laps.driver_number,
    laps.season_driver_team_key,
    laps.lap_number,
    laps.stint_nr,
    laps.lap_time_seconds,
    lag(laps.lap_time_seconds) over(partition by laps.year, laps.event_id_within_year, laps.driver_number, laps.stint_nr order by laps.lap_number) lagged_lap_time,
    laps.compound,
    laps.tyre_life_in_laps,
    lag(laps.tyre_life_in_laps) over(partition by laps.year, laps.event_id_within_year, laps.driver_number, laps.stint_nr order by laps.lap_number) lagged_tyre_age
from laps
where 
  laps.is_only_clear_track is true
  and laps.pit_in_time_seconds is null
  and laps.pit_out_time_seconds is null
  and laps.lap_time_seconds is not null
  and laps.tyre_life_in_laps is not null
)

select
    lagged_data.lap_id,
    lagged_data.year,
    lagged_data.event_id_within_year,
    lagged_data.driver_number,
    lagged_data.season_driver_team_key,
    lagged_data.lap_number,
    lagged_data.stint_nr,
    lagged_data.lap_time_seconds,
    lagged_data.lagged_lap_time,
    lagged_data.compound,
    lagged_data.tyre_life_in_laps,
    lagged_data.lagged_tyre_age,
    lagged_data.lap_time_seconds - lagged_data.lagged_lap_time slowed_down_by_seconds,
    results.event_name
from lagged_data
left join results
    on lagged_data.year = results.year
    and lagged_data.event_id_within_year = results.event_id_within_year
where 
    lagged_tyre_age is not null
    and (lagged_data.tyre_life_in_laps - lagged_data.lagged_tyre_age) = 1 -- excluding changes to used sets