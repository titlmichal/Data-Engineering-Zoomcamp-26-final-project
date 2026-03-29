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

with results as (
    select
        result_id,
        year,
        event_id_within_year,
        event_name,
        season_driver_team_key,
        driver_number,
        finishing_position,
        classified_position,
        grid_position,
        points,
        completed_laps,
        gap_or_total_driver_race_time_seconds,
        race_status
    from {{ ref('fct_results') }}
),

degradation as (
    select
        year,
        event_id_within_year,
        driver_number,
        compound,       --> due to this, result_id is no more unique (!)
        count(*) as number_of_clear_laps_used,
        avg(lap_time_seconds) as avg_clear_lap_time_seconds,
        avg(slowed_down_by_seconds) as avg_lap_degradation_seconds,
        max(slowed_down_by_seconds) as max_lap_degradation_seconds
    from {{ ref('rpt_tyre_degradation') }}
    group by 1, 2, 3, 4
),

pit_efficiency as (
    select
        year,
        event_id_within_year,
        driver_number,
        countif(is_valid_pit_efficiency_event) as number_of_valid_pit_events,
        avg(case when is_valid_pit_efficiency_event then pit_length_seconds end) as avg_valid_pit_length_seconds,
        max(case when is_valid_pit_efficiency_event then pit_length_seconds end) as max_valid_pit_length_seconds,
        sum(case when is_valid_pit_efficiency_event then pit_length_seconds end) as total_valid_pit_length_seconds
    from {{ ref('rpt_pit_efficiency') }}
    group by 1, 2, 3
)


select
    results.result_id,
    results.year,
    results.event_id_within_year,
    results.event_name,
    results.season_driver_team_key,
    results.driver_number,

    results.finishing_position,
    results.classified_position,
    results.grid_position,
    results.points,
    results.completed_laps,
    results.gap_or_total_driver_race_time_seconds,
    results.race_status,

    degradation.compound,
    degradation.number_of_clear_laps_used,
    degradation.avg_clear_lap_time_seconds,
    degradation.avg_lap_degradation_seconds,
    degradation.max_lap_degradation_seconds,

    pit_efficiency.number_of_valid_pit_events,
    pit_efficiency.avg_valid_pit_length_seconds,
    pit_efficiency.max_valid_pit_length_seconds,
    pit_efficiency.total_valid_pit_length_seconds

from results
left join degradation
    on results.year = degradation.year
    and results.event_id_within_year = degradation.event_id_within_year
    and results.driver_number = degradation.driver_number
left join pit_efficiency
    on results.year = pit_efficiency.year
    and results.event_id_within_year = pit_efficiency.event_id_within_year
    and results.driver_number = pit_efficiency.driver_number