with 
source as (
select 
    *
from {{ ref('stg_results') }}
)

select
    farm_fingerprint(
        concat(
            coalesce(cast(year as string), ''),
            '|',
            coalesce(driver_number, ''),
            '|',
            coalesce(team_id, '')
        )
    ) as season_driver_team_key,
    year,
    driver_number,
    team_id,

    -- e.g. Stroll had 2 record for 2023, both for AM but one with missing values like broadcast name --> aggregating
    max(team_name) as team_name,
    max(team_color) as team_color,
    max(broadcast_name) as broadcast_name,
    max(driver_id) as driver_id,
    max(driver_code) as driver_code,
    max(first_name) as first_name,
    max(last_name) as last_name,
    max(full_name) as full_name,
    max(headshot_url) as headshot_url,
    max(country_code) as country_code
from source
group by 1, 2, 3, 4
