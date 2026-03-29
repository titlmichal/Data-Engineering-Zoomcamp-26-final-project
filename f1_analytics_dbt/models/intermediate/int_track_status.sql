with 
source as (
select 
    *
from {{ ref('stg_laps') }}
)

-- breaking down track_status_concat into separate columns
-->creating decoder table for join to fct_laps via track_status_concat

select distinct

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

    track_status_concat,
    regexp_contains(track_status_concat, r'1') as is_track_clear,
    regexp_contains(track_status_concat, r'2') as is_yellow_flag,
    regexp_contains(track_status_concat, r'3') as is_unknown_status_3,
    regexp_contains(track_status_concat, r'4') as is_safety_car,
    regexp_contains(track_status_concat, r'5') as is_red_flag,
    regexp_contains(track_status_concat, r'6') as is_virtual_safety_car,
    regexp_contains(track_status_concat, r'7') as is_virtual_safety_car_ending,
    track_status_concat = '1' as is_only_clear_track

from source
where track_status_concat is not null
