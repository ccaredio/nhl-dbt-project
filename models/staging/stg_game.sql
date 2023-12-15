select 
    game_id,
    concat(left(season, 4),'-', right(season, 4)) as season,
    type,
    date_time_gmt as date_time,
    away_team_id,
    home_team_id,
    away_goals,
    home_goals,
    upper(left(outcome,8)) as outcome,
    trim(upper(right(outcome, 3))) as game_type,
    upper(home_rink_side_start) as home_rink_side_start,
    upper(venue) as venue,
    upper(venue_time_zone_id) as venue_time_zone_id,
    venue_time_zone_offset,
    venue_time_zone_tz
from 
    {{ source('nhl_data','game')}}