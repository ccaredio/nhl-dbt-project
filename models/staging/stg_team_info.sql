select 
    team_id,
    franchiseid as franchise_id,
    trim(upper(shortname)) as city_name,
    trim(upper(teamname)) as team_name,
    abbreviation
from
    {{ source('nhl_data','team_info')}}