select 
    game_id,
    trim(upper(official_name)) as official_name,
    trim(upper(official_type)) as official_type
from
    {{ source('nhl_data','game_officials')}}