select 
    player_id,
    trim(upper(firstname)) as first_name,
    trim(upper(lastname)) as last_name,
    nationality,
    trim(upper(birthcity)) as birth_city,
    primaryposition as primary_position,
    to_date(birthdate) as birth_date,
    birthstateprovince as birth_state_province,
    height,
    height_cm,
    weight,
    shootscatches as shoots_catches
from
    {{ source('nhl_data','player_info')}}