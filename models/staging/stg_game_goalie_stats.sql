select 
    game_id,
    player_id,
    team_id,
    timeonice/60 as time_on_ice_min,
    timeonice as time_on_ice_sec,
    assists,
    goals,
    pim,
    shots,
    saves,
    powerplaysaves as power_play_saves,
    shorthandedsaves as short_handed_saves,
    evensaves as even_saves,
    shorthandedshotsagainst as short_handed_shots_against,
    evenshotsagainst as even_shots_against,
    powerplayshotsagainst as power_play_shots_against,
    decision,
    case
        when savepercentage = 'NA' then null
        else round(savepercentage,2)
    end as save_percentage,
    case
        when powerplaysavepercentage = 'NA' then null
        else round(powerplaysavepercentage,2) 
    end as power_play_save_percentage,
    case
        when evenstrengthsavepercentage = 'NA' then null
        else round(evenstrengthsavepercentage,2)
    end as even_strength_save_percentage
from
        {{ source('nhl_data','game_goalie_stats')}}