select 
    game_id,
    team_id,
    trim(upper(hoa)) as hoa,
    won,
    settled_in,
    trim(upper(head_coach)) as head_coach,
    goals,
    shots,
    hits,
    pim,
    powerplayopportunities as power_play_opportunities,
    powerplaygoals as power_play_goals,
    faceoffwinpercentage as faceoff_win_percentage,
    giveaways,
    takeaways,
    blocked as blocked_shots,
    trim(upper(startrinkside)) as start_rink_side
from
        {{ source('nhl_data','game_teams_stats')}}