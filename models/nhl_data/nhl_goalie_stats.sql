with clean_game as (
select 
    game_id, season, type, game_type, away_team_id, home_team_id 
from(
    select game_id, season, type, game_type, away_team_id, home_team_id, row_number() over (partition by game_id order by season)              as rn 
        from {{ref('stg_game')}}) 
    where rn = 1
),

first as (
select
    gs.player_id as player_id,
    g.season as season,
    p.first_name as first_name,
    p.last_name as last_name,
    p.primary_position as primary_position,
    t.city_name as city_name,
    t.team_name as team_name,
    t.abbreviation as abbreviation,
    sum(gs.shots) as total_shots_faced,
    sum(gs.saves) as total_saves,
    avg(gs.save_percentage) as season_avg_save_pct
from {{ref('stg_game_goalie_stats')}} gs
left join clean_game g on gs.game_id=g.game_id
inner join {{ref('stg_team_info')}} t on gs.team_id=t.team_id
inner join {{ref('stg_player_info')}} p on gs.player_id=p.player_id
where
    p.primary_position = 'G'
group by 
    1,2,3,4,5,6,7,8
order by 
    g.season desc
),

wins as (
select
    gs.player_id as player_id,
    g.season as season,
    count(gs.decision) as total_wins 
from 
    {{ref('stg_game_goalie_stats')}} gs
left join clean_game g on gs.game_id=g.game_id
where 
    decision = 'W' and g.type = 'R' 
group by
    1,2
),

reg_losses as (
select
    gs.player_id as player_id,
    g.season as season,
    count(gs.decision) as total_reg_losses 
from 
    {{ref('stg_game_goalie_stats')}} gs
left join clean_game g on gs.game_id=g.game_id
where 
    gs.decision = 'L' and g.game_type = 'REG' and g.type = 'R' 
group by
    1,2
),

ot_losses as (
select
    gs.player_id as player_id,
    g.season as season,
    count(gs.decision) as total_ot_losses 
from 
    {{ref('stg_game_goalie_stats')}} gs
left join clean_game g on gs.game_id=g.game_id
where 
    gs.decision = 'L' and g.game_type = 'OT' and g.type = 'R' 
group by
    1,2
),

final as (
select 
    f.*,
    round(w.total_wins/2) as wins,
    round(r.total_reg_losses/2) as regulation_losses,
    round(o.total_ot_losses/2) as ot_losses
from
    first f
left join wins w on f.player_id=w.player_id and f.season=w.season
left join reg_losses r on f.player_id=r.player_id and f.season=r.season
left join ot_losses o on f.player_id=o.player_id and f.season=o.season
)

select 
    * 
from 
    final 
order by 
    season desc