{{
    config(
        materialized='incremental',
        unique_key='PRI_ID'
    )
}}

with clean_game as (
select
    *
from (
    select 
        *, row_number() over (partition by game_id order by season) as rn
    from 
        {{ref('stg_game')}})
where
    rn = 1),

clean_game_stats as (
select
    *
from (
    select 
        *, row_number() over (partition by game_id order by team_id) as rn
    from 
        {{ref('stg_game_team_stats')}})
where
    rn = 1),

first as ( 
select 
    s.game_id as game_id,
    t.team_name as team_name,
    case 
        when s.won = 'FALSE' and s.settled_in = 'REG' then 'Reg Loss'
        when s.won = 'FALSE' and s.settled_in = 'OT' then 'OT Loss'
        when s.won = 'TRUE' then 'Win'
        else null
    end as game_decision,
    g.season as season,
    to_date(g.date_time) as game_date
from
    clean_game_stats s
left join 
    clean_game g on s.game_id=g.game_id
inner join 
    {{ref('stg_team_info')}} t on s.team_id=t.team_id

{% if is_incremental() %}

  where to_date(g.date_time) >= dateadd(day, -30, current_date)

{% endif %}

),

wins as (
select
    *
from
    first
where
    game_decision = 'Win'
),

reg_loss as (
select
    *
from
    first
where
    game_decision = 'Reg Loss'
),

ot_loss as (
select
    *
from
    first
where
    game_decision = 'OT Loss'
),

final as (
select
    f.team_name,
    f.season,
    count(w.*) as wins,
    count(r.*) as reg_loss,
    count(o.*) as ot_loss
from
    first f
left join
    wins w on f.game_id=w.game_id and f.team_name=w.team_name and f.season=w.season
left join
    reg_loss r on f.game_id=r.game_id and f.team_name=r.team_name and f.season=r.season
left join
    ot_loss o on f.game_id=o.game_id and f.team_name=o.team_name and f.season=o.season
group by
    1,2
)

select 
    {{ dbt_utils.generate_surrogate_key(['TEAM_NAME','SEASON'])}} as PRI_ID,
    * 
from 
    final 
order by 
    season desc, 
    team_name