CREATE TABLE `{{ params.project_id }}.{{ params.src_dataset_name }}.batter_inns_played_temp` AS
select 
season_year,
player_id,
player_team,
count(distinct match_id) as total_matches_played,
sum(inn) as innings_played,
(sum(inn) -sum(out_ind) )as not_out
from 
(
select a.match_id,
	   a.player_id,
	   a.season_year,
	   a.player_team,
       case when (a.player_id=b.striker or a.player_id=b.non_striker) then 1 else 0 end as inn,
       case when a.player_id=c.player_out then 1 else 0 end as out_ind 
from `{{ params.project_id }}.{{ params.src_dataset_name }}.player_match_info` a
left join 
`{{ params.project_id }}.{{ params.src_dataset_name }}.ball_by_ball_info` b
on a.match_id=b.match_id and a.season_year=b.season and (a.player_id=b.striker or a.player_id=b.non_striker)
left join 
`{{ params.project_id }}.{{ params.src_dataset_name }}.ball_by_ball_info` c on c.match_id=a.match_id and a.player_id=c.player_out and c.player_out is not null
GROUP BY 1,2,3,4,5,6) a
group by 1,2,3
order by player_id;


CREATE TABLE `{{ params.project_id }}.{{ params.src_dataset_name }}.total_runs_and_balls_faced_temp` AS
select 
season,
striker,
sum(runs_scored) as scored_match_runs ,
sum(case when extra_type in ('No Extras','Byes','Legbyes','legbyes','byes','Noballs','noballs') then 1 else 0 end) as balls_faced
from  `{{ params.project_id }}.{{ params.src_dataset_name }}.ball_by_ball_info`
group by 1,2;


CREATE  TABLE `{{ params.project_id }}.{{ params.src_dataset_name }}.batter_season_highg_scores_temp` AS
select 
season,
striker,
runs
from
(
select 
season,
striker,
match_id,
sum(runs_scored) as runs,
row_number() over( partition by  season,striker order by season asc , sum(runs_scored) desc) as runs_rank
from `{{ params.project_id }}.{{ params.src_dataset_name }}.ball_by_ball_info`
group by 1,2,3
) a where  runs_rank=1;


CREATE  TABLE `{{ params.project_id }}.{{ params.src_dataset_name }}.centuries_temp` AS
select 
season,
striker, 
sum(case when runs>=50 and runs<100 then 1 else 0 end) as fifties ,
sum(case when runs>=100 then 1 else 0 end) as hundreds 
from (
select 
season,
striker,
match_id,
sum(runs_scored) as runs
from `{{ params.project_id }}.{{ params.src_dataset_name }}.ball_by_ball_info`
group by 1,2,3 ) a 
group by season,striker;


CREATE   TABLE `{{ params.project_id }}.{{ params.src_dataset_name }}.boundry_count_temp` AS
select 
season,
striker,
sum (case when runs_scored=4 then 1 else 0 end ) as fours,
sum (case when runs_scored=6 then 1 else 0 end ) as sixes
from `{{ params.project_id }}.{{ params.src_dataset_name }}.ball_by_ball_info`
where runs_scored in (4,6) 
group by 1,2  ; 


CREATE  TABLE `{{ params.project_id }}.{{ params.src_dataset_name }}.fastest_fifty_hundred_temp` AS
select 
match_id,
season,
striker,
sum(runs_scored) over(partition by match_id,season,striker order by over_id asc, ball_id asc) as run_mark,
row_number() over(partition by match_id,season,striker) as balls_faced_to_get_runs
from `{{ params.project_id }}.{{ params.src_dataset_name }}.ball_by_ball_info` 
where  extra_type in ('No Extras','Byes','Legbyes','legbyes','byes');


CREATE  TABLE `{{ params.project_id }}.{{ params.src_dataset_name }}.fours_sixes_temp` AS
select 
a.match_id,
a.season,
a.striker,
a.battingteam_sk,
a.bowlingteam_sk,
sum(a.runs_scored) as runs_scored,
sum (case when runs_scored=4 then 1 else 0 end ) as fours,
sum (case when runs_scored=6 then 1 else 0 end ) as sixes
from `{{ params.project_id }}.{{ params.src_dataset_name }}.ball_by_ball_info` a
group by 1,2,3,4,5;