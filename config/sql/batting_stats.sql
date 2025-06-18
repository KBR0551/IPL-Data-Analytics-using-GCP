------------------------------------------ ORANGE_CAP ------------------------------------------
CREATE TABLE `{{ params.project_id }}.{{ params.tgt_dataset_name }}.ORANGE_CAP` AS
select 
a.season_year,
a.player_id as PLAYER, 
h.player_name,
a.player_team,
coalesce(a.total_matches_played,0) as MAT ,
coalesce(a.innings_played,0) as INNS,
coalesce(a.not_out,0) as `NO`,
coalesce(b.scored_match_runs,0) as RUNS,
--row_number() over(partition by(a.season_year) order by coalesce(b.scored_match_runs,0) desc ) as POS,
coalesce(c.runs,0) as HS,
coalesce(ROUND(cast(NULLIF(b.scored_match_runs,0) as NUMERIC)/cast(NULLIF((NULLIF(a.innings_played,0) -NULLIF(a.not_out,0)) ,0) as NUMERIC),2),0) as `AVG`,
coalesce(b.balls_faced,0) as BF,
coalesce(ROUND((cast(NULLIF(b.scored_match_runs,0) as NUMERIC)/cast(NULLIF(b.balls_faced,0) as NUMERIC)*100),2),0) as SR,
coalesce(d.hundreds,0) as `100`,
coalesce(d.fifties,0) as `50`,
coalesce(f.fours,0) as `4S`,
coalesce(f.sixes,0) as `6S`
from `{{ params.project_id }}.{{ params.src_dataset_name }}.batter_inns_played_temp` a 
left join 
`{{ params.project_id }}.{{ params.src_dataset_name }}.total_runs_and_balls_faced_temp` b on a.season_year=b.season and a.player_id=b.striker
left join
`{{ params.project_id }}.{{ params.src_dataset_name }}.batter_season_highg_scores_temp` c on a.season_year=c.season and a.player_id=c.striker
left join 
`{{ params.project_id }}.{{ params.src_dataset_name }}.centuries_temp` d on a.season_year=d.season and a.player_id=d.striker
left join
`{{ params.project_id }}.{{ params.src_dataset_name }}.boundry_count_temp` f on a.season_year=f.season and a.player_id=f.striker
left join 
`{{ params.project_id }}.{{ params.src_dataset_name }}.player_info` h on a.player_id=h.player_id
order by a.season_year asc,RUNS desc;


------------------------------------------ RuPay_On_The_Go_4s_of_the_Season ------------------------------------------
CREATE TABLE `{{ params.project_id }}.{{ params.tgt_dataset_name }}.RuPay_On_The_Go_4s_of_the_Season` AS 
select 
season_year,
Player,
player_name,
player_team,
`4S`,
mat,
inns,
`no`,
runs,
hs,
`AVG`,
bf,
sr,
`100`,
`50`,
`6S`
from `{{ params.project_id }}.{{ params.tgt_dataset_name }}.ORANGE_CAP` order by season_year asc, `4S` desc;

------------------------------------------ Angel_One_Super_Sixes_of_the_Season ------------------------------------------

CREATE TABLE `{{ params.project_id }}.{{ params.tgt_dataset_name }}.Angel_One_Super_Sixes_of_the_Season` AS 
select 
season_year,
Player,
player_name,
player_team,
`6S`,
mat,
inns,
`no`,
runs,
hs,
`AVG`,
bf,
sr,
`100`,
`50`,
`4S`
from `{{ params.project_id }}.{{ params.tgt_dataset_name }}.ORANGE_CAP` order by season_year asc, `6S` desc;

------------------------------------------ Most_Fifties ------------------------------------------

CREATE TABLE `{{ params.project_id }}.{{ params.tgt_dataset_name }}.Most_Fifties` AS 
select 
season_year,
Player,
player_name,
player_team,
`50`,
mat,
inns,
`no`,
runs,
hs,
`AVG`,
bf,
sr,
`100`,
`6S`,
`4S`
from `{{ params.project_id }}.{{ params.tgt_dataset_name }}.ORANGE_CAP` order by season_year asc, `50` desc;

------------------------------------------ Most_Centuries ------------------------------------------

CREATE TABLE `{{ params.project_id }}.{{ params.tgt_dataset_name }}.Most_Centuries` AS 
select 
season_year,
Player,
player_name,
player_team,
`100`,
mat,
inns,
`no`,
runs,
hs,
`AVG`,
bf,
sr,
`50`,
`6S`,
`4S`
from `{{ params.project_id }}.{{ params.tgt_dataset_name }}.ORANGE_CAP` order by season_year asc, `100` desc;


------------------------------------------ Highest_Scores ------------------------------------------

CREATE TABLE `{{ params.project_id }}.{{ params.tgt_dataset_name }}.Highest_Scores` AS 
select 
season_year,
Player,
player_name,
player_team,
hs,
mat,
inns,
`no`,
runs,
`AVG`,
bf,
sr,
`100`,
`50`,
`6S`,
`4S`
from `{{ params.project_id }}.{{ params.tgt_dataset_name }}.ORANGE_CAP` order by season_year asc, hs desc;

------------------------------------------ Punch_ev_Electric_Striker_of_the_Season ------------------------------------------

CREATE TABLE `{{ params.project_id }}.{{ params.tgt_dataset_name }}.Punch_ev_Electric_Striker_of_the_Season` AS 
select 
season_year,
Player,
player_name,
player_team,
sr,
mat,
inns,
`no`,
runs,
hs,
`AVG`,
bf,
`100`,
`50`,
`6S`,
`4S`
from `{{ params.project_id }}.{{ params.tgt_dataset_name }}.ORANGE_CAP` order by season_year asc, sr desc;

------------------------------------------ Best_Batting_Averages ------------------------------------------


CREATE TABLE `{{ params.project_id }}.{{ params.tgt_dataset_name }}.Best_Batting_Averages` AS 
select 
season_year,
Player,
player_name,
player_team,
`AVG`,
mat,
inns,
`no`,
runs,
hs,
bf,
sr,
`100`,
`50`,
`6S`,
`4S`
from `{{ params.project_id }}.{{ params.tgt_dataset_name }}.ORANGE_CAP` order by season_year asc, `AVG` desc;
	

------------------------------------------ ------------------------------------------
--- Innings level stats 
---- Most fours 
---  Most sixes
---- Fastest Fifty
---- Fastest Centuries
---- Best Batting Strike Rate
------------------------------------------------------------------------------------

CREATE  TABLE `{{ params.project_id }}.{{ params.tgt_dataset_name }}.most_fours_sixes` AS
select 
a.season,
p.player_name as PLAYER,
a.runs_scored as RUNS,
a.BF as BF,
coalesce(ROUND((cast(NULLIF(a.runs_scored,0) as NUMERIC)/cast(NULLIF(a.BF,0) as NUMERIC)*100),2),0) as SR,
a.fours as `4S`,
a.sixes as `6S`,
a.AGAINST,
m.venue_name as VENU,
m.match_date as `MATCH DATE`
from
(select 
match_id,
season,
striker,
b.team_name as player_team,
c.team_name as AGAINST ,
sum(runs_scored) as runs_scored,
sum(case when extra_type in ('No Extras','Byes','Legbyes','legbyes','byes') then 1 else 0 end ) as BF,
sum (case when runs_scored=4 then 1 else 0 end ) as fours,
sum (case when runs_scored=6 then 1 else 0 end ) as sixes
from `{{ params.project_id }}.{{ params.src_dataset_name }}.ball_by_ball_info` a
left join 
 `{{ params.project_id }}.{{ params.src_dataset_name }}.teams_info` b on a.battingteam_sk=b.team_sk
 left join 
 `{{ params.project_id }}.{{ params.src_dataset_name }}.teams_info` c on a.bowlingteam_sk=c.team_sk
group by 1,2,3,4,5
order by season desc) a
left join 
`{{ params.project_id }}.{{ params.src_dataset_name }}.matches_info` m on a.match_id=m.match_id
left join 
`{{ params.project_id }}.{{ params.src_dataset_name }}.player_info` p on p.player_id=a.striker;

------------------------------------------ Most fours in inns ------------------------------------------
CREATE  TABLE `{{ params.project_id }}.{{ params.tgt_dataset_name }}.most_fours` AS
select 
season,
player,
runs,
bf,
sr,
`4S`,
`6S`,
against,
venu,
`MATCH DATE`
 from `{{ params.project_id }}.{{ params.tgt_dataset_name }}.most_fours_sixes` order by season asc, `4S` desc;

------------------------------------------ Most sixes in inns ------------------------------------------
CREATE  TABLE `{{ params.project_id }}.{{ params.tgt_dataset_name }}.most_sixes` AS
select 
season,
player,
runs,
bf,
sr,
`4S`,
`6S`,
against,
venu,
`MATCH DATE`
from `{{ params.project_id }}.{{ params.tgt_dataset_name }}.most_fours_sixes` order by season asc, `6S` desc;

------------------------------------------ Fastest Fifties ------------------------------------------

Create Table `{{ params.project_id }}.{{ params.tgt_dataset_name }}.Fastest_Fifties` as
select a.season,
           a.player_name as PLAYER,
	   b.player_team,
	   b.runs_scored as RUNS, 
	   a.balls_faced_to_get_to_fifty_runs as BF,
	   Round((cast(b.runs_scored as NUMERIC)/cast(a.balls_faced_to_get_to_fifty_runs as NUMERIC)*100),2) as SR,
           b.AGAINST,
	   b.venue,
	   b.`MATCH DATE`	   
from
(select 
match_id,
season,
striker,
p.player_name,
min(balls_faced_to_get_runs) as balls_faced_to_get_to_fifty_runs
from 
`{{ params.project_id }}.{{ params.src_dataset_name }}.fastest_fifty_hundred_temp` a 
left join
`{{ params.project_id }}.{{ params.src_dataset_name }}.player_info` p on a.striker=p.player_id
 where a.run_mark >=50 and a.run_mark <60
group by 1,2,3,4 ) a
left join 
(select 
a.match_id,
a.season,
a.striker,
b.team_name as player_team,
c.team_name as AGAINST ,
m.venue_name as VENUE,
m.match_date as `MATCH DATE`,
a.runs_scored,
from `{{ params.project_id }}.{{ params.src_dataset_name }}.fours_sixes_temp` a
 left join
`{{ params.project_id }}.{{ params.src_dataset_name }}.teams_info`  b on a.battingteam_sk=b.team_sk
 left join 
`{{ params.project_id }}.{{ params.src_dataset_name }}.teams_info`  c on a.bowlingteam_sk=c.team_sk
 left join 
`{{ params.project_id }}.{{ params.src_dataset_name }}.matches_info` m on a.match_id=m.match_id) b on a.match_id=b.match_id and a.season=b.season and a.striker=b.striker
order by a.season asc, bf asc;

------------------------------------------ Fastest Centuries ------------------------------------------
Create Table `{{ params.project_id }}.{{ params.tgt_dataset_name }}.Fastest_Centuries` as
select     a.season,
           a.player_name as PLAYER,
	   b.player_team,
	   b.runs_scored as RUNS, 
	   a.balls_faced_to_get_to_hundred_runs as BF,
	   Round((cast(b.runs_scored as NUMERIC)/cast(a.balls_faced_to_get_to_hundred_runs as NUMERIC)*100),2) as SR,
           b.AGAINST,
	   b.venue,
	   b.`MATCH DATE`
	   
from
(select 
match_id,
season,
striker,
p.player_name,
min(balls_faced_to_get_runs) as balls_faced_to_get_to_hundred_runs
from 
`{{ params.project_id }}.{{ params.src_dataset_name }}.fastest_fifty_hundred_temp` a 
left join
`{{ params.project_id }}.{{ params.src_dataset_name }}.player_info` p on a.striker=p.player_id
 where a.run_mark >=100
group by 1,2,3,4 ) a
left join 
	(select 
	a.match_id,
	a.season,
	a.striker,
	b.team_name as player_team,
	c.team_name as AGAINST ,
	m.venue_name as VENUE,
	m.match_date as `MATCH DATE`,
	a.runs_scored,
	from `{{ params.project_id }}.{{ params.src_dataset_name }}.fours_sixes_temp` a
left join
	`{{ params.project_id }}.{{ params.src_dataset_name }}.teams_info` b on a.battingteam_sk=b.team_sk
left join 
	`{{ params.project_id }}.{{ params.src_dataset_name }}.teams_info` c on a.bowlingteam_sk=c.team_sk
left join 
	`{{ params.project_id }}.{{ params.src_dataset_name }}.matches_info` m on a.match_id=m.match_id) b on a.match_id=b.match_id and a.season=b.season and a.striker=b.striker
order by a.season asc, bf asc;

