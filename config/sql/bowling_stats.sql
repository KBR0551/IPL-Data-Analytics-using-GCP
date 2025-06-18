------------------------------------------------- PURPLE_CAP -------------------------------------------------
CREATE TABLE `{{ params.project_id }}.{{ params.tgt_dataset_name }}.Purple_Cap` as 
select  
a.SEASON,
a.PLAYER_ID,
a.PLAYER_NAME,
a.PLAYER_TEAM,
a.MAT,
a.INNS,
a.OV,
a.RUNS,
a.WKTS,
concat(b.wickets,'/',b.runs) as BBI,
a.AVG,
a.ECON,
a.SR,
a.`4W`,
a.`5W`
from `{{ params.project_id }}.{{ params.src_dataset_name }}.bowler_inns_stats_temp` a
join 
`{{ params.project_id }}.{{ params.src_dataset_name }}.bbi_wickets_temp` b on a.PLAYER_ID=b.bowler
Order by season asc, wkts desc;



-------------------------------------------------MOST_MADINS-------------------------------------------------
CREATE TABLE `{{ params.project_id }}.{{ params.tgt_dataset_name }}.Most_Madins` as 
select  
a.SEASON,
a.PLAYER_ID,
a.PLAYER_NAME,
a.PLAYER_TEAM,
a.MAT,
a.INNS,
a.OV,
a.RUNS,
a.WKTS,
b.MAID,
a.AVG,
a.ECON,
a.SR,
a.`4W`,
a.`5W`
from `{{ params.project_id }}.{{ params.src_dataset_name }}.bowler_inns_stats_temp` a
join 
(select season,bowler,b.player_name,count(*) as MAID
from (
select season,match_id,bowler,over_id,
sum(runs_scored+bowler_extras) as runs,
count(ball_id) as ball_count
from `{{ params.project_id }}.{{ params.src_dataset_name }}.ball_by_ball_info`
group by 1,2,3,4 
having sum(runs_scored+bowler_extras)=0 and count(ball_id) >=6 ) a
join 
`{{ params.project_id }}.{{ params.src_dataset_name }}.player_info` b on a.bowler=b.player_id
group by 1,2,3)  b  on a.season=b.season and a.player_id=b.bowler 
order by season asc, MAID desc;


------------------------------------------------- MOST DOT BALLS -------------------------------------------------


CREATE TABLE `{{ params.project_id }}.{{ params.tgt_dataset_name }}.Most_dots_balls` as 
select  
a.SEASON,
a.PLAYER_ID,
a.PLAYER_NAME,
a.PLAYER_TEAM,
a.MAT,
a.INNS,
a.OV,
a.RUNS,
a.WKTS,
b.DOTS,
a.AVG,
a.ECON,
a.SR,
a.`4W`,
a.`5W`
from `{{ params.project_id }}.{{ params.src_dataset_name }}.bowler_inns_stats_temp` a
join
(select season,bowler,sum(DOTS) as DOTS
from `{{ params.project_id }}.{{ params.src_dataset_name }}.dot_balls_temp` a
group by 1,2) b on a.season=b.season and a.player_id=b.bowler
order by b.DOTS desc;


------------------------------------------------- Best Bowling Average -------------------------------------------------

CREATE TABLE `{{ params.project_id }}.{{ params.tgt_dataset_name }}.Best_Bowling_Average` as 
select  
SEASON,
PLAYER_ID,
PLAYER_NAME,
PLAYER_TEAM,
MAT,
INNS,
OV,
RUNS,
WKTS,
BBI,
AVG,
ECON,
SR,
`4W`,
`5W`
from `{{ params.project_id }}.{{ params.tgt_dataset_name }}.Purple_Cap` 
Order by season asc, AVG  asc;


------------------------------------------------- Best Bowling Economy -------------------------------------------------

CREATE TABLE `{{ params.project_id }}.{{ params.tgt_dataset_name }}.Best_Bowling_Economy` as 
select  
SEASON,
PLAYER_ID,
PLAYER_NAME,
PLAYER_TEAM,
MAT,
INNS,
OV,
RUNS,
WKTS,
BBI,
AVG,
ECON,
SR,
`4W`,
`5W`
from `{{ params.project_id }}.{{ params.tgt_dataset_name }}.Purple_Cap`
Order by season asc, ECON  asc;

------------------------------------------------- Best Bowling Strike Rate -------------------------------------------------

CREATE TABLE `{{ params.project_id }}.{{ params.tgt_dataset_name }}.Best_Bowling_Strike_Rate` as 
select  
SEASON,
PLAYER_ID,
PLAYER_NAME,
PLAYER_TEAM,
MAT,
INNS,
OV,
RUNS,
WKTS,
BBI,
AVG,
ECON,
SR,
`4W`,
`5W`
from `{{ params.project_id }}.{{ params.tgt_dataset_name }}.Purple_Cap` 
Order by season asc, SR  asc;


----- Stats At innings/match level -----
-- Most Dot Balls (innings)
-- Best_Bowling_Strike_Rate (innings)
-- Best_Bowling_Strike_Rate  (innings)
-- Most_Runs_Conceded (innings)
-- Best_Bowling_Figures_innings

-------------------------------------------------Best_Bowling_Figures_innings -------------------------------------------------




CREATE TABLE `{{ params.project_id }}.{{ params.tgt_dataset_name }}.Best_Bowling_Figures_innings` as 	
select  season,
	bowler as player_id,
	player_name,
	player_team,
	BBI,
	OV,
	RUNS,
	WKTS,
	SR,
	AGNAIST,
	venue,
	match_date
from `{{ params.project_id }}.{{ params.src_dataset_name }}.Best_Bowling_Figures_innings_temp` order by season asc, wkts desc;
	
	
-------------------------------------------------Most_Dot_Balls_Innings-------------------------------------------------

CREATE TABLE `{{ params.project_id }}.{{ params.tgt_dataset_name }}.Most_Dot_Balls_Innings` as 	
select 
a.season,
a.bowler as player_id,
b.player_name,
b.player_team,
b.ov,
b.runs,
b.wkts,
a.dots,
b.sr,
b.AGNAIST,
b.venue,
b.match_date
from `{{ params.project_id }}.{{ params.src_dataset_name }}.dot_balls_temp` a
left join 
`{{ params.project_id }}.{{ params.src_dataset_name }}.Best_Bowling_Figures_innings_temp` b on a.match_id=b.match_id and a.season=b.season and a.bowler=b.bowler
order by a.season asc , dots desc;


------------------------------------------------- Best_Bowling_Economy_Innings -------------------------------------------------

CREATE TABLE `{{ params.project_id }}.{{ params.tgt_dataset_name }}.Best_Bowling_Economy_Innings` as 		
select 
a.season,
a.bowler as player_id,
b.player_name,
b.player_team,
b.ov,
b.runs,
b.wkts,
a.dots,
coalesce(round(NULLIF(b.runs,0)/NULLIF(cast(b.ov as decimal),0),2),0) as ECON,
b.sr,
b.AGNAIST,
b.venue,
b.match_date
from `{{ params.project_id }}.{{ params.src_dataset_name }}.dot_balls_temp` a
left join 
`{{ params.project_id }}.{{ params.src_dataset_name }}.Best_Bowling_Figures_innings_temp` b on a.match_id=b.match_id and a.season=b.season and a.bowler=b.bowler
where cast(b.ov as decimal)>1
order by a.season asc , ECON asc;

-------------------------------------------------Best_Bowling_Strike_Rate_Innings -------------------------------------------------


	
CREATE TABLE `{{ params.project_id }}.{{ params.tgt_dataset_name }}.Best_Bowling_Strike_Rate_Innings` as 	
select 
a.season,
a.bowler as player_id,
b.player_name,
b.player_team,
b.ov,
b.runs,
b.wkts,
b.sr,
b.AGNAIST,
b.venue,
b.match_date
from `{{ params.project_id }}.{{ params.src_dataset_name }}.bowler_played_matches_temp` a
left join 
`{{ params.project_id }}.{{ params.src_dataset_name }}.Best_Bowling_Figures_innings_temp` b on a.match_id=b.match_id and a.season=b.season and a.bowler=b.bowler
where b.wkts>1 
order by a.season asc , sr asc;


------------------------------------------------- Most_Runs_Conceded_Innings -------------------------------------------------

CREATE TABLE `{{ params.project_id }}.{{ params.tgt_dataset_name }}.Most_Runs_Conceded_Innings` as 
select 
a.season,
a.bowler as player_id,
b.player_name,
b.player_team,
b.ov,
b.runs,
b.wkts,
b.sr,
b.AGNAIST,
b.venue,
b.match_date
from `{{ params.project_id }}.{{ params.src_dataset_name }}.bowler_played_matches_temp` a
left join 
`{{ params.project_id }}.{{ params.src_dataset_name }}.Best_Bowling_Figures_innings_temp` b on a.match_id=b.match_id and a.season=b.season and a.bowler=b.bowler
order by a.season asc , runs desc;




------------------------------------------------- HATRIC -------------------------------------------------------------------------




CREATE  TABLE `{{ params.project_id }}.{{ params.tgt_dataset_name }}.hatricks` AS
select 
hatrick_count.season,
hatrick_count.bowler as player_id,
pc.player_name,
pc.player_team,
hatrick_count.HAT_TRICKS,
pc.mat,
pc.inns,
pc.ov,
pc.runs,
pc.wkts,
pc.avg,
pc.sr,
pc.`4W`,
pc.`5W`
from
 (select a.season,a.bowler,count(*) as HAT_TRICKS from 
 (select season,bowler,b.player_name,
 case when over_id_1=over_id_2 and over_id_2=over_id_3
           and (ball_id_1+1=ball_id_2 and ball_id_2+1=ball_id_3) then 'YES' --Hartick in same Over
	  when (ball_id_1=5 and ball_id_2=6 and over_id_1=over_id_2  and ball_id_3 =1) then 'YES'
	  when (ball_id_1=6 and ball_id_2=1 and ball_id_3 =2 and over_id_2=over_id_3) then 'YES'
	  when (ball_id_1=7 and ball_id_2=1 and ball_id_3 =2  and over_id_2=over_id_3) then 'YES'
      when (ball_id_1=6 and ball_id_2=7  and over_id_1=over_id_2 and ball_id_3 =1) then 'YES'
   else 'NO' end as HATRICK
 from `{{ params.project_id }}.{{ params.src_dataset_name }}.hatrick_temp` a
 join 
  `{{ params.project_id }}.{{ params.src_dataset_name }}.player_info` b on a.bowler=b.player_id
 ) a where HATRICK='YES'
 group by 1,2
) hatrick_count
join `{{ params.project_id }}.{{ params.tgt_dataset_name }}.Purple_Cap` PC on hatrick_count.season=pc.season and hatrick_count.bowler=pc.player_id
 order by season asc;
 