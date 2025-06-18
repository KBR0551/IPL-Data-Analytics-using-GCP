CREATE  or replace TABLE `{{ params.project_id }}.{{ params.src_dataset_name }}.bowler_inns_stats_temp` AS
select * from (
select 
season ,
player_id,
player_name,
player_team,
count( match_id) as MAT,
sum(inn) as INNS,
sum(overs) as OV,
coalesce(sum(runs),0) as RUNS,
sum(wickets) as WKTS,
coalesce(round(NULLIF(sum(runs),0)/NULLIF(sum(wickets),0),2),0) as AVG,
coalesce(round(NULLIF(sum(runs),0)/NULLIF(sum(overs),0),2),0) as ECON,
coalesce(round(NULLIF(sum(overs)*6,0)/NULLIF(sum(wickets),0),2),0) as SR,
sum(four_wick) as `4W`,
sum(five_wick) as `5W`
from
(
select a.season_year as season,
	   a.match_id,
	   a.player_id,
	   a.player_name,
	   a.player_team,
	   count(distinct b.bowler) as inn,
	   count(distinct b.over_id) as overs,
	   sum(b.runs_scored+b.bowler_extras)  as runs,
	   sum(case when b.bowler_wicket=1 then 1 else 0 end) as wickets,
	   case when sum(bowler_wicket)=4 then 1 else 0 end as four_wick,
	   case when sum(bowler_wicket)>=5 then 1 else 0 end as five_wick
from `{{ params.project_id }}.{{ params.src_dataset_name }}.player_match_info` a
left join 
`{{ params.project_id }}.{{ params.src_dataset_name }}.ball_by_ball_info` b
on a.match_id=b.match_id and a.season_year=b.season and a.player_id=b.bowler
GROUP BY 1,2,3,4,5
) a
group by 1,2,3,4) a 
where a.WKTS>0 ;


CREATE or replace TABLE `{{ params.project_id }}.{{ params.src_dataset_name }}.bbi_wickets_temp` as 
select bowler,wickets,runs from 
(
select 
        match_id,
        bowler, 
		season,
		sum(bowler_wicket) as wickets,
	    sum(runs_scored+bowler_extras)  as runs,
		dense_rank() over(partition by bowler order by sum(bowler_wicket) desc, sum(runs_scored+bowler_extras) asc )  as bbi_rnk,
		sum(runs_scored) as runs_concided 
		from `{{ params.project_id }}.{{ params.src_dataset_name }}.ball_by_ball_info`   
	    group by 1,2,3 ) a 
where bbi_rnk=1
group by 1,2,3 ;



CREATE  or replace TABLE `{{ params.project_id }}.{{ params.src_dataset_name }}.dot_balls_temp` as 
select 
	season,
	match_id,
	bowler,
	count(*) as DOTS
from 
	`{{ params.project_id }}.{{ params.src_dataset_name }}.ball_by_ball_info` a
join 
	`{{ params.project_id }}.{{ params.src_dataset_name }}.player_info` b on a.bowler=b.player_id 
where runs_scored+extra_runs+bowler_extras+extra_runs=0
group by 1,2,3 ;


CREATE  or replace TABLE `{{ params.project_id }}.{{ params.src_dataset_name }}.over_ball_count_temp` as    -- at over level 
select 
season,
match_id,
bowler,
over_id,
sum(case when bowler_extras=0 then 1 else 0 end) as ball_count,  --balls bowled ignoring extra balls bowled by bowler
sum(case when bowler_wicket=1 then 1 else 0 end) as over_wickets,  -- wickets taken in each over
sum(runs_scored+bowler_extras) as runs  --runs conceded in each over
from `{{ params.project_id }}.{{ params.src_dataset_name }}.ball_by_ball_info` 
group by 1,2,3,4;


CREATE  or replace TABLE `{{ params.project_id }}.{{ params.src_dataset_name }}.Best_Bowling_Figures_innings_temp` as 
select a.season,
a.match_id,
a.bowler,
d.player_name,
d.player_team,
case when ball_count IS NULL then cast(a.over_count as string) else concat(a.over_count,'.',b.ball_count) end as OV,
c.runs,
c.wkts,
concat(c.wkts,'/',c.runs) as BBI,
coalesce(round(NULLIF(total_match_balls,0)/NULLIF(wkts,0),2),0) as SR,
d.opposit_team as AGNAIST,
e.venue_name as venue,
e.match_date
from 
(select season,match_id,bowler,
 sum(ball_count) as total_match_balls,
sum(case when ball_count>=6 then 1 else 0 end) as  over_count
from `{{ params.project_id }}.{{ params.src_dataset_name }}.over_ball_count_temp`
group by 1,2,3 ) a
left join 
(select season,match_id,bowler,ball_count
from `{{ params.project_id }}.{{ params.src_dataset_name }}.over_ball_count_temp` where ball_count<6) b on a.season=b.season and a.match_id=b.match_id and a.bowler=b.bowler
left join 
(select season,match_id,bowler,sum(runs) as runs,sum(over_wickets) as wkts from `{{ params.project_id }}.{{ params.src_dataset_name }}.over_ball_count_temp` group by 1,2,3) c
on a.season=c.season and a.match_id=c.match_id and a.bowler=c.bowler
left join 
`{{ params.project_id }}.{{ params.src_dataset_name }}.player_match_info`  d on a.bowler=d.player_id and a.season=d.season_year and a.match_id=d.match_id 
left join 
`{{ params.project_id }}.{{ params.src_dataset_name }}.matches_info` e on a.season=e.season_year and a.match_id=e.match_id ;


CREATE  or replace TABLE `{{ params.project_id }}.{{ params.src_dataset_name }}.bowler_played_matches_temp` as 
select 
	season,match_id,bowler
from `{{ params.project_id }}.{{ params.src_dataset_name }}.ball_by_ball_info` a 
group by 1,2,3 ;


CREATE  or replace TABLE `{{ params.project_id }}.{{ params.src_dataset_name }}.bowler_over_order_wicket_cnt_temp`  as
select 
a.season,a.bowler, a.match_id,a.over_id,is_wicket_taken,over_wicket_cnt,
row_number() over(partition by a.season,a.match_id,a.bowler order by a.over_id asc) as over_order_num
from 
(SELECT 
season,bowler, match_id,over_id,max(bowler_wicket)as is_wicket_taken,sum(bowler_wicket) as over_wicket_cnt
from `{{ params.project_id }}.{{ params.src_dataset_name }}.ball_by_ball_info` 
group by 1,2,3,4) a ;

	

CREATE  or replace TABLE `{{ params.project_id }}.{{ params.src_dataset_name }}.bowler_current_over_next_over_temp` AS
select 
a.season,a.bowler,a.match_id,
a.over_id as current_over ,
a.is_wicket_taken as current_over_is_wicket_taken,
a.over_order_num as current_over_over_order_num,
a.over_wicket_cnt as current_over_over_wicket_cnt,
b.over_id as next_over ,
b.is_wicket_taken as next_over_is_wicket_taken,
b.over_order_num as next_over_over_order_num,
b.over_wicket_cnt as next_over_over_wicket_cnt
from  `{{ params.project_id }}.{{ params.src_dataset_name }}.bowler_over_order_wicket_cnt_temp` a 
join 
`{{ params.project_id }}.{{ params.src_dataset_name }}.bowler_over_order_wicket_cnt_temp` b on a.season=b.season and a.match_id=b.match_id 
                              and a.bowler=b.bowler and a.over_order_num+1=b.over_order_num;


CREATE  or replace TABLE `{{ params.project_id }}.{{ params.src_dataset_name }}.bowler_cont_wick_taken_overs_temp` AS
select a.*,b.player_name from `{{ params.project_id }}.{{ params.src_dataset_name }}.bowler_current_over_next_over_temp`  a
join `{{ params.project_id }}.{{ params.src_dataset_name }}.player_info` b on a.bowler=b.player_id
where  
(current_over_over_wicket_cnt=0 and next_over_over_wicket_cnt>=3) or
(current_over_over_wicket_cnt>=3 and next_over_over_wicket_cnt=0) or
(current_over_over_wicket_cnt>=1 and next_over_over_wicket_cnt>=2) or
(current_over_over_wicket_cnt>=2 and next_over_over_wicket_cnt>=1) ;

	
CREATE  or replace TABLE `{{ params.project_id }}.{{ params.src_dataset_name }}.wicket_taken_balls_temp` AS
select  
season,bowler,match_id,over_id,ball_id
from `{{ params.project_id }}.{{ params.src_dataset_name }}.ball_by_ball_info` where  bowler_wicket=1 ;

	
CREATE  or replace TABLE `{{ params.project_id }}.{{ params.src_dataset_name }}.overs_with_contunious_wickets_temp` AS
select a.*,
row_number() over(partition by a.season,a.match_id,a.bowler order by a.over_id,a.ball_id) as rnk
from 
(select 
a.season,a.bowler,a.match_id,a.current_over as over_id ,b.ball_id
from `{{ params.project_id }}.{{ params.src_dataset_name }}.bowler_cont_wick_taken_overs_temp` a
left join 
`{{ params.project_id }}.{{ params.src_dataset_name }}.wicket_taken_balls_temp` b on a.season=b.season and a.bowler=b.bowler and a.match_id=b.match_id 
and a.current_over=b.over_id
union all
select 
a.season,a.bowler,a.match_id,a.next_over as over_id ,b.ball_id
from `{{ params.project_id }}.{{ params.src_dataset_name }}.bowler_cont_wick_taken_overs_temp` a
left join 
`{{ params.project_id }}.{{ params.src_dataset_name }}.wicket_taken_balls_temp` b on a.season=b.season and a.bowler=b.bowler and a.match_id=b.match_id 
and a.next_over=b.over_id) a ;


CREATE  or replace TABLE `{{ params.project_id }}.{{ params.src_dataset_name }}.hatrick_temp` AS
select  
a.season,a.bowler,a.match_id,a.over_id as over_id_1,
a.ball_id as ball_id_1,b.over_id as  over_id_2 ,b.ball_id as  ball_id_2,
c.over_id as over_id_3,c.ball_id as ball_id_3
from `{{ params.project_id }}.{{ params.src_dataset_name }}.overs_with_contunious_wickets_temp` a ,
`{{ params.project_id }}.{{ params.src_dataset_name }}.overs_with_contunious_wickets_temp` b,
`{{ params.project_id }}.{{ params.src_dataset_name }}.overs_with_contunious_wickets_temp` c 
where a.season=b.season and a.match_id=b.match_id and a.bowler=b.bowler and a.rnk+1=b.rnk
and b.season=c.season and b.match_id=c.match_id and b.bowler=c.bowler and b.rnk+1=c.rnk;