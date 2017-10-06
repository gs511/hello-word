-- show tables;
-- create table for mintimestam
drop table if EXISTS pilot_sessionAndMinTimeStamp;
create table pilot_sessionAndMinTimeStamp as select sessionId, min((timestamp*1)) as ptimestamp from (select sessionId, timestamp from cs_search where location like '%meal-planning%') abc group by sessionId;


drop table if EXISTS pilot_pilotSubset1;
create table pilot_pilotSubset1 as select * from cs_search where sessionId in (select distinct(sessionId) from cs_search where location like '%meal-planning%') ;


drop table if EXISTS pilot_pilotSubset2;
create table pilot_pilotSubset2 as select a.*,b.ptimestamp from
  pilot_pilotSubset1 a join pilot_sessionAndMinTimeStamp b
where a.sessionId=b.sessionId and b.ptimestamp<=a.timestamp;

-- creating bounced session records
 drop table if EXISTS pilot_bounced_session_records;

create table pilot_bounced_session_records as select * from pilot_pilotSubset2
    where sessionId in (select DISTINCT(sessionId) from pilot_pilotSubset2
      group by sessionId having count(distinct(location))=1 and max((scroll_position*1))<2327) ;

 drop table if EXISTS pilot_unbounced_session_records;

create table pilot_unbounced_session_records as select * from pilot_pilotSubset2
where sessionId not in (select distinct(sessionId) from pilot_bounced_session_records);


 -- un bounced records
select count(DISTINCT(sessionId)) from pilot_unbounced_session_records;
select count(DISTINCT(sessionId)) from pilot_pilotSubset2;
-- pilot subset 2 is contains all the data of the meal-plan

-- getting unique sessions

select count(DISTINCT(sessionId)),week(from_unixtime((TIMESTAMP*1)/1000,"%Y-%c-%d"))as converted_time from pilot_pilotSubset2 group by converted_time;


-- getting unique visitors

select count(DISTINCT(partyId)),week(from_unixtime((TIMESTAMP*1)/1000,"%Y-%c-%d"))as converted_time
from pilot_pilotSubset2 group by converted_time;

-- repeat visitos

select count(partyId),converted_time from(
select partyId,count(DISTINCT(sessionId)) as session_count,week(from_unixtime((TIMESTAMP*1)/1000,"%Y-%c-%d"))as converted_time from
  pilot_pilotSubset2 group BY partyId,converted_time) abcd where session_count>1 group by  converted_time;

-- page views

select avg(session_count),week_number from(select count(distinct(location))
as session_count,sessionId,week(from_unixtime((timestamp*1)/1000,"%Y-%c-%d")) as week_number from pilot_pilotSubset2
  group by sessionId,week_number) abcd group by week_number;


-- non bounced session

select count(distinct(sessionId)) as session_count,week(from_unixtime(timestamp/1000,"%Y-%c-%d"))
as converted_time from pilot_unbounced_session_records group by converted_time;

-- time spent per visit

select avg(time_diff),converted_time from(
select timestampdiff(SECOND,from_unixtime(min(timestamp)/1000),from_unixtime(max(TIMESTAMP/1000))) as time_diff,sessionId
,week(from_unixtime(timestamp/1000,"%Y-%c-%d"))
as converted_time
from pilot_unbounced_session_records
group  by sessionId,converted_time) abc GROUP BY converted_time;


-- pilot hero 1\

-- wave 1
select count(distinct(sessionId)) as session_count,week(from_unixtime(timestamp/1000,"%Y-%c-%d"))
as converted_time from pilot_unbounced_session_records
where location like '%healthy-eating%vegetables%' group by converted_time;

-- wave 2
select count(distinct(sessionId)) as session_count,week(from_unixtime(timestamp/1000,"%Y-%c-%d"))
as converted_time from pilot_unbounced_session_records where location like '%everything-need-know-slow-cooking-success%' group by converted_time;

-- wave3
select count(distinct(sessionId)) as session_count,week(from_unixtime(timestamp/1000,"%Y-%c-%d"))
as converted_time from pilot_unbounced_session_records where location like '%5%ways%make%meals%affordable%' group by converted_time;


-- pilot hero 2

-- wave 1
select count(distinct(sessionId)) as session_count,week(from_unixtime(timestamp/1000,"%Y-%c-%d"))
as converted_time from pilot_unbounced_session_records where location like '%cook-from-the-pantry%' group by converted_time

-- wave 2
select count(distinct(sessionId)) as session_count,week(from_unixtime(timestamp/1000,"%Y-%c-%d"))
as converted_time from pilot_unbounced_session_records where location like '%cooking%tipssmart%substitutions%' group by converted_time

-- wave 3
select count(distinct(sessionId)) as session_count,week(from_unixtime(timestamp/1000,"%Y-%c-%d"))
as converted_time from pilot_unbounced_session_records where location like '%santa%fe%chicken%saut%' group by converted_time



-- pilot hero 3

-- wave 1
select count(distinct(sessionId)) as session_count,week(from_unixtime(timestamp/1000,"%Y-%c-%d"))
as converted_time from  pilot_unbounced_session_records where location like '%family-favorites%' group by converted_time;

-- wave 2
select count(distinct(sessionId)) as session_count,week(from_unixtime(timestamp/1000,"%Y-%c-%d"))
as converted_time from  pilot_unbounced_session_records where location like '%delicious%make%ahead%freezer%meals%' group by converted_time;

-- wave 3

select count(distinct(sessionId)) as session_count,week(from_unixtime(timestamp/1000,"%Y-%c-%d"))
as converted_time from  pilot_unbounced_session_records where location like '%cook%eat%twice%' group by converted_time;

-- pilot trending 1

-- wave 1
select count(distinct(sessionId)) as session_count,week(from_unixtime(timestamp/1000,"%Y-%c-%d"))
as converted_time from pilot_unbounced_session_records where location like '%5-hints-busy-cooks%' group by converted_time

-- wave 2
select count(distinct(sessionId)) as session_count,week(from_unixtime(timestamp/1000,"%Y-%c-%d"))
as converted_time from pilot_unbounced_session_records where location like '%the-prepared-pantry%' group by converted_time

-- wave3

select count(distinct(sessionId)) as session_count,week(from_unixtime(timestamp/1000,"%Y-%c-%d"))
as converted_time from pilot_unbounced_session_records where location like '%3%ways%shop%budget%' group by converted_time


-- pilot trending 2

-- wave 1
select count(distinct(sessionId)) as session_count,week(from_unixtime(timestamp/1000,"%Y-%c-%d"))
as converted_time from pilot_unbounced_session_records  where location like '%helpful-hints-busy-times%' group by converted_time

-- wave 2
select count(distinct(sessionId)) as session_count,week(from_unixtime(timestamp/1000,"%Y-%c-%d"))
as converted_time from pilot_unbounced_session_records  where location like '%storing-prepped-foods%' group by converted_time

-- wave 3
select count(distinct(sessionId)) as session_count,week(from_unixtime(timestamp/1000,"%Y-%c-%d"))
as converted_time from pilot_unbounced_session_records where location like '%7%dinners%70%' group by converted_time


-- pilot trending 3

-- wave 1
select count(distinct(sessionId)) as session_count,week(from_unixtime(timestamp/1000,"%Y-%c-%d"))
as converted_time from pilot_unbounced_session_records where location like '%fast-prep-slow-cook%' group by converted_time;
-- wave 2
select count(distinct(sessionId)) as session_count,week(from_unixtime(timestamp/1000,"%Y-%c-%d"))
as converted_time from pilot_unbounced_session_records where location like '%tips-for-busy-cooks%' group by converted_time;

-- wave 3
select count(distinct(sessionId)) as session_count,week(from_unixtime(timestamp/1000,"%Y-%c-%d"))
as converted_time from pilot_unbounced_session_records where location like '%quick-30-minute-dinners%' group by converted_time;




-- non recipe

-- wave 1
select count(distinct(sessionId)) as session_count,week(from_unixtime(timestamp/1000,"%Y-%c-%d"))
as converted_time from pilot_unbounced_session_records where location like '%fast-prep-slow-cook%'
or location like '%helpful-hints-busy-times%'
or location like '%5-hints-busy-cooks%'
or location like '%family-favorites%'
or location like '%cook-from-the-pantry%'
or location like '%healthy-eating%vegetables%'
# or click_text like "%check%out%the%kitchen%guide%"
group by converted_time;

-- wave 2

select count(distinct(sessionId)) as session_count,week(from_unixtime(timestamp/1000,"%Y-%c-%d"))
as converted_time from pilot_unbounced_session_records where location like '%fast-prep-slow-cook%'
or location like '%everything-need-know-slow-cooking-success%'
or location like '%cooking%tipssmart%substitutions%'
or location like '%delicious%make%ahead%freezer%meals%'
or location like '%the-prepared-pantry%'
or location like '%storing-prepped-foods%'
or location like '%tips-for-busy-cooks%'
# or click_text like "%check%out%the%kitchen%guide%"
group by converted_time;


-- article

select count(distinct(sessionId)) as session_count,week(from_unixtime(timestamp/1000,"%Y-%c-%d"))
as converted_time from pilot_unbounced_session_records where location like "%guide%"  group by converted_time;

-- tips

select count(distinct(sessionId)) as session_count,week(from_unixtime(timestamp/1000,"%Y-%c-%d"))
as converted_time from pilot_unbounced_session_records where click_text like "%check%out%the%kitchen%guide%" group by converted_time


-- recipe

select count(distinct(sessionId)) as session_count,week(from_unixtime(timestamp/1000,"%Y-%c-%d"))
as converted_time from pilot_unbounced_session_records  where location like "%recipe%" group by converted_time;

-- meal planning

select count(distinct(sessionId)) as session_count,week(from_unixtime(timestamp/1000,"%Y-%c-%d"))
  as converted_time from pilot_unbounced_session_records where location like '%family-favorites%'
  OR location like "%helpful-hints-busy-times%"
  or location like '%cook-from-the-pantry%'
group by converted_time;

-- wave 2
select count(distinct(sessionId)) as session_count,week(from_unixtime(timestamp/1000,"%Y-%c-%d"))
as converted_time from pilot_unbounced_session_records where location like '%everything-need-know-slow-cooking-success%'
OR location like  '%the-prepared-pantry%'
or location like '%tips-for-busy-cooks%'
group by converted_time;

-- wave 3

select count(distinct(sessionId)) as session_count,week(from_unixtime(timestamp/1000,"%Y-%c-%d"))
as converted_time from pilot_unbounced_session_records where location like '%5%ways%make%meals%affordable%'
or location like '%3%ways%shop%budget%'
  group by converted_time;
-- meal making

select count(distinct(sessionId)) as session_count,week(from_unixtime(timestamp/1000,"%Y-%c-%d"))
as converted_time from pilot_unbounced_session_records where location like "%healthy-eating%vegetables%"
OR location like "%5-hints-busy-cooks%"
or location like "%fast-prep-slow-cook%"
group by converted_time;

-- wave 2
select count(distinct(sessionId)) as session_count,week(from_unixtime(timestamp/1000,"%Y-%c-%d"))
as converted_time from pilot_unbounced_session_records where location like '%helpful-hints-busy-times%'
OR location like '%cooking%tipssmart%substitutions%'
or location like '%delicious%make%ahead%freezer%meals%'
group by converted_time;

-- wave 3
select count(distinct(sessionId)) as session_count,week(from_unixtime(timestamp/1000,"%Y-%c-%d"))
as converted_time from pilot_unbounced_session_records where location like '%quick-30-minute-dinners%'
or location like '%cook%eat%twice%'
  group by converted_time;

-- recipe content
-- wave 3
select count(distinct(sessionId)) as session_count,week(from_unixtime(timestamp/1000,"%Y-%c-%d"))
as converted_time from pilot_unbounced_session_records where location like '%santa%fe%chicken%saut%'
or location like '%7%dinners%70%'
  group by converted_time;

-- referer distribution

-- create table for the distibution
drop table if exists first_in_session_pilot_unbounced;

create table first_in_session_pilot_unbounced as
    select min(timestamp) as mt,sessionId from pilot_unbounced_session_records group by sessionId;

drop table if EXISTS pilot_by_source;

create table pilot_by_source as
select b.*,a.mt from first_in_session_pilot_unbounced a join pilot_unbounced_session_records b
where a.mt=b.timestamp and a.sessionId=b.sessionId;

-- creating table for referer timestamp with fistinsession timestamp

SELECT
    CASE
         WHEN referer LIKE '%campbell-soup%' THEN 'campbell-soup'
        WHEN referer LIKE "%swanson%" THEN "swanson"
        WHEN referer LIKE '%facebook%' THEN 'Facebook'
        WHEN referer LIKE '%instagram%' THEN 'Instagram'
        WHEN referer LIKE '%pin%' THEN 'Pinterest'
        WHEN referer LIKE '%pacefood%' THEN 'pacefood'
        WHEN referer LIKE '%prego%' THEN 'prego'
        when referer like "%mealmail%" then 'mealmail'
        when referer like "%creativecooking%" then "creativecooking"
			  when referer like "" or referer like "%none%" or referer like "%yahoo%" or referer like "%outlook%" then "None"
        WHEN referer LIKE '%campbell%kitchen%' THEN 'ck'
        ELSE "others"
    END AS referers,
    COUNT(DISTINCT (sessionId)) AS session_count,
    week(FROM_UNIXTIME(timestamp / 1000, '%Y-%c-%d')) AS converted_time
FROM pilot_by_source
GROUP BY referers , converted_time ORDER BY  converted_time ;

-- join scrapped table with  bounce table

drop TABLE if EXISTS brands_unbounced_pilot_data;

create table brands_unbounced_pilot_data as
    select a.sessionId,b.brands_engaged,b.brands_count,a.timestamp,a.location
    from pilot_unbounced_session_records a join scrapped_data_with_locationId b
    where a.locationId=b.locationId;

-- wave 2
drop table if exists substring_location;

create table substring_location AS
  select *,substring_index(location,"?",1) as sub_location
  from pilot_unbounced_session_records where location like "%recipe%" ;

drop table if exists brands_unbounced_pilot_data;
create table brands_unbounced_pilot_data as
    select a.sessionId,b.brands_engaged,b.brands_count,a.timestamp,a.location
    from substring_location a join scrapped_data b
    where b.location like  a.sub_location ;

-- brand wise stuff

# select count(*) from brands_unbounced_pilot_data;
 -- prego
select count(distinct(sessionId)),week(from_unixtime(timestamp/1000,"%Y-%c-%d"))
as converted_time from brands_unbounced_pilot_data where brands_engaged like "%prego%" GROUP BY converted_time;

-- sawnson
select count(distinct(sessionId)),week(from_unixtime(timestamp/1000,"%Y-%c-%d"))
as converted_time from brands_unbounced_pilot_data where brands_engaged like "%swanson%" GROUP BY converted_time;

-- r&w soup

 select count(distinct(sessionId)),week(from_unixtime(timestamp*1/1000,"%Y-%c-%d"))
as converted_time from brands_unbounced_pilot_data where brands_engaged like "%campbell%" GROUP BY converted_time;

-- pace

 select count(distinct(sessionId)),week(from_unixtime(timestamp/1000,"%Y-%c-%d"))
as converted_time from brands_unbounced_pilot_data where brands_engaged like "%pace%" GROUP BY converted_time;

-- soup + prego

 select count(distinct(sessionId)),week(from_unixtime(timestamp/1000,"%Y-%c-%d"))
as converted_time from brands_unbounced_pilot_data  where brands_engaged like "%campbell%" and brands_engaged like "%prego%"
GROUP BY converted_time;

-- campbell +sawnson

 select count(distinct(sessionId)),week(from_unixtime(timestamp/1000,"%Y-%c-%d"))
as converted_time from brands_unbounced_pilot_data where brands_engaged like "%campbell%" and brands_engaged like "%swanson%"
GROUP BY converted_time;

-- soup + pace

select count(distinct(sessionId)),week(from_unixtime(timestamp/1000,"%Y-%c-%d"))
as converted_time from brands_unbounced_pilot_data
where brands_engaged like "%campbell%" and brands_engaged like "%pace%" GROUP BY converted_time;

-- swanson + prego


select count(distinct(sessionId)),week(from_unixtime(timestamp/1000,"%Y-%c-%d"))
as converted_time from brands_unbounced_pilot_data where brands_engaged like "%swanson%" and brands_engaged like "%pergo%"
GROUP BY converted_time;

-- swanson + pace

select count(distinct(sessionId)),week(from_unixtime(timestamp/1000,"%Y-%c-%d"))
as converted_time from brands_unbounced_pilot_data where brands_engaged like "%swanson%" and brands_engaged like "%pace%"
GROUP BY converted_time;

-- prego +pace

select count(distinct(sessionId)),week(from_unixtime(timestamp/1000,"%Y-%c-%d"))
as converted_time from brands_unbounced_pilot_data where brands_engaged like "%prego%" and brands_engaged like "%pace%"
GROUP BY converted_time;


-- scroll % for the un bounced
select avg(max_p),converted_time
from(
select max(scroll_position*1) as max_p,sessionId,week(from_unixtime((TIMESTAMP*1)/1000,"%Y-%c-%d"))as converted_time
from pilot_unbounced_session_records where location like "%meal-planning%"
group by sessionId,converted_time) abc group by converted_time;

-- scroll % for all the sessions

select avg(max_p),converted_time
from(
select max(scroll_position*1) as max_p,sessionId,week(from_unixtime((TIMESTAMP*1)/1000,"%Y-%c-%d"))as converted_time
from pilot_pilotSubset2 where location like "%meal-planning%"
group by sessionId,converted_time) abc group by converted_time;


-- video completion

select count(DISTINCT(sessionId)),week(from_unixtime((TIMESTAMP*1)/1000,"%Y-%c-%d"))as converted_time from pilot_unbounced_session_records
 where click_text like "%_video_%" group by converted_time;

-- subscribe

select count(DISTINCT(sessionId)),week(from_unixtime((TIMESTAMP*1)/1000,"%Y-%c-%d"))as converted_time from pilot_unbounced_session_records
 where click_text like "%subscribe%" group by converted_time;

-- video time spent
drop table if exists unbounced_pilot_session_video;
create table unbounced_pilot_session_video as
select *,@curRank := @curRank + 1 AS rank,from_unixtime(TIMESTAMP/1000) as converted_time from pilot_unbounced_session_records,(SELECT @curRank := 0) r
where sessionId in (
  select sessionId from pilot_unbounced_session_records  where click_text like "%_video_%"
);

select count(click_text) as cnt,sessionId from unbounced_pilot_session_video where click_text like "%_video_%"
group by sessionId order by cnt
desc;

delete from unbounced_pilot_session_video where sessionId =1663620;
delete from unbounced_pilot_session_video where sessionId =1525491;

drop table if exists max_time_video_pilot;
create table max_time_video_pilot as
select max(timestamp) as m_time,sessionId from unbounced_pilot_session_video GROUP BY  sessionId;

drop table if exists video_click_text_max_timestamp;
create table video_click_text_max_timestamp as
  select a.click_text,a.sessionId from unbounced_pilot_session_video a JOIN max_time_video_pilot b WHERE
  b.m_time =a.TIMESTAMP;

delete from unbounced_pilot_session_video where sessionId in
                                                (select video_click_text_max_timestamp.sessionId
                                                 from video_click_text_max_timestamp where video_click_text_max_timestamp.click_text
                                                                                           like "%_video_%");

select avg(timedifference),week(from_unixtime((TIMESTAMP*1)/1000,"%Y-%c-%d"))as converted_week
FROM (
  SELECT
    (timestampdiff(second,A.converted_time,B.converted_time)) AS timedifference,
    A.timestamp
  FROM unbounced_pilot_session_video A INNER JOIN unbounced_pilot_session_video B ON B.rank = (A.rank + 1)
  WHERE B.click_text LIKE "%_video_%"
) abc group by converted_week;


select * from unbounced_pilot_session_video limit 10;

-- for getting content date
SELECT
    CASE
    when referer like "%mealmail%8%14%" then 'mealmail 14-8'
     when referer like "%mealmail%8%21%" then 'mealmail 21-8'
      when referer like "%mealmail%8%28%" then 'mealmail 28-8'
#     when referer like "%mealmail%" then referer
      else "none"
    END AS referers,
    COUNT(DISTINCT (sessionId)) AS session_count,
    week(FROM_UNIXTIME(timestamp / 1000, '%Y-%c-%d')) AS converted_time
FROM pilot_by_source
GROUP BY referers , converted_time ORDER BY  converted_time ;



  -- old defination bounce

kill 313;

show PROCESSLIST ;

select count(max_p) from(
select max(scroll_position*1) as max_p,sessionId,count(distinct(location)) from kitchen_unbounced_session_records
  group by sessionId having count(distinct(location))=1
) abc where max_p>2584;

select count(distinct(location)) as cnt,location from cs_search where location like "%kitchen%" group by location order by cnt desc


select count(distinct(sessionId)) from cs_search where location like "%kitchen%";


select count(DISTINCT(location)) as cont_loc,sessionId from cs_search GROUP BY sessionId;

show processlist;

select referer from ck_unbounced_session where referer like "%campbell%kitchen%" and firstInSession="True";

kill 424;

select

kill 355;

kill 426;

select sessionId from ck_first_loc_1 group by sessionId having count(DISTINCT(location))>1;

select distinct(location) from ck_first_loc_1 where sessionId=7253;


select count(distinct(sessionId)) as session_count,week(from_unixtime(timestamp/1000,"%Y-%c-%d"))
as converted_time from cs_search where firstInSession="True" group by converted_time;

select count(distinct(sessionId)) as session_count from cs_search;

select count(distinct(sessionId)) as session_count,week(from_unixtime(timestamp/1000,"%Y-%c-%d"))
as converted_time from ck_unbounced_session where firstInSession="True" group by converted_time;


show PROCESSLIST;


-- getting count using substring

select count(distinct(sessionId)),substring_index(referer,"linkId",1) as ref from temp_fb_3 where referer like "%linkId%" group by ref ;

create temporary table temp_fb_1 as select * from cs where location like "%facebook%" or referer like "%facebook%" or location like "%fb%" or referer like "%fb%";

