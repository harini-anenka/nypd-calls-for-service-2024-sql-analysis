                                  --------- DESCRIPTIVE ANALYSIS---------

-- TOTAL NUMBER OF CALLS (OVERALL + PER QUARTER)
select
	extract(quarter from "CREATE_DATE") as "Quarter[2024]" ,
	count(*) "Total Calls"
from
	nypd_calls_2
group by
	extract(quarter from "CREATE_DATE")
order by
	count(*) desc;

-- CALLS BY TYPE CATEGORY TOP 10
select
	type_desc_1 as "Call Category" ,
	count(*) "Total Calls"
from
	nypd_calls_2
group by
	type_desc_1
order by
	"Total Calls" desc
limit 10;

-- CALLS BY BOROUGH
select
	"BORO_NM" as "Borough Name" ,
	count(*) "Total Calls"
from
	nypd_calls_2
group by
	"BORO_NM"
order by
	"Total Calls" desc;

-- CALLS PER DAY OF WEEK
select
	to_char("CREATE_DATE", 'Day') as "Day" ,
	count(*) "Total Calls"
from
	nypd_calls_2
group by
	to_char("CREATE_DATE", 'Day')
order by
	"Total Calls" desc;


-- TOP 10 BUSIEST PRECINCTS
select
	"NYPD_PCT_CD" as "Percinct" ,
	"BORO_NM" ,
	count(*) "Total Calls"
from
	nypd_calls_2
group by
	"NYPD_PCT_CD",
	"BORO_NM"
order by
	"Total Calls" desc
limit 10;

-- GROWTH/DECLINE IN CALLS ACROSS MONTHS (JAN–SEP 2024)
select
	TO_CHAR("CREATE_DATE", 'Month') as month,
	COUNT(*) as total_events
from
	nypd_calls_2
group by
	TO_CHAR("CREATE_DATE", 'Month'),
	extract(month from "CREATE_DATE")
order by
	extract(month from "CREATE_DATE");

-- TOP COMMON CALLS IN MONTHS AND DAYS 
with cte as(
select
    TO_CHAR("CREATE_DATE", 'Month') as "month", extract(month from "CREATE_DATE") as month_num,
    type_desc_1 ,
    COUNT(*) as total_calls
from nypd_calls_2
group by TO_CHAR("CREATE_DATE", 'Month'), extract(month from "CREATE_DATE") , type_desc_1
order by extract(month from "CREATE_DATE") asc, count(*) desc
) ,
 ranked as(
 select "month", month_num, type_desc_1 , total_calls, row_number() over(partition by "month" order by total_calls desc) rn
 from cte
 )
 select
	*
from
	ranked
where
	rn<6
order by
	month_num,
	rn;

with cte as(
select trim(to_char("CREATE_DATE", 'Day')) as "Day", type_desc_1 , type_desc_2, count(*) "Total Calls" 
from nypd_calls_2 
group by to_char("CREATE_DATE", 'Day'), type_desc_1, type_desc_2 order by "Day", "Total Calls" desc
),
ranked as (
select
	"Day",
	type_desc_1 ,
	type_desc_2,
	"Total Calls",
	row_number() over(partition by "Day" order by "Total Calls" desc) rn
from
	cte
)
select
	*
from
	ranked
where
	rn<4; -- and "Day" in ('Saturday','Sunday');


-- COMPARE QUARTER-OVER-QUARTER
select
	extract(quarter from "CREATE_DATE") as "Quarter[2024]" ,
	type_desc_1,
	type_desc_2 ,
	count(*) "Total Calls"
from
	nypd_calls_2
group by
	extract(quarter from "CREATE_DATE"),
	type_desc_1,
	type_desc_2
having
	count(*)>100000
order by
	count(*) desc ;

--WHICH BOROUGH HAS THE HIGHEST VOLUME OF SERIOUS INCIDENTS?
select
	"BORO_NM",
	"CIP_JOBS",
	count(*)
from
	nypd_calls_2
where
	"CIP_JOBS" = 'Serious'
group by
	"BORO_NM" ,
	"CIP_JOBS"
order by
	count(*) desc;

-- % OF CALLS FLAGGED AS CRIME IN PROGRESS (CIP)
with cte as(
select case when "CIP_JOBS" = 'Non CIP' then 'N'
else 'Y' end as cip_calls, type_desc_1, type_desc_2 , round(count(*)* 100.0 / sum(count(*)) over(), 3) as percent 
from nypd_calls_2 group by case when "CIP_JOBS" = 'Non CIP' then 'N'
else 'Y' end, type_desc_1, type_desc_2 order by percent desc),
cte2 as(
select *, row_number() over(partition by cip_calls, type_desc_1 order by percent desc) rn from cte)
select
	*
from
	cte2
where
	rn<6;


-- AVG TIME TAKEN FOR CALLS FLAGGED AS CRIME IN PROGRESS (CIP)
select
	"CIP_JOBS" ,
	AVG("CLOSNG_TS"-"ADD_TS" ) "Time Taken"
from
	nypd_calls_2
group by
	"CIP_JOBS"
order by
	"Time Taken";  































