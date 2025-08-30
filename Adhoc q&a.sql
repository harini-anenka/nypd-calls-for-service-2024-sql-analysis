/*1. CALL HANDLING EFFICIENCY ANALYSIS
BUSINESS CASE: HOW EFFICIENT IS THE NYPD IN HANDLING CALLS FROM ARRIVAL TO CLOSURE?
QUESTION:
OVER THE PAST 90 DAYS TYPES WITH AT LEAST 100 INCIDENTS, SHOW THE SLOWEST HANDLING TIMES, 
AND HOW DO THEY COMPARE TO OTHERS?*/
SELECT
    type_desc_1,
   round(avg(extract(EPOCH from ("CLOSNG_TS" - "ARRIVD_TS")) / 60), 2) as minutes
FROM
    nypd_calls_2
WHERE
    EXTRACT(QUARTER FROM "CREATE_DATE") = 3
GROUP BY
    type_desc_1
HAVING
    COUNT(*) >= 100
ORDER BY
    minutes;

/*2. UNUSUAL DELAY DETECTING
BUSINESS CASE: SOME INCIDENTS HAVE ABNORMALLY LONG DURATIONS. 
NYPD WANTS TO IDENTIFY POTENTIAL OUTLIERS THAT MAY REQUIRE INVESTIGATION.
QUESTION:
FIND THE TOP 10 INCIDENTS WITH THE LONGEST TIME (IN HOURS) BETWEEN ARRIVD_TS AND CLOSNG_TS.
RETURN THE CAD_EVNT_ID, TYP_DESC, BORO_NM, DURATION IN HOURS, AND THE RELEVANT TIMESTAMPS.
RESTRICT TO INCIDENTS THAT OCCURRED IN THE PAST 90 DAYS. */
select
	"CAD_EVNT_ID" ,
	type_desc_1,
	type_desc_2 ,
	"BORO_NM",
	"ARRIVD_TS" ,
	"CLOSNG_TS" ,
	"CLOSNG_TS"-"ARRIVD_TS" duration
from
	nypd_calls_2
where
	extract(month from "CREATE_DATE") <4
order by
	duration desc
limit 10;


/*3. END-TO-END RESPONSE LAG AUDIT
BUSINESS CASE: MEASURE OVERALL SYSTEM LATENCY FROM WHEN A CALL WAS ADDED TO WHEN IT WAS CLOSED.
QUESTION: 
IN THE PAST 12 WEEKS, WHAT’S THE AVERAGE TOTAL TIME IT TAKES TO HANDLE CALLS FROM START TO FINISH 
ACROSS EACH BOROUGH? ARE SOME BOROUGHS CONSISTENTLY SLOWER?*/
with cte1 as(
select "BORO_NM", round(avg(extract(EPOCH from ("CLOSNG_TS"- "ADD_TS")) / 60), 2) as "Latency"
from nypd_calls_2 group by "BORO_NM" order by "Latency"),
cte2 as(
select "BORO_NM" , "Latency" , row_number() over(order by "Latency") rn from cte1
),
cte3 as(
select "BORO_NM", to_char("CREATE_DATE", 'Day') week, round(extract(minute from avg("CLOSNG_TS"-"ADD_TS" )), 2) as "Latency"
from nypd_calls_2 group by "BORO_NM", to_char("CREATE_DATE", 'Day')
)
select
	c3."BORO_NM",
	c3.week,
	c3."Latency",
	c2."Latency" as avg_latency_per_borough
from
	cte3 c3
join cte2 c2 on
	c3."BORO_NM" = c2."BORO_NM"
order by
	c2.rn;

/*4. DISPATCH DELAY BUCKETS
	BUSINESS CASE: CLASSIFY RESPONSE DELAYS FOR DISPATCHERS.
QUESTION:
HOW QUICKLY ARE CALLS DISPATCHED ONCE THEY’RE LOGGED? 
CAN WE BUCKET INCIDENTS INTO RANGES (LIKE UNDER A MINUTE, 1–5 MINUTES, ETC.) 
AND SEE HOW EACH PATROL BOROUGH COMPARES?*/
with cte1 as(
select "CAD_EVNT_ID", "PATRL_BORO_NM", extract(EPOCH from ("DISP_TS" - "ADD_TS")) / 60 as "time taken" from nypd_calls_2
),
cte2 as(
select "CAD_EVNT_ID", "PATRL_BORO_NM",
case when "time taken"<1 then 'less than 1 minute'
     when "time taken" between 1 and 5 then 'less than 5 minutes'
 when "time taken" between 6 and 15 then 'less than 15 minutes'
     else 'more than 15 minutes' end
     as delay_dispatch from cte1
     )
select
	"PATRL_BORO_NM" ,
	delay_dispatch,
	count(*) "Total Calls" ,
	round(count(*)* 100.0 / sum(count(*)) over(), 2) percentage
from
	cte2
group by
	"PATRL_BORO_NM",
	delay_dispatch
order by
	count(*) desc;


/*5. CRIME IN PROGRESS — PERFORMANCE REVIEW
BUSINESS CASE: ARE CIP (CRIME IN PROGRESS) CALLS HANDLED FASTER?
QUESTION:
WHEN A CALL IS FLAGGED AS ‘CRIME IN PROGRESS’ ARE WE ACTUALLY RESPONDING FASTER COMPARED TO OTHER CALLS? 
OVER THE LAST 60 DAYS, HOW DIFFERENT ARE THOSE RESPONSE TIMES?.*/
select
	case
		when "CIP_JOBS" = 'Non CIP' then 'N'
		else 'Y'
	end as cip_calls,
	round( PERCENTILE_CONT(0.5) within group (order by extract(EPOCH from ( "ARRIVD_TS"-"DISP_TS")) / 60)::numeric , 2) as median_time,
	round(avg(extract(EPOCH from ("ARRIVD_TS"- "DISP_TS")) / 60), 2) avg_response_time
from
	nypd_calls_2
where
	extract(month from "CREATE_DATE") between 8 and 9
group by
	case
		when "CIP_JOBS" = 'Non CIP' then 'N'
		else 'Y'
	end ;


/*6. INCIDENT VOLUME DEVIATION DETECTION
BUSINESS CASE: SPOT PRECINCTS WITH ABNORMAL CHANGES IN CALL VOLUME.
QUESTION:
LOOKING AT THE PAST 6 WEEKS, ARE THERE ANY PRECINCTS WHERE WEEKLY CALL VOLUMES HAVE SUDDENLY SPIKED OR DROPPED 
MORE THAN 40% COMPARED TO THE PRIOR WEEK?*/
with mycte1 as(
select distinct "CAD_EVNT_ID", "NYPD_PCT_CD",
  "CREATE_DATE",
  case
    when "CREATE_DATE" between last_date - interval '7 days' and last_date then '1'
    when "CREATE_DATE" between last_date - interval '14 days' and last_date - interval '8 days' then '2'
    when "CREATE_DATE" between last_date - interval '21 days' and last_date - interval '15 days' then '3'
    when "CREATE_DATE" between last_date - interval '28 days' and last_date - interval '22 days' then '4'
    when "CREATE_DATE" between last_date - interval '35 days' and last_date - interval '29 days' then '5'
    when "CREATE_DATE" between last_date - interval '42 days' and last_date - interval '36 days' then '6'
  end as previous_n_week
from (
  select "CAD_EVNT_ID", "NYPD_PCT_CD", "CREATE_DATE", (select MAX("CREATE_DATE") from nypd_calls_2) as last_date
  from nypd_calls_2
) cte1 
order by "CREATE_DATE" desc),
mycte2 as(
select "NYPD_PCT_CD", previous_n_week , round(count(*)* 100 / sum(count(*)) over(), 2) percentage from mycte1 
where previous_n_week is not null and "NYPD_PCT_CD" != 0 group by "NYPD_PCT_CD", previous_n_week 
order by "NYPD_PCT_CD" , previous_n_week ),
final as(
select *, lag(percentage) over() Previous_week_percent, abs(lag(percentage) over()- percentage) absolute_diff,
round(abs(percentage-lag(percentage) over())/ lag(percentage)over(), 2) as relative_diff
from mycte2 )
select
	*,
	case
		when relative_diff >= 0.40 then '40% change'
	end as FLag
from
	final;


/*7. TOP CALL TYPES BY GEOGRAPHIC SPREAD
BUSINESS CASE: WHICH INCIDENT TYPES OCCUR ACROSS THE WIDEST AREA?
QUESTION:
WHICH INCIDENT TYPES ARE MOST WIDESPREAD ACROSS THE CITY? 
FOR EXAMPLE, WHICH CALL TYPES SHOW UP IN THE MOST PRECINCTS AND BOROUGHS?*/
select
	distinct "type_desc_1" ,
	"BORO_NM",
	"NYPD_PCT_CD",
	count(*)
from
	nypd_calls_2
group by
	"type_desc_1",
	"BORO_NM",
	"NYPD_PCT_CD"
order by
	count(*) desc
limit 10;


/*8 .ANALYZING CALL TYPES BY DAY VS. NIGHT ACTIVITY 
 BUSINESS CASE : WHICH RADIO CODES OCCUR MOST FREQUENTLY AT NIGHT VS DURING THE DAY?*/
select
	"RADIO_CODE" ,
	"BORO_NM",
	case
		when extract(hour from "ADD_TS") between 6 and 17 then 'Day'
		else 'Night'
	end as "day/night",
	count(*)
from
	nypd_calls_2
group by
	"RADIO_CODE" ,
	"BORO_NM",
	case
		when extract(hour from "ADD_TS") between 6 and 17 then 'Day'
		else 'Night'
	end
order by
	count(*) desc;


