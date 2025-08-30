
SELECT COUNT(DISTINCT "CAD_EVNT_ID") AS unique_events, 
       COUNT(*) AS total_events
FROM nypd_calls_2;



 ----- *****     DATA CLEANING  ****** ------


-- checking duplicates
WITH duplicates_cte AS (
  SELECT *,
  COUNT(*) OVER (PARTITION BY "CAD_EVNT_ID") AS id_count
  FROM nypd_calls_2
)
select * from duplicates_cte order BY "CAD_EVNT_ID", "duplicates_row_num"  ;


select count(*) from nypd_calls_2  where duplicates_row_num =1;

SELECT COUNT(*) FROM nypd_calls_2 WHERE "ARRIVD_TS_2" is null;
select count(*) from nypd_calls_2 where "ARRIVD_TS" is null and "duplicates_row_num" > 1;


-- deleting nulls from "arrive_ts" columns and handles duplicates
delete  from nypd_calls_2 where "duplicates_row_num" > 1;


ALTER TABLE nypd_calls_2 
ADD COLUMN "ARRIVD_TS_2" TIMESTAMP;

-- Updated nulls in ARRIVD_TS, CLOSNG_TS columns by replacing them with avg of time difference in before step's time 
-- and adding it to the same before step's time.

UPDATE nypd_calls_2
SET "ARRIVD_TS_2" = "DISP_TS" + avg_diff.avg_delay
FROM (
    SELECT AVG("ARRIVD_TS" - "DISP_TS") AS avg_delay
    FROM nypd_calls_2
    WHERE "ARRIVD_TS" IS NOT NULL
) AS avg_diff
WHERE "ARRIVD_TS" IS NULL;

select * from nypd_calls_2 where "ADD_TS" is null or "DISP_TS" is null or "ARRIVD_TS" is null or "CLOSNG_TS" is null
or "CREATE_DATE" is null or "INCIDENT_DATE" is null; 

UPDATE nypd_calls_2
SET "CLOSNG_TS" = "ARRIVD_TS" + avg_diff.avg_delay
FROM (
    SELECT AVG("CLOSNG_TS" - "ARRIVD_TS" ) AS avg_delay
    FROM nypd_calls_2
    WHERE "CLOSNG_TS" IS NOT NULL
) AS avg_diff
WHERE "CLOSNG_TS" IS NULL;

-- checking nulls in other columns

select distinct "NYPD_PCT_CD" from nypd_calls_2; --has nulls
select distinct "BORO_NM" from nypd_calls_2; -- has null as text (null)
select distinct "PATRL_BORO_NM" from nypd_calls_2; -- has null as text

select "NYPD_PCT_CD" , count(*) from nypd_calls_2 group by "NYPD_PCT_CD";

-- set null to 0 as unknow pretinct call
update nypd_calls_2 set "NYPD_PCT_CD"=0 where "NYPD_PCT_CD" is null


select "NYPD_PCT_CD" , "BORO_NM" ,"PATRL_BORO_NM", count(*) from nypd_calls_2 group by "BORO_NM", "NYPD_PCT_CD" ,"PATRL_BORO_NM";

select "PATRL_BORO_NM" , count(*) from nypd_calls_2 group by "PATRL_BORO_NM";

select * from nypd_calls_2 where "PATRL_BORO_NM" = '(null)';

select "RADIO_CODE","TYP_DESC" , count(*) from nypd_calls_2 group by "RADIO_CODE","TYP_DESC";

update nypd_calls_2 set "types_desc_backup"= "TYP_DESC" ;


ALTER TABLE nypd_calls_2 
ADD COLUMN type_desc_1 varchar(100),
ADD COLUMN type_desc_2 varchar(100);

update nypd_calls_2 set type_desc_1  = SPLIT_PART("TYP_DESC" , ':', 1),
  type_desc_2  = SPLIT_PART("TYP_DESC" , ':', 2);

select "RADIO_CODE" , "TYP_DESC" , type_desc_1 , type_desc_2  from nypd_calls_2 ;

update nypd_calls_2 set type_desc_1  = TRIM(type_desc_1),
  type_desc_2  = TRIM(type_desc_2);

update nypd_calls_2 set type_desc_2=type_desc_1  where type_desc_2 = '';

select "CIP_JOBS"   , count(*) from nypd_calls_2 group by "CIP_JOBS"  order by count(*) desc;

