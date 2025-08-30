
SELECT COUNT(*) FROM nypd_calls_2;

SELECT COUNT(DISTINCT cad_evnt_id) FROM nypd_calls_for_service ncfs;

SELECT "CREATE_DATE" FROM "Project Sql".nypd_calls_for_service;

SELECT "INCIDENT_TIME"  FROM nypd_calls_2;

CREATE TABLE nypd_calls_2 (LIKE "Project Sql".nypd_calls_for_service INCLUDING ALL);

INSERT INTO nypd_calls_2 SELECT * FROM "Project Sql".nypd_calls_for_service;

-- modifying create_date
ALTER TABLE nypd_calls_2
ALTER COLUMN "CREATE_DATE" TYPE DATE
USING TO_DATE("CREATE_DATE", 'MM/DD/YYYY');

SELECT distinct TO_CHAR("CREATE_DATE", 'Month') AS month_name FROM nypd_calls_2;




-- modifying indcident_date
ALTER TABLE nypd_calls_2
ADD COLUMN "INCIDENT_DATE_NEW" DATE;
UPDATE nypd_calls_2
SET "INCIDENT_DATE_NEW" = TO_DATE("INCIDENT_DATE", 'MM/DD/YYYY');


-- modifying incident time
ALTER TABLE nypd_calls_2
ALTER COLUMN "INCIDENT_TIME" TYPE TIME USING "INCIDENT_TIME"::TIME;
SELECT EXTRACT(hour FROM "INCIDENT_TIME") FROM nypd_calls_2;


--modifying ts columns
ALTER TABLE nypd_calls_2 
ADD COLUMN "ADD_TS_2" TIMESTAMP,
ADD COLUMN "DISP_TS_2" TIMESTAMP,
ADD COLUMN "ARRIVD_TS_2" TIMESTAMP,
ADD COLUMN "CLOSNG_TS_2" TIMESTAMP;
UPDATE nypd_calls_2
SET "ADD_TS_2" = TO_TIMESTAMP("ADD_TS", 'MM/DD/YYYY HH12:MI:SS AM');
UPDATE nypd_calls_2
SET "DISP_TS_2" = TO_TIMESTAMP("DISP_TS", 'MM/DD/YYYY HH12:MI:SS AM');
UPDATE nypd_calls_2
SET "ARRIVD_TS_2" = TO_TIMESTAMP("ARRIVD_TS", 'MM/DD/YYYY HH12:MI:SS AM')
WHERE "ARRIVD_TS" <> '';
UPDATE nypd_calls_2
SET "CLOSNG_TS_2" = TO_TIMESTAMP("CLOSNG_TS", 'MM/DD/YYYY HH12:MI:SS ')
WHERE "CLOSNG_TS" <> '';


-- Renamed all the above columns and deleted original ones






