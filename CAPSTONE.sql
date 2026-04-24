create OR replace database air;

create schema raw;

create schema stg;

create schema curated;

----------------------------------------------
-- FILE_FORMAT
----------------------------------------------

CREATE OR REPLACE FILE FORMAT CSV_FF
TYPE = CSV
SKIP_HEADER = 1
FIELD_DELIMITER = ',';

CREATE OR REPLACE FILE FORMAT JSON_FF
TYPE = JSON
STRIP_OUTER_ARRAY = TRUE;

----------------------------------------------
-- STAGE
----------------------------------------------

CREATE OR REPLACE STAGE AIRLINE_CSV
FILE_FORMAT = CSV_FF;

CREATE OR REPLACE STAGE AIRLINE_JSON
FILE_FORMAT = JSON_FF;

----------------------------------------------
-- RAW TABLES CSV
----------------------------------------------

CREATE OR REPLACE TRANSIENT TABLE RAW.AIRCRAFT_MASTER (
    aircraft_id           VARCHAR(20),
    tail_number           VARCHAR(20),
    aircraft_type         VARCHAR(100),
    fleet_group           VARCHAR(50),
    seating_capacity      NUMBER(5),
    maintenance_status    VARCHAR(50),
    base_airport          VARCHAR(10),
    manufacture_year      NUMBER(4)
);

CREATE OR REPLACE TRANSIENT TABLE RAW.FLIGHTS_SCHEDULED (
    flight_id                  VARCHAR(20),
    flight_number              VARCHAR(20),
    origin_airport             VARCHAR(10),
    destination_airport        VARCHAR(10),
    route_code                 VARCHAR(30),
    aircraft_id                VARCHAR(20),
    scheduled_departure_ts     TIMESTAMP,
    scheduled_arrival_ts       TIMESTAMP,
    service_type               VARCHAR(30),
    scheduled_status           VARCHAR(30)
);

CREATE OR REPLACE TRANSIENT TABLE RAW.CREW_ASSIGNMENTS (
    assignment_id          VARCHAR(20),
    flight_id              VARCHAR(20),
    crew_id                VARCHAR(20),
    crew_role              VARCHAR(50),
    base_airport           VARCHAR(10),
    certification_type     VARCHAR(100),
    duty_start_ts          TIMESTAMP,
    duty_end_ts            TIMESTAMP,
    assignment_status      VARCHAR(30)
);

CREATE OR REPLACE TRANSIENT TABLE RAW.AIRPORT_TURNAROUND_LOGS (
    turnaround_id STRING,
    flight_id STRING,
    airport_code STRING,
    turnaround_start_ts TIMESTAMP,
    turnaround_end_ts TIMESTAMP,
    fueling_complete_flag STRING,
    catering_complete_flag STRING,
    cleaning_complete_flag STRING,
    baggage_load_complete_flag STRING,
    readiness_status STRING
);

CREATE OR REPLACE TRANSIENT TABLE RAW.GATE_ALLOCATION (
    gate_allocation_id STRING,
    flight_id STRING,
    airport_code STRING,
    gate_number STRING,
    terminal STRING,
    allocation_status STRING,
    allocation_ts TIMESTAMP
);

CREATE OR REPLACE TRANSIENT TABLE RAW.MAINTENANCE_CLEARANCE (
    maintenance_event_id STRING,
    aircraft_id STRING,
    clearance_ts TIMESTAMP,
    status STRING,
    issue_category STRING,
    release_flag STRING,
    station_code STRING
);

SELECT * FROM RAW.AIRCRAFT_MASTER;

SELECT * FROM RAW.CREW_ASSIGNMENTS;

SELECT * FROM RAW.FLIGHTS_SCHEDULED;

SELECT * FROM RAW.MAINTENANCE_CLEARANCE;

----------------------------------------------
-- RAW TABLES JSON
----------------------------------------------

CREATE OR REPLACE TRANSIENT TABLE RAW.RAW_DISRUPTION_ALERTS (
    src VARIANT
);

copy into RAW_DISRUPTION_ALERTS
from @AIRLINE_JSON/disruption_alerts.json
file_format = JSON_FF;

create or replace TRANSIENT table RAW.DISRUPTION_ALERTS as
SELECT
    src:alert_id::STRING AS alert_id,
    src:flight_id::STRING AS flight_id,
    src:alert_ts::TIMESTAMP AS alert_ts,
    src:alert_type::STRING AS alert_type,
    src:severity::STRING AS severity,
    src:impacted_airport::STRING AS impacted_airport,
    src:sla_threshold_min::NUMBER AS sla_threshold,
    src:current_delay_estimate_min::NUMBER AS delay_estimate,
    src:recommendation::STRING AS recommendation
FROM RAW_DISRUPTION_ALERTS;

SELECT * FROM RAW.DISRUPTION_ALERTS;

SELECT * FROM RAW_DISRUPTION_ALERTS;

---------------------------------------------
-- 2ND JSON FILE
---------------------------------------------

CREATE OR REPLACE TRANSIENT TABLE RAW.RAW_FLIGHT_EVENTS(
    src variant
);

copy into RAW_FLIGHT_EVENTS
from @AIRLINE_JSON/flight_status_events.json
file_format = JSON_FF;

create or replace transient table RAW.FLIGHT_EVENTS as
select 
     src:event_id::string as event_id,
     src:flight_id::string as flight_id,
     src:event_ts::timestamp as event_ts,
     src:event_type::string as event_type,
     src:status_code::string as status_code,
     src:delay_reason::string as delay_reason,
     src:delay_minutes::number as delay_minutes,
     src:airport_code::string as airport_code,
    -- NESTED
     src:ops_context:gate::STRING AS gate,
     src:ops_context:terminal::STRING AS terminal,
     src:ops_context:source_system::STRING AS source_system,
    -- FLATTEN
     f.value::string as tags
from RAW_FLIGHT_EVENTS,
lateral flatten(input => src:tags) as f;

select * from FLIGHT_EVENTS;


----------------------------------------------
-- PIPE
----------------------------------------------

CREATE OR REPLACE PIPE CAP_AIRCRAFT_MASTER_PIPE  --1
AUTO_INGEST = TRUE
AS
COPY INTO RAW.AIRCRAFT_MASTER
FROM @AIRLINE_CSV/ORA_AIRCRAFT_MASTER.csv
FILE_FORMAT = CSV_FF;

CREATE OR REPLACE PIPE CAP_FLIGHT_SCHEDULE_PIPE  -- 2
AUTO_INGEST = TRUE
AS
COPY INTO RAW.FLIGHTS_SCHEDULED
FROM @AIRLINE_CSV/ORA_FLIGHTS_SCHEDULED.csv
FILE_FORMAT = CSV_FF;

CREATE OR REPLACE PIPE CAP_CREW_ASSIGNMENTS_PIPE  -- 3
AUTO_INGEST = TRUE
AS
COPY INTO RAW.CREW_ASSIGNMENTS
FROM @AIRLINE_CSV/ORA_CREW_ASSIGNMENTS.csv
FILE_FORMAT = CSV_FF;

CREATE OR REPLACE PIPE CAP_TURNAROUND_PIPE  -- 4
AUTO_INGEST = TRUE
AS
COPY INTO RAW.AIRPORT_TURNAROUND_LOGS
FROM @AIRLINE_CSV/AIRPORT_TURNAROUND_LOGS.csv
FILE_FORMAT = CSV_FF;

CREATE OR REPLACE PIPE CAP_GATE_ALLOCATION_PIPE  -- 5
AUTO_INGEST = TRUE
AS
COPY INTO RAW.GATE_ALLOCATION
FROM @AIRLINE_CSV/GATE_ALLOCATION.csv
FILE_FORMAT = CSV_FF;

CREATE OR REPLACE PIPE CAP_MAINTAINENCE_PIPE  -- 6
AUTO_INGEST = TRUE
AS
COPY INTO RAW.MAINTENANCE_CLEARANCE
FROM @AIRLINE_CSV/MAINTENANCE_CLEARANCE.csv
FILE_FORMAT = CSV_FF;

SHOW PIPES;

---------------------------------------------
-- STAGING TABLES VALID
---------------------------------------------

CREATE OR REPLACE TRANSIENT DYNAMIC TABLE STG.STG_AIRCRAFT_MASTER
TARGET_LAG = '5 MINUTE'
WAREHOUSE = COMPUTE_WH
REFRESH_MODE = INCREMENTAL
AS
SELECT
    TRIM(UPPER(aircraft_id)) AS aircraft_id,
    TRIM(tail_number) AS tail_number,
    TRIM(aircraft_type) AS aircraft_type,
    TRIM(fleet_group) AS fleet_group,
    seating_capacity,
    TRIM(UPPER(maintenance_status)) AS maintenance_status,
    TRIM(UPPER(base_airport)) AS base_airport,
    manufacture_year
FROM RAW.AIRCRAFT_MASTER
WHERE aircraft_id IS NOT NULL
      AND maintenance_status IS NOT NULL
      AND manufacture_year < YEAR(CURRENT_DATE);

CREATE OR REPLACE TRANSIENT DYNAMIC TABLE STG.STG_FLIGHTS_SCHEDULED
TARGET_LAG = '5 MINUTE'
WAREHOUSE = COMPUTE_WH
REFRESH_MODE = FULL
AS
SELECT
    TRIM(UPPER(flight_id)) AS flight_id,
    TRIM(flight_number) AS flight_number,
    TRIM(UPPER(origin_airport)) AS origin_airport,
    TRIM(UPPER(destination_airport)) AS destination_airport,
    TRIM(route_code) AS route_code,
    TRIM(UPPER(aircraft_id)) AS aircraft_id,
    scheduled_departure_ts,
    scheduled_arrival_ts,
    TRIM(UPPER(service_type)) AS service_type,
    TRIM(UPPER(scheduled_status)) AS scheduled_status
FROM RAW.FLIGHTS_SCHEDULED
WHERE flight_id IS NOT NULL
  AND aircraft_id IS NOT NULL
  AND scheduled_departure_ts < scheduled_arrival_ts
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY flight_id
    ORDER BY scheduled_departure_ts DESC
) = 1;


CREATE OR REPLACE TRANSIENT DYNAMIC TABLE STG.STG_CREW_ASSIGNMENTS
TARGET_LAG = '5 MINUTE'
WAREHOUSE = COMPUTE_WH
REFRESH_MODE = INCREMENTAL
AS
SELECT
    TRIM(UPPER(assignment_id)) AS assignment_id,
    TRIM(UPPER(flight_id)) AS flight_id,
    TRIM(UPPER(crew_id)) AS crew_id,
    TRIM(UPPER(crew_role)) AS crew_role,
    TRIM(UPPER(base_airport)) AS base_airport,
    TRIM(certification_type) AS certification_type,
    duty_start_ts,
    duty_end_ts,
    TRIM(UPPER(assignment_status)) AS assignment_status
FROM RAW.CREW_ASSIGNMENTS
WHERE assignment_id IS NOT NULL
  AND flight_id IS NOT NULL
  AND crew_role IS NOT NULL
  AND duty_start_ts < duty_end_ts;

CREATE OR REPLACE TRANSIENT DYNAMIC TABLE STG.STG_GATE_ALLOCATION
TARGET_LAG = '5 MINUTE'
WAREHOUSE = COMPUTE_WH
REFRESH_MODE = INCREMENTAL
AS
SELECT
    gate_allocation_id,
    flight_id,
    UPPER(airport_code) AS airport_code,
    gate_number,
    terminal,
    UPPER(allocation_status) AS allocation_status,
    allocation_ts
FROM RAW.GATE_ALLOCATION
WHERE gate_allocation_id IS NOT NULL
  AND flight_id IS NOT NULL
  AND SUBSTR(flight_id,1,2) = 'FL'
  AND allocation_status IN ('ALLOCATED','CHANGED');

CREATE OR REPLACE TRANSIENT DYNAMIC TABLE STG.STG_AIRPORT_TURNAROUND_LOGS
TARGET_LAG = '5 MINUTE'
WAREHOUSE = COMPUTE_WH
REFRESH_MODE = INCREMENTAL
AS
SELECT
    turnaround_id,
    flight_id,
    UPPER(airport_code) AS airport_code,
    turnaround_start_ts,
    turnaround_end_ts,
    UPPER(fueling_complete_flag) AS fueling_complete_flag,
    UPPER(catering_complete_flag) AS catering_complete_flag,
    UPPER(cleaning_complete_flag) AS cleaning_complete_flag,
    UPPER(baggage_load_complete_flag) AS baggage_load_complete_flag,
    UPPER(readiness_status) AS readiness_status
FROM RAW.AIRPORT_TURNAROUND_LOGS
WHERE turnaround_id IS NOT NULL
  AND flight_id IS NOT NULL
  AND turnaround_start_ts < turnaround_end_ts
  AND readiness_status IN ('READY','AT_RISK');

CREATE OR REPLACE TRANSIENT DYNAMIC TABLE STG.STG_MAINTENANCE_CLEARANCE
TARGET_LAG = '5 MINUTE'
WAREHOUSE = COMPUTE_WH
REFRESH_MODE = INCREMENTAL
AS
SELECT
    maintenance_event_id,
    aircraft_id,
    clearance_ts,
    UPPER(status) AS status,
    UPPER(issue_category) AS issue_category,
    release_flag,
    UPPER(station_code) AS station_code
FROM RAW.MAINTENANCE_CLEARANCE
WHERE maintenance_event_id IS NOT NULL
  AND aircraft_id IS NOT NULL;

CREATE OR REPLACE TRANSIENT DYNAMIC TABLE STG.STG_FLIGHT_EVENTS
TARGET_LAG = '5 MINUTE'
WAREHOUSE = COMPUTE_WH
REFRESH_MODE = INCREMENTAL
AS
SELECT
    event_id,
    flight_id,
    event_ts,
    UPPER(event_type) AS event_type,
    UPPER(status_code) AS status_code,
    UPPER(delay_reason) AS delay_reason,
    delay_minutes,
    airport_code,
    gate,
    terminal,
    source_system,
    tags
FROM RAW.FLIGHT_EVENTS
WHERE event_id IS NOT NULL
  AND flight_id IS NOT NULL
  AND status_code != 'UNKNOWN_STATUS';


CREATE OR REPLACE TRANSIENT DYNAMIC TABLE STG.STG_DISRUPTION_ALERTS
TARGET_LAG = '5 MINUTE'
WAREHOUSE = COMPUTE_WH
REFRESH_MODE = INCREMENTAL
AS
SELECT
    alert_id,
    flight_id,
    alert_ts,
    UPPER(alert_type) AS alert_type,
    UPPER(severity) AS severity,
    impacted_airport,
    sla_threshold,
    delay_estimate,
    recommendation
FROM RAW.DISRUPTION_ALERTS
WHERE alert_id IS NOT NULL
  AND flight_id IS NOT NULL
  AND impacted_airport IS NOT NULL;

---------------------------------------------
-- STAGING VIEWS NOT-VALID
---------------------------------------------

CREATE OR REPLACE VIEW STG.VW_REJ_AIRCRAFT_MASTER AS
SELECT *
FROM RAW.AIRCRAFT_MASTER
WHERE aircraft_id IS NULL
      OR maintenance_status IS NULL
      OR manufacture_year >= YEAR(CURRENT_DATE);

CREATE OR REPLACE VIEW STG.VW_REJ_FLIGHTS_SCHEDULED AS
SELECT *
FROM RAW.FLIGHTS_SCHEDULED
QUALIFY 
    ROW_NUMBER() OVER (
        PARTITION BY flight_id
        ORDER BY scheduled_departure_ts DESC
    ) != 1
    OR flight_id IS NULL
    OR aircraft_id IS NULL
    OR scheduled_departure_ts >= scheduled_arrival_ts;

    
CREATE OR REPLACE VIEW STG.VW_REJ_CREW_ASSIGNMENTS AS
SELECT *
FROM RAW.CREW_ASSIGNMENTS
WHERE assignment_id IS NULL
   OR flight_id IS NULL
   OR crew_role IS NULL
   OR duty_start_ts >= duty_end_ts;

CREATE OR REPLACE VIEW STG.VW_REJ_GATE_ALLOCATION AS
SELECT *
FROM RAW.GATE_ALLOCATION
WHERE gate_allocation_id IS NULL
   OR flight_id IS NULL
   OR SUBSTR(flight_id,1,2) != 'FL'
   OR allocation_status NOT IN ('ALLOCATED','CHANGED');

CREATE OR REPLACE VIEW STG.VW_REJ_AIRPORT_TURNAROUND_LOGS AS
SELECT *
FROM RAW.AIRPORT_TURNAROUND_LOGS
WHERE turnaround_id IS NULL
   OR flight_id IS NULL
   OR turnaround_start_ts >= turnaround_end_ts
   OR readiness_status NOT IN ('READY','AT_RISK');

CREATE OR REPLACE VIEW STG.VW_REJ_MAINTENANCE_CLEARANCE AS
SELECT *
FROM RAW.MAINTENANCE_CLEARANCE
WHERE maintenance_event_id IS NULL
   OR aircraft_id IS NULL;

CREATE OR REPLACE VIEW STG.VW_REJ_FLIGHT_EVENTS AS
SELECT *
FROM RAW.FLIGHT_EVENTS
WHERE event_id IS NULL
   OR flight_id IS NULL
   OR status_code = 'UNKNOWN_STATUS';

CREATE OR REPLACE VIEW STG.VW_REJ_DISRUPTION_ALERTS AS
SELECT *
FROM RAW.DISRUPTION_ALERTS
WHERE alert_id IS NULL
   OR flight_id IS NULL
   OR impacted_airport IS NULL;
   
SELECT * FROM STG.VW_REJ_AIRCRAFT_MASTER;
SELECT * FROM STG.VW_REJ_AIRPORT_TURNAROUND_LOGS;

SELECT * FROM RAW.AIRPORT_TURNAROUND_LOGS;


--------------------------------------------------
-- AUDIT TABLE
--------------------------------------------------

CREATE OR REPLACE TABLE STG.AUDIT_FLIGHTS_SCHEDULED (
    table_name STRING,
    load_ts TIMESTAMP,
    total_records NUMBER,
    valid_records NUMBER,
    invalid_records NUMBER
);

CREATE OR REPLACE TASK STG.AUDIT_FLIGHTS_TASK
WAREHOUSE = COMPUTE_WH
SCHEDULE = '5 MINUTE'
AS
INSERT INTO STG.AUDIT_FLIGHTS_SCHEDULED
SELECT
    'FLIGHTS_SCHEDULED',
    CURRENT_TIMESTAMP,
    (SELECT COUNT(*) FROM RAW.FLIGHTS_SCHEDULED),
    (SELECT COUNT(*) FROM STG.STG_FLIGHTS_SCHEDULED),
    (SELECT COUNT(*) FROM STG.VW_REJ_FLIGHTS_SCHEDULED);

ALTER TASK STG.AUDIT_FLIGHTS_TASK RESUME;


--------------------------------------------------
-- CURATED
--------------------------------------------------
CREATE OR REPLACE SEQUENCE CURATED.SEQ_FLIGHT START = 1;
CREATE OR REPLACE SEQUENCE CURATED.SEQ_AIRCRAFT START = 1;
CREATE OR REPLACE SEQUENCE CURATED.SEQ_CREW START = 1;
CREATE OR REPLACE SEQUENCE CURATED.SEQ_DATE START = 1;

CREATE OR REPLACE TABLE CURATED.DIM_FLIGHT AS
SELECT
    CURATED.SEQ_FLIGHT.NEXTVAL AS flight_key,
    flight_id,
    flight_number,
    origin_airport,
    destination_airport,
    route_code
FROM STG.STG_FLIGHTS_SCHEDULED;

CREATE OR REPLACE TABLE CURATED.DIM_AIRCRAFT AS
SELECT
    CURATED.SEQ_AIRCRAFT.NEXTVAL AS aircraft_key,
    aircraft_id,
    aircraft_type,
    fleet_group,
    seating_capacity,
    base_airport
FROM STG.STG_AIRCRAFT_MASTER;

CREATE OR REPLACE TABLE CURATED.DIM_CREW AS
SELECT
    CURATED.SEQ_CREW.NEXTVAL AS crew_key,
    crew_id,
    crew_role,
    base_airport,
    certification_type
FROM STG.STG_CREW_ASSIGNMENTS;

CREATE OR REPLACE TABLE CURATED.DIM_AIRPORT AS
SELECT DISTINCT airport_code
FROM (
    SELECT origin_airport AS airport_code FROM STG.STG_FLIGHTS_SCHEDULED
    UNION
    SELECT destination_airport FROM STG.STG_FLIGHTS_SCHEDULED
    UNION
    SELECT airport_code FROM STG.STG_GATE_ALLOCATION
    UNION
    SELECT airport_code FROM STG.STG_AIRPORT_TURNAROUND_LOGS
);

CREATE OR REPLACE TABLE CURATED.DIM_DATE AS
SELECT
    CURATED.SEQ_DATE.NEXTVAL AS date_key,
    DATEADD(DAY, SEQ4(), '2024-01-01') AS flight_date,
    YEAR(flight_date) AS year,
    MONTH(flight_date) AS month,
    DAY(flight_date) AS day
FROM TABLE(GENERATOR(ROWCOUNT => 1000));

-------------------------
-- FACT TABLES
-------------------------

CREATE OR REPLACE TABLE CURATED.FACT_FLIGHT_OPERATION (
    flight_key NUMBER,
    aircraft_key NUMBER,
    date_key NUMBER,
    scheduled_departure_ts TIMESTAMP,
    scheduled_arrival_ts TIMESTAMP
);

INSERT INTO CURATED.FACT_FLIGHT_OPERATION
SELECT
    df.flight_key,
    da.aircraft_key,
    dd.date_key,
    s.scheduled_departure_ts,
    s.scheduled_arrival_ts
FROM STG.STG_FLIGHTS_SCHEDULED s
JOIN CURATED.DIM_FLIGHT df 
    ON s.flight_id = df.flight_id
JOIN CURATED.DIM_AIRCRAFT da 
    ON s.aircraft_id = da.aircraft_id
JOIN CURATED.DIM_DATE dd 
    ON DATE(s.scheduled_departure_ts) = dd.flight_date;

CREATE OR REPLACE TABLE CURATED.FACT_CREW_ASSIGNMENT (
    flight_key NUMBER,
    crew_key NUMBER,
    duty_start_ts TIMESTAMP,
    duty_end_ts TIMESTAMP
    
);

INSERT INTO CURATED.FACT_CREW_ASSIGNMENT
SELECT
    df.flight_key,
    dc.crew_key,
    c.duty_start_ts,
    c.duty_end_ts
FROM STG.STG_CREW_ASSIGNMENTS c
JOIN CURATED.DIM_FLIGHT df 
    ON c.flight_id = df.flight_id
JOIN CURATED.DIM_CREW dc 
    ON c.crew_id = dc.crew_id;

CREATE OR REPLACE TABLE CURATED.FACT_DISRUPTION_EVENT (
    flight_key NUMBER,
    event_id STRING,
    event_ts TIMESTAMP,
    event_type STRING,
    delay_minutes NUMBER
);

INSERT INTO CURATED.FACT_DISRUPTION_EVENT
SELECT
    df.flight_key,
    e.event_id,
    e.event_ts,
    e.event_type,
    COALESCE(e.delay_minutes,0)
FROM STG.STG_FLIGHT_EVENTS e
JOIN CURATED.DIM_FLIGHT df 
    ON e.flight_id = df.flight_id;

CREATE OR REPLACE TABLE CURATED.FACT_MAINTENANCE_CLEARANCE (
    aircraft_key NUMBER,
    clearance_ts TIMESTAMP,
    status STRING
);

INSERT INTO CURATED.FACT_MAINTENANCE_CLEARANCE
SELECT
    da.aircraft_key,
    m.clearance_ts,
    m.status
FROM STG.STG_MAINTENANCE_CLEARANCE m
JOIN CURATED.DIM_AIRCRAFT da 
    ON m.aircraft_id = da.aircraft_id;

-- GRAIN CHECK

SELECT flight_key, COUNT(*)
FROM CURATED.FACT_FLIGHT_OPERATION
GROUP BY flight_key
HAVING COUNT(*) > 1;

-- ORPHAN CHECK

SELECT e.*
FROM CURATED.FACT_DISRUPTION_EVENT e
LEFT JOIN CURATED.FACT_FLIGHT_OPERATION f
ON e.flight_key = f.flight_key
WHERE f.flight_key IS NULL;

SELECT COUNT(DISTINCT flight_key)
FROM CURATED.FACT_DISRUPTION_EVENT;

SELECT COUNT(*) FROM CURATED.FACT_DISRUPTION_EVENT;

SELECT COUNT(*) 
FROM CURATED.FACT_FLIGHT_OPERATION;

SELECT COUNT(DISTINCT flight_key)
FROM CURATED.FACT_DISRUPTION_EVENT;

-- ON_TIME_PERFORMANCE

SELECT
    COUNT(*) AS total_flights,
    SUM(CASE WHEN delay_minutes > 15 THEN 1 ELSE 0 END) AS delayed_flights,
    ROUND(
        100 * (COUNT(*) - SUM(CASE WHEN delay_minutes > 15 THEN 1 ELSE 0 END)) / COUNT(*),
        2
    ) AS on_time_percentage
FROM CURATED.FACT_DISRUPTION_EVENT;

-- CREW UTILIZATION

SELECT crew_key, COUNT(DISTINCT flight_key)
FROM CURATED.FACT_CREW_ASSIGNMENT
GROUP BY crew_key;

SELECT
    f.flight_key,
    t.readiness_status
FROM CURATED.FACT_FLIGHT_OPERATION f
JOIN CURATED.DIM_FLIGHT df ON f.flight_key = df.flight_key
JOIN STG.STG_AIRPORT_TURNAROUND_LOGS t ON df.flight_id = t.flight_id;


----------------------------------
-- KPI VIEWS
----------------------------------

-- ON_TIME_PERFORMANCE

CREATE OR REPLACE VIEW CURATED.KPI_ON_TIME_PERFORMANCE AS
SELECT
    COUNT(*) AS total_flights,
    SUM(CASE WHEN delay_minutes > 15 THEN 1 ELSE 0 END) AS delayed_flights,
    ROUND(
        100 * (COUNT(*) - SUM(CASE WHEN delay_minutes > 15 THEN 1 ELSE 0 END)) / COUNT(*),
        2
    ) AS on_time_percentage
FROM CURATED.FACT_DISRUPTION_EVENT;

SELECT * FROM CURATED.KPI_ON_TIME_PERFORMANCE;

-- AIRPORT_DELAY

CREATE OR REPLACE VIEW CURATED.KPI_AIRPORT_DELAY AS
SELECT
    df.origin_airport,
    COUNT(*) AS delayed_flights
FROM CURATED.FACT_DISRUPTION_EVENT e
JOIN CURATED.DIM_FLIGHT df
    ON e.flight_key = df.flight_key
WHERE delay_minutes > 15
GROUP BY df.origin_airport
ORDER BY delayed_flights DESC;

SELECT * FROM CURATED.KPI_AIRPORT_DELAY;

-- CREW UTILIZATION

CREATE OR REPLACE VIEW CURATED.KPI_CREW_UTILIZATION AS
SELECT
    dc.crew_id,
    COUNT(*) AS flights_handled
FROM CURATED.FACT_CREW_ASSIGNMENT f
JOIN CURATED.DIM_CREW dc
    ON f.crew_key = dc.crew_key
GROUP BY dc.crew_id
ORDER BY flights_handled DESC;

SELECT * FROM CURATED.KPI_CREW_UTILIZATION;

-- MAINTAINANCE

CREATE OR REPLACE VIEW CURATED.KPI_MAINTENANCE AS
SELECT
    da.aircraft_id,
    COUNT(*) AS total_events,
    SUM(CASE WHEN status = 'CLEARED' THEN 1 ELSE 0 END) AS cleared_events
FROM CURATED.FACT_MAINTENANCE_CLEARANCE f
JOIN CURATED.DIM_AIRCRAFT da
    ON f.aircraft_key = da.aircraft_key
GROUP BY da.aircraft_id;

SELECT * FROM CURATED.KPI_MAINTENANCE;

-- FLIGHT_TREND

CREATE OR REPLACE VIEW CURATED.KPI_FLIGHT_TREND AS
SELECT
    dd.flight_date,
    COUNT(*) AS total_flights
FROM CURATED.FACT_FLIGHT_OPERATION f
JOIN CURATED.DIM_DATE dd
    ON f.date_key = dd.date_key
GROUP BY dd.flight_date
ORDER BY dd.flight_date;

SELECT * FROM CURATED.KPI_FLIGHT_TREND;


--------------------------------------------------
-- RBAC — ROLE-BASED ACCESS CONTROL
--------------------------------------------------

USE ROLE SECURITYADMIN;

CREATE OR REPLACE ROLE DATA_ENGINEER_ROLE;
CREATE OR REPLACE ROLE READ_ONLY_ROLE;

GRANT ROLE DATA_ENGINEER_ROLE TO ROLE SYSADMIN;
GRANT ROLE READ_ONLY_ROLE     TO ROLE SYSADMIN;

USE ROLE SYSADMIN;

-- ========== DATA_ENGINEER_ROLE (DML on all schemas) ==========

GRANT USAGE ON DATABASE AIR             TO ROLE DATA_ENGINEER_ROLE;
GRANT USAGE ON SCHEMA   AIR.RAW         TO ROLE DATA_ENGINEER_ROLE;
GRANT USAGE ON SCHEMA   AIR.STG         TO ROLE DATA_ENGINEER_ROLE;
GRANT USAGE ON SCHEMA   AIR.CURATED     TO ROLE DATA_ENGINEER_ROLE;

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES    IN DATABASE AIR TO ROLE DATA_ENGINEER_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN DATABASE AIR TO ROLE DATA_ENGINEER_ROLE;

GRANT SELECT ON ALL VIEWS    IN DATABASE AIR TO ROLE DATA_ENGINEER_ROLE;
GRANT SELECT ON FUTURE VIEWS IN DATABASE AIR TO ROLE DATA_ENGINEER_ROLE;

GRANT CREATE TABLE ON SCHEMA AIR.RAW     TO ROLE DATA_ENGINEER_ROLE;
GRANT CREATE TABLE ON SCHEMA AIR.STG     TO ROLE DATA_ENGINEER_ROLE;
GRANT CREATE TABLE ON SCHEMA AIR.CURATED TO ROLE DATA_ENGINEER_ROLE;

GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE DATA_ENGINEER_ROLE;

-- ========== READ_ONLY_ROLE (SELECT on CURATED only) ==========

GRANT USAGE ON DATABASE AIR            TO ROLE READ_ONLY_ROLE;
GRANT USAGE ON SCHEMA   AIR.CURATED    TO ROLE READ_ONLY_ROLE;

GRANT SELECT ON ALL TABLES    IN SCHEMA AIR.CURATED TO ROLE READ_ONLY_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA AIR.CURATED TO ROLE READ_ONLY_ROLE;

GRANT SELECT ON ALL VIEWS    IN SCHEMA AIR.CURATED TO ROLE READ_ONLY_ROLE;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA AIR.CURATED TO ROLE READ_ONLY_ROLE;

GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE READ_ONLY_ROLE;

-- ========== ASSIGN TO USER ==========

USE ROLE SECURITYADMIN;

GRANT ROLE DATA_ENGINEER_ROLE TO USER PRANEE;
GRANT ROLE READ_ONLY_ROLE     TO USER PRANEE;

-- ========== VERIFY ==========

USE ROLE SYSADMIN;

SHOW GRANTS TO ROLE DATA_ENGINEER_ROLE;
SHOW GRANTS TO ROLE READ_ONLY_ROLE;


--------------------------------------------------
-- MASKING POLICY — CREW DATA
--------------------------------------------------

USE ROLE ACCOUNTADMIN;
USE DATABASE AIR;
USE SCHEMA CURATED;

CREATE OR REPLACE MASKING POLICY AIR.CURATED.MASK_CREW_VARCHAR AS
(val VARCHAR) RETURNS VARCHAR ->
    CASE
        WHEN CURRENT_ROLE() IN ('DATA_ENGINEER_ROLE', 'ACCOUNTADMIN', 'SYSADMIN') THEN val
        ELSE '***MASKED***'
    END;

ALTER TABLE AIR.CURATED.DIM_CREW MODIFY COLUMN CREW_ID
    SET MASKING POLICY AIR.CURATED.MASK_CREW_VARCHAR;

USE ROLE READ_ONLY_ROLE;
SELECT * FROM AIR.CURATED.DIM_CREW LIMIT 5;

USE ROLE Data_Engineer_role;
SELECT * FROM AIR.CURATED.DIM_CREW LIMIT 5;


--------------------------------------------------
-- CORTEX AI FUNCTIONS
--------------------------------------------------

USE ROLE ACCOUNTADMIN;
USE DATABASE AIR;
USE SCHEMA CURATED;

-- ========== 1. SUMMARIZE — Summarize disruption alerts from STG ==========

SELECT
    a.ALERT_ID,
    a.FLIGHT_ID,
    a.ALERT_TYPE,
    a.SEVERITY,
    a.DELAY_ESTIMATE,
    SNOWFLAKE.CORTEX.SUMMARIZE(
        CONCAT(
            'Alert ID: ', a.ALERT_ID, '. ',
            'Flight: ', f.FLIGHT_NUMBER, ' (', a.FLIGHT_ID, '). ',
            'Route: ', f.ORIGIN_AIRPORT, ' to ', f.DESTINATION_AIRPORT, '. ',
            'Type: ', a.ALERT_TYPE, ', Severity: ', a.SEVERITY, '. ',
            'Delay: ', a.DELAY_ESTIMATE, ' minutes. ',
            'Impacted Airport: ', COALESCE(a.IMPACTED_AIRPORT, 'UNKNOWN'), '. ',
            'Recommendation: ', COALESCE(a.RECOMMENDATION, 'NONE'), '.'
        )
    ) AS AI_SUMMARY
FROM AIR.STG.STG_DISRUPTION_ALERTS a
JOIN AIR.STG.STG_FLIGHTS_SCHEDULED f 
    ON a.FLIGHT_ID = f.FLIGHT_ID
LIMIT 5;


-- ========== 2. SENTIMENT — Analyze sentiment of flight delay reasons ==========

SELECT
    e.EVENT_ID,
    e.FLIGHT_ID,
    e.EVENT_TYPE,
    e.DELAY_REASON,
    e.DELAY_MINUTES,
    SNOWFLAKE.CORTEX.SENTIMENT(
        CONCAT(
            'Flight ', e.FLIGHT_ID, 
            ' delay reason: ', COALESCE(e.DELAY_REASON, 'unknown'), 
            '. Delay minutes: ', e.DELAY_MINUTES
        )
    ) AS SENTIMENT_SCORE
FROM AIR.STG.STG_FLIGHT_EVENTS e
WHERE e.DELAY_REASON IS NOT NULL
LIMIT 10;

-- ========== 3. TRANSLATE — Translate flight schedule details to French ==========

SELECT
    f.FLIGHT_ID,
    f.FLIGHT_NUMBER,
    f.ORIGIN_AIRPORT,
    f.DESTINATION_AIRPORT,
    SNOWFLAKE.CORTEX.TRANSLATE(
        CONCAT(
            'Flight ', f.FLIGHT_NUMBER,
            ' from ', f.ORIGIN_AIRPORT,
            ' to ', f.DESTINATION_AIRPORT,
            '. Departure: ', f.SCHEDULED_DEPARTURE_TS,
            ', Arrival: ', f.SCHEDULED_ARRIVAL_TS
        ),
        'en', 'fr'
    ) AS FRENCH_TRANSLATION
FROM AIR.STG.STG_FLIGHTS_SCHEDULED f
LIMIT 5;


-- ========== 4. EXTRACT_ANSWER — Q&A over maintenance clearance ==========

SELECT
    m.MAINTENANCE_EVENT_ID,
    m.AIRCRAFT_ID,
    m.STATUS,
    m.ISSUE_CATEGORY,
    SNOWFLAKE.CORTEX.EXTRACT_ANSWER(
        CONCAT(
            'Aircraft ', m.AIRCRAFT_ID,
            ' status: ', m.STATUS,
            '. Issue category: ', COALESCE(m.ISSUE_CATEGORY, 'UNKNOWN')
        ),
        'What is the issue category?'
    ) AS AI_ANSWER
FROM AIR.STG.STG_MAINTENANCE_CLEARANCE m
LIMIT 5;

--------------------------------------
-- PERFORMANCE OPTIMIZATION
--------------------------------------

ALTER WAREHOUSE COMPUTE_WH 
SET AUTO_SUSPEND = 60;

ALTER WAREHOUSE COMPUTE_WH 
SET AUTO_RESUME = TRUE;

ALTER TABLE CURATED.FACT_DISRUPTION_EVENT
CLUSTER BY (flight_key);

ALTER TABLE CURATED.FACT_FLIGHT_OPERATION
CLUSTER BY (date_key);