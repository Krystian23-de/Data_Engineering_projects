---Create employee_dim

CREATE TABLE EMPLOYEE_DIM (
  surrogate_employee_id   NUMBER PRIMARY KEY,
  employee_id             NUMBER NOT NULL,
  full_name               VARCHAR2(100),
  hire_date               DATE,
  job_id                  VARCHAR2(10),
  salary                  NUMBER(8,2),
  commission_pct          NUMBER(5,2),
  email                   VARCHAR2(100),
  phone_number            VARCHAR2(20),
  manager_id              NUMBER,
  department_id           NUMBER,
  tenure_band             VARCHAR2(20)
)

--- Insert into employee_dim

INSERT INTO EMPLOYEE_DIM (
  surrogate_employee_id,
  employee_id,
  full_name,
  hire_date,
  job_id,
  salary,
  commission_pct,
  email,
  phone_number,
  manager_id,
  department_id,
  tenure_band
)

SELECT
  ORA_HASH(
       NVL(TO_CHAR(employee_id),   '') || '|' ||
       NVL(first_name,             '') || '|' ||
       NVL(last_name,              '') || '|' ||
       TO_CHAR(hire_date,'YYYYMMDD')   || '|' ||
       NVL(job_id,                 '') || '|' ||
       NVL(TO_CHAR(salary,'FM9999990.00'), '') || '|' ||
       NVL(TO_CHAR(commission_pct,'FM90.00'), '') || '|' ||
       NVL(LOWER(email), 			'') || '|' ||
       NVL(REPLACE(phone_number,'.','-'), '') || '|' ||
       NVL(TO_CHAR(manager_id),    '') || '|' ||
       NVL(TO_CHAR(department_id), '')
  ) AS surrogate_employee_id,


 employee_id,
 
--- Full name concat
  first_name ||' '|| last_name AS full_name,
  hire_date,
  job_id,
  salary,
  commission_pct,
  
--- oracle email domain
  LOWER(email) || '@oracle.com' AS email,
  
--- Phone format change
  '+359' || REPLACE(phone_number, '.', '-') AS phone_number,
  manager_id,
  department_id,
CASE
  WHEN MONTHS_BETWEEN( hire_date, DATE '2009-12-31' ) < 12
    THEN 'Less than 1 year'
  WHEN MONTHS_BETWEEN( hire_date, DATE '2009-12-31' ) BETWEEN 12 AND 36
    THEN '1-3 years'
  WHEN MONTHS_BETWEEN( hire_date, DATE '2009-12-31' ) BETWEEN 37 AND 72
    THEN '4-6 years'
  WHEN MONTHS_BETWEEN( hire_date, DATE '2009-12-31' ) BETWEEN 73 AND 120
    THEN '7-10 years'
  ELSE '10+ years'
END AS tenure_band
FROM KW_Employees;


SELECT * FROM EMPLOYEE_DIM

--- JOB DIM table creation

CREATE TABLE JOB_DIM (
  surrogate_job_id  NUMBER       PRIMARY KEY,
  job_id            VARCHAR2(10) NOT NULL,
  job_title         VARCHAR2(35),
  min_salary        NUMBER(8,2),
  max_salary        NUMBER(8,2),
  job_category      VARCHAR2(30)
);


INSERT INTO JOB_DIM
SELECT

ORA_HASH(
  NVL(job_id,'')       || '|' ||
  NVL(TRIM(job_title),'')|| '|' ||
  NVL(TO_CHAR(min_salary,'FM9999990.00'), '') || '|' ||
  NVL(TO_CHAR(max_salary,'FM9999990.00'), '')
) as surrogate_job_id,

	job_id,
    job_title,
    min_salary,
    max_salary,

      CASE
    WHEN job_title IN (

        'Accounting Manager',
        'Purchasing Manager',
        'Sales Manager',
        'Stock Manager',
        'Administration Vice President',
        'Marketing Manager',
        'Finance Manager',
        'President'
    ) THEN 'Management'

      WHEN job_title IN (
      'Programmer',
      'Public Accountant',
      'Accountant',
      'Public Relations Representative',
      'Human Resources Representative',
      'Marketing Representative'
    ) THEN 'Technical/Professional'

    WHEN job_title IN (
      'Administration Assistant',
      'Purchasing Clerk',
      'Shipping Clerk',
      'Stock Clerk'
    ) THEN 'Clerical/Support'

    ELSE 'Other'
  END AS job_category

FROM KW_JOBS;

SELECT * FROM JOB_DIM;

--- Create TIME_DIM table

CREATE TABLE TIME_DIM (
    surrogate_time_id   RAW(16) PRIMARY KEY,  -- Surrogate key based on hash of the row
    time_id             NUMBER,  -- Natural key representing the date in a numeric format
    dates                DATE,  -- The specific calendar date
    year                NUMBER,  -- The year component of the date
    quarter             NUMBER,  -- The quarter of the year (1 through 4)
    month               NUMBER,  -- The month of the year (1 through 12)
    week                NUMBER,  -- The week number of the year
    day                 NUMBER,  -- The day of the month
    day_of_week         VARCHAR2(10),  -- The name of the day (e.g., 'Monday')
    fiscal_year         NUMBER,  -- The fiscal year corresponding to the date
    fiscal_quarter      NUMBER   -- The fiscal quarter corresponding to the date
);



DECLARE
    v_start_date DATE := TO_DATE('01-01-1995', 'dd-MM-yyyy');  -- Start date, adjusted to cover the earliest date in the data
    v_end_date   DATE := TO_DATE('31-12-2024', 'dd-MM-yyyy');  -- End date, extended slightly beyond the latest date in the data
BEGIN
    FOR d IN (SELECT v_start_date + LEVEL - 1 AS current_date
              FROM DUAL
              CONNECT BY LEVEL <= (v_end_date - v_start_date + 1))
    LOOP
        INSERT INTO TIME_DIM (
            surrogate_time_id,
            time_id,
            dates,
            year,
            quarter,
            month,
            week,
            day,
            day_of_week,
            fiscal_year,
            fiscal_quarter
        )
        VALUES (
            SYS_GUID(),  -- Generate a unique surrogate key
            TO_NUMBER(TO_CHAR(d.current_date, 'YYYYMMDD')),  -- Generate time_id in YYYYMMDD format
            d.current_date,  -- Date value
            TO_NUMBER(TO_CHAR(d.current_date, 'YYYY')),  -- Year value
            TO_NUMBER(TO_CHAR(d.current_date, 'Q')),  -- Quarter value
            TO_NUMBER(TO_CHAR(d.current_date, 'MM')),  -- Month value
            TO_NUMBER(TO_CHAR(d.current_date, 'IW')),  -- ISO week of year
            TO_NUMBER(TO_CHAR(d.current_date, 'DD')),  -- Day of the month
            TO_CHAR(d.current_date, 'Day', 'NLS_DATE_LANGUAGE=ENGLISH'),  -- Day of the week
            TO_NUMBER(TO_CHAR(d.current_date, 'YYYY')),  -- Fiscal year (assumed to align with calendar year)
            TO_NUMBER(TO_CHAR(d.current_date, 'Q'))  -- Fiscal quarter (assumed to align with calendar quarters)
        );
    END LOOP;

    COMMIT;
END;
/


CREATE TABLE DEPARTMENT_DIM (
  surrogate_department_id NUMBER(10) PRIMARY KEY,
  department_id           NUMBER(10) NOT NULL,
  department_name         VARCHAR2(30),
  location_id             NUMBER(10),
  manager_id              NUMBER(10)
);

--- Insert into TIME_DIM

INSERT INTO DEPARTMENT_DIM (
  surrogate_department_id,
  department_id,
  department_name,
  location_id,
  manager_id
)
SELECT
  ORA_HASH(
    NVL(TO_CHAR(department_id), '')   || '|' ||
    NVL(TRIM(department_name), '')    || '|' ||
    NVL(TO_CHAR(manager_id), '')      || '|' ||
    NVL(TO_CHAR(location_id), '')
  ) AS surrogate_department_id,
  department_id,
  department_name,
  location_id,
  manager_id
FROM KW_DEPARTMENTS;

INSERT INTO department_dim (
  surrogate_department_id,
  department_id,
  department_name,
  location_id,
  manager_id
) VALUES (
  0,
  0,
  'UNKNOWN',
  NULL,
  NULL
);

--- Create LOCATION_DIM table

CREATE TABLE location_dim 
(
  surrogate_location_id NUMBER(10) PRIMARY KEY,
  location_id           NUMBER(10) NOT NULL,
  street_address        VARCHAR2(100),
  postal_code           VARCHAR2(20),
  city                  VARCHAR2(50) NOT NULL,
  state_province        VARCHAR2(50),
  country_id            VARCHAR2(2)  NOT NULL,
  country_name          VARCHAR2(100),
  region_id             NUMBER(10),
  region_name           VARCHAR2(100)
  
  );

INSERT INTO location_dim (
  surrogate_location_id,
  location_id,
  street_address,
  postal_code,
  city,
  state_province,
  country_id,
  country_name,
  region_id,
  region_name
)
SELECT DISTINCT
  ORA_HASH(
    NVL(TO_CHAR(loc.location_id),   '') || '|' ||
    NVL(TRIM(loc.street_address),   '') || '|' ||
    NVL(TRIM(loc.postal_code),      '') || '|' ||
    NVL(TRIM(loc.city),             '') || '|' ||

    NVL(
      CASE 
        WHEN TRIM(loc.city) = 'London' THEN 'Greater London'
        ELSE TRIM(loc.state_province)
      END
    , '') || '|' ||
    NVL(
      CASE WHEN loc.country_id = 'UK' THEN 'GB'
           ELSE loc.country_id
      END
    , '')
  ) AS surrogate_location_id,

  loc.location_id,
  TRIM(loc.street_address)   AS street_address,
  TRIM(loc.postal_code)      AS postal_code,
  TRIM(loc.city)             AS city,
  
  INSERT INTO location_dim (
  surrogate_location_id,
  location_id,
  street_address,
  postal_code,
  city,
  state_province,
  country_id,
  country_name,
  region_id,
  region_name
) VALUES (
  0,             -- surrogate value for Unknown Location
  0,             -- dummy natural key
  'UNKNOWN',     -- street_address
  'UNKNOWN',     -- postal_code
  'UNKNOWN',     -- city
  'UNKNOWN',     -- state_province
  'ZZ',          -- 2 character placeholder for country_id
  'Unknown Country',  -- Unknown Country
  0,             -- region_id
  'Unknown Region'   -- Unknown Region

--- London override for state_province
  CASE 
    WHEN TRIM(loc.city) = 'London' THEN 'Greater London'
    ELSE TRIM(loc.state_province)
  END AS state_province,

--- map UK to GB 
  CASE WHEN loc.country_id = 'UK' THEN 'GB'
       ELSE loc.country_id
  END AS country_id,

  COALESCE(c.country_name, 'UNKNOWN') AS country_name,
  COALESCE(c.region_id,     0)         AS region_id,
  COALESCE(r.region_name,   'UNKNOWN') AS region_name

FROM KW_LOCATIONS loc
LEFT JOIN hr.countries c
  ON (CASE WHEN loc.country_id = 'UK' THEN 'GB' ELSE loc.country_id END) = c.country_id
LEFT JOIN hr.regions r
  ON c.region_id = r.region_id
JOIN department_dim dd
  ON loc.location_id = dd.location_id;

  SELECT * FROM LOCATION_DIM
  
--- Check for missing location_id between department_dim and location_dim
  
  
  SELECT DISTINCT location_id
FROM department_dim
UNION ALL
SELECT DISTINCT location_id
FROM location_dim
ORDER BY location_id;

--- Check distinct location_id from department_dim that is missing in location_dim
SELECT dd.location_id
FROM (
  SELECT DISTINCT location_id FROM department_dim
) dd
MINUS
SELECT location_id FROM location_dim;


--- Create Employee Salary Fact table

CREATE TABLE employee_salary_fact (
  surrogate_fact_id        NUMBER(10)     NOT NULL,   --- populated via ORA_HASH
  surrogate_employee_id    NUMBER(10)     NOT NULL,   --- FK for Employee_Dim
  surrogate_department_id  NUMBER(10)     NOT NULL,   --- FK for Department_Dim
  surrogate_job_id         NUMBER(10)     NOT NULL,   --- FK for Job_Dim
  surrogate_time_id        RAW(16)        NOT NULL,   --- FK for Time_Dim
  surrogate_location_id    NUMBER(10)     NOT NULL,   --- FK for Location_Dim

  salary                   NUMBER(15,2)   NOT NULL,
  bonus                    NUMBER(15,2)   NOT NULL,
  total_compensation       NUMBER(15,2)
    GENERATED ALWAYS AS (salary + bonus) VIRTUAL,

  effective_date           DATE           NOT NULL,

  CONSTRAINT pk_emp_salary_fact
    PRIMARY KEY (surrogate_fact_id),

  CONSTRAINT fk_esf_emp
    FOREIGN KEY (surrogate_employee_id)
      REFERENCES employee_dim(surrogate_employee_id),
  CONSTRAINT fk_esf_dept
    FOREIGN KEY (surrogate_department_id)
      REFERENCES department_dim(surrogate_department_id),
  CONSTRAINT fk_esf_job
    FOREIGN KEY (surrogate_job_id)
      REFERENCES job_dim(surrogate_job_id),
  CONSTRAINT fk_esf_time
    FOREIGN KEY (surrogate_time_id)
      REFERENCES time_dim(surrogate_time_id),
  CONSTRAINT fk_esf_loc
    FOREIGN KEY (surrogate_location_id)
      REFERENCES location_dim(surrogate_location_id)
);


--- INSERT INTO employee_salary_fact

INSERT INTO employee_salary_fact (
  surrogate_fact_id,
  surrogate_employee_id,
  surrogate_department_id,
  surrogate_job_id,
  surrogate_time_id,
  surrogate_location_id,
  salary,
  bonus,
  effective_date
)
SELECT
--- surrogate key based on dimension FK's
  ORA_HASH(
    TO_CHAR(ed.surrogate_employee_id)           || '|' ||
    TO_CHAR(NVL(dd.surrogate_department_id, 0)) || '|' ||
    TO_CHAR(jd.surrogate_job_id)                || '|' ||
    RAWTOHEX(td.surrogate_time_id)              || '|' ||
    TO_CHAR(NVL(ld.surrogate_location_id,  0))
  ) AS surrogate_fact_id,

--- Dimension FK's guaranteed non-NULL by NVL for department and location dims
  ed.surrogate_employee_id,
  NVL(dd.surrogate_department_id, 0) AS surrogate_department_id,
  jd.surrogate_job_id,
  td.surrogate_time_id,
  NVL(ld.surrogate_location_id,  0) AS surrogate_location_id,

--- Measures
  ke.salary,
  ke.salary * NVL(ke.commission_pct, 0) AS bonus,

--- Natural date for the Time_Dim join
  TRUNC(ke.hire_date) AS effective_date

FROM KW_EMPLOYEES ke

  LEFT JOIN employee_dim   ed ON ke.employee_id   = ed.employee_id
  LEFT JOIN department_dim dd ON ke.department_id = dd.department_id
  LEFT JOIN job_dim        jd ON ke.job_id        = jd.job_id
  LEFT JOIN time_dim       td ON td.dates         = TRUNC(ke.hire_date)
  LEFT JOIN location_dim   ld ON dd.location_id   = ld.location_id
;

---SCD2 columns for all dims

ALTER TABLE employee_dim
  ADD (
    effective_start_date DATE DEFAULT SYSDATE NOT NULL,
    effective_end_date   DATE,
    is_current           CHAR(1) DEFAULT 'Y' NOT NULL
  );

  ALTER TABLE department_dim
  ADD (
    effective_start_date DATE DEFAULT SYSDATE NOT NULL,
    effective_end_date   DATE,
    is_current           CHAR(1) DEFAULT 'Y' NOT NULL
  );

ALTER TABLE job_dim
  ADD (
    effective_start_date DATE DEFAULT SYSDATE NOT NULL,
    effective_end_date   DATE,
    is_current           CHAR(1) DEFAULT 'Y' NOT NULL
  );

  ALTER TABLE location_dim
  ADD (
    effective_start_date DATE DEFAULT SYSDATE NOT NULL,
    effective_end_date   DATE,
    is_current           CHAR(1) DEFAULT 'Y' NOT NULL
  );
  
--- Update records as current
  
  UPDATE employee_dim
   SET effective_start_date = NVL(hire_date, SYSDATE),
       effective_end_date   = NULL,
       is_current           = 'Y';

UPDATE department_dim
   SET effective_start_date = SYSDATE,
       effective_end_date   = NULL,
       is_current           = 'Y';

UPDATE job_dim
   SET effective_start_date = SYSDATE,
       effective_end_date   = NULL,
       is_current           = 'Y';

UPDATE location_dim
   SET effective_start_date = SYSDATE,
       effective_end_date   = NULL,
       is_current           = 'Y';


--- Index creation - business keys for SCD2

CREATE UNIQUE INDEX ux_employee_dim_bkey
  ON employee_dim(employee_id, effective_start_date);

CREATE UNIQUE INDEX ux_department_dim_bkey
  ON department_dim(department_id, effective_start_date);

CREATE UNIQUE INDEX ux_job_dim_bkey
  ON job_dim(job_id, effective_start_date);

CREATE UNIQUE INDEX ux_location_dim_bkey
  ON location_dim(location_id, effective_start_date);
  
--- Apply SCD2 updates

--- Update to simulate a salary increase for an employee
UPDATE kw_employees
SET salary = salary + 1000
WHERE employee_id = 101;

--- Update to simulate a department change for an employee
UPDATE kw_employees
SET department_id = 60
WHERE employee_id = 103;

--- Update to simulate a job change for an employee
UPDATE kw_employees
SET job_id = 'SA_REP'
WHERE employee_id = 110;

--- Update to standardize email format
UPDATE kw_employees
SET email = CONCAT(email, '@oracle_NEW.com')
WHERE employee_id = 104;


--- Update to simulate department name change
UPDATE kw_departments
SET department_name = 'Global Marketing'
WHERE department_id = 20;

--- Update to simulate department manager change
UPDATE kw_departments
SET manager_id = 102
WHERE department_id = 40;

--- Update to simulate department relocation
UPDATE kw_departments
SET location_id = 1900
WHERE department_id = 50;

--- Update to simulate adding a new manager to a department
UPDATE kw_departments
SET manager_id = 105
WHERE department_id = 60;


--- Update to simulate job title change
UPDATE kw_jobs
SET job_title = 'Senior Sales Representative'
WHERE job_id = 'SA_REP';

--- Update to simulate change in minimum salary for a job
UPDATE kw_jobs
SET min_salary = 5000
WHERE job_id = 'IT_PROG';

--- Update to simulate change in maximum salary for a job
UPDATE kw_jobs
SET max_salary = 30000
WHERE job_id = 'FI_ACCOUNT';

--- Update to simulate job title reclassification
UPDATE kw_jobs
SET job_title = 'Sales Vice President'
WHERE job_id = 'SA_MAN';




--- Update to simulate location city change
UPDATE kw_locations
SET city = 'New York'
WHERE location_id = 1700;

--- Update to simulate state or province change
UPDATE kw_locations
SET state_province = 'California'
WHERE location_id = 1500;

--- Update to simulate country change for a location
UPDATE kw_locations
SET country_id = 'UK'
WHERE location_id = 2400;

--- Update to simulate postal code standardization
UPDATE kw_locations
SET postal_code = '10001'
WHERE location_id = 1800;


UPDATE kw_employees
SET salary = salary * 1.05  -- Increase salary by 5%
WHERE hire_date <= TO_DATE('31-DEC-2008', 'DD-MON-YYYY');

UPDATE kw_employees
SET commission_pct = CASE
    WHEN job_id IN ('SA_MAN', 'SA_REP') THEN 0.10  -- 10% commission
    ELSE NULL
END
WHERE hire_date <= TO_DATE('31-DEC-2005', 'DD-MON-YYYY');


--- Example: Employee 101 moved from department 10 to 20 in 2010
INSERT INTO kw_job_history (employee_id, start_date, end_date, job_id, department_id)
VALUES (1001, TO_DATE('01-JAN-2005', 'DD-MON-YYYY'), TO_DATE('31-DEC-2009', 'DD-MON-YYYY'), 'IT_PROG', 10);

--- Update kw_employees to reflect current department after the change
UPDATE kw_employees
SET department_id = 20
WHERE employee_id = 101;

--- more updates
INSERT INTO kw_job_history (employee_id, start_date, end_date, job_id, department_id)
VALUES (1002, TO_DATE('01-JAN-2008', 'DD-MON-YYYY'), TO_DATE('31-DEC-2011', 'DD-MON-YYYY'), 'MK_REP', 20);

UPDATE kw_employees
SET department_id = 30
WHERE employee_id = 102;


UPDATE kw_employees
SET salary = salary + 15000
WHERE employee_id IN (100, 101, 110, 111, 112);

UPDATE kw_employees
SET salary = salary + 12000
WHERE employee_id IN (102, 113, 114, 115, 116);

UPDATE kw_employees
SET salary = salary + 10000
WHERE employee_id IN (103, 117, 118, 119, 120);



INSERT INTO kw_employees (
  employee_id, first_name, last_name, email,
  phone_number, hire_date, job_id, salary,
  commission_pct, manager_id, department_id
)
VALUES (
  1100, 'Alice', 'Johnson', 'AJOHNSON',
  '515.123.4567', TO_DATE('15-FEB-2015','DD-MON-YYYY'),
  'FI_ACCOUNT', 75000, NULL, 100, 10
);

INSERT INTO kw_employees (
  employee_id, first_name, last_name, email,
  phone_number, hire_date, job_id, salary,
  commission_pct, manager_id, department_id
)
VALUES (
  1110, 'Bob', 'Lee', 'BLEE',
  '515.123.4568', TO_DATE('20-MAR-2016','DD-MON-YYYY'),
  'FI_ACCOUNT', 72000, NULL, 100, 10
);

INSERT INTO kw_employees (
  employee_id, first_name, last_name, email,
  phone_number, hire_date, job_id, salary,
  commission_pct, manager_id, department_id
)
VALUES (
  1120, 'Carol', 'King', 'CKING',
  '515.123.4569', TO_DATE('10-APR-2017','DD-MON-YYYY'),
  'FI_ACCOUNT', 70000, NULL, 100, 10
);

--- Similar inserts for Departments 20 and 30
--- Department 20 new employees
INSERT INTO kw_employees (
  employee_id, first_name, last_name, email,
  phone_number, hire_date, job_id, salary,
  commission_pct, manager_id, department_id
)
VALUES (
  1130, 'David', 'Brown', 'DBROWN',
  '515.123.4570', TO_DATE('05-MAY-2015','DD-MON-YYYY'),
  'SA_REP', 68000, NULL, 102, 20
);
    

COMMIT;  
--- SCD2 type fields ingestion

--- employee_dim expire records

MERGE INTO employee_dim tgt
USING (
  SELECT
    ke.employee_id                        AS new_employee_id,
    ke.first_name ||' '|| ke.last_name AS new_full_name,
    ke.job_id                             AS new_job_id,
    ke.salary                             AS new_salary,
    ke.commission_pct                     AS new_commission_pct,
    LOWER(ke.email) || '@oracle.com'      AS new_email,
    '+359' || REPLACE(ke.phone_number, '.', '-') AS new_phone_number,
    ke.manager_id                         AS new_manager_id,
    ke.department_id                      AS new_department_id,
    SYSDATE                               AS new_effective_start_date,
    CASE
      WHEN MONTHS_BETWEEN(ke.hire_date, DATE '2009-12-31') < 12 THEN 'Less than 1 year'
      WHEN MONTHS_BETWEEN(ke.hire_date, DATE '2009-12-31') BETWEEN 12 AND 36 THEN '1-3 years'
      WHEN MONTHS_BETWEEN(ke.hire_date, DATE '2009-12-31') BETWEEN 37 AND 72 THEN '4-6 years'
      WHEN MONTHS_BETWEEN(ke.hire_date, DATE '2009-12-31') BETWEEN 73 AND 120 THEN '7-10 years'
      ELSE '10+ years'
    END AS new_tenure_band
  FROM kw_employees ke
) src
ON (tgt.employee_id = src.new_employee_id
    AND tgt.is_current = 'Y')
WHEN MATCHED AND (
       tgt.full_name           <> src.new_full_name
    OR tgt.job_id              <> src.new_job_id
    OR tgt.salary              <> src.new_salary
    OR NVL(tgt.commission_pct,0) <> NVL(src.new_commission_pct,0)
    OR NVL(tgt.email, '')      <> NVL(src.new_email, '')
    OR NVL(tgt.phone_number,'')<> NVL(src.new_phone_number,'')
    OR NVL(tgt.manager_id,0)   <> NVL(src.new_manager_id,0)
    OR NVL(tgt.department_id,0)<> NVL(src.new_department_id,0)
    OR NVL(tgt.tenure_band,'') <> NVL(src.new_tenure_band,'')
) THEN
  UPDATE SET
    tgt.effective_end_date = src.new_effective_start_date - INTERVAL '1' DAY,
    tgt.is_current         = 'N';
COMMIT;
--- Employee_dim insert new records
MERGE INTO employee_dim tgt
USING (
  SELECT
    ke.employee_id                        AS new_employee_id,
    ke.first_name || ' ' || ke.last_name AS new_full_name,
    ke.job_id                             AS new_job_id,
    ke.salary                             AS new_salary,
    ke.commission_pct                     AS new_commission_pct,
    LOWER(ke.email) || '@oracle.com'      AS new_email,
    '+359' || REPLACE(ke.phone_number, '.', '-') AS new_phone_number,
    ke.manager_id                         AS new_manager_id,
    ke.department_id                      AS new_department_id,
    SYSDATE                               AS new_effective_start_date,
    CASE
      WHEN MONTHS_BETWEEN(ke.hire_date, DATE '2009-12-31') < 12 THEN 'Less than 1 year'
      WHEN MONTHS_BETWEEN(ke.hire_date, DATE '2009-12-31') BETWEEN 12 AND 36 THEN '1-3 years'
      WHEN MONTHS_BETWEEN(ke.hire_date, DATE '2009-12-31') BETWEEN 37 AND 72 THEN '4-6 years'
      WHEN MONTHS_BETWEEN(ke.hire_date, DATE '2009-12-31') BETWEEN 73 AND 120 THEN '7-10 years'
      ELSE '10+ years'
    END AS new_tenure_band
  FROM kw_employees ke
) src
ON (tgt.employee_id = src.new_employee_id
    AND tgt.is_current = 'Y')
WHEN NOT MATCHED THEN
  INSERT (
    surrogate_employee_id,
    employee_id,
    full_name,
    hire_date,
    job_id,
    salary,
    commission_pct,
    email,
    phone_number,
    manager_id,
    department_id,
    tenure_band,
    effective_start_date,
    effective_end_date,
    is_current
  ) VALUES (
    employee_dim_seq.NEXTVAL,
    src.new_employee_id,
    src.new_full_name,
    src.new_effective_start_date,
    src.new_job_id,
    src.new_salary,
    src.new_commission_pct,
    src.new_email,
    src.new_phone_number,
    src.new_manager_id,
    src.new_department_id,
    src.new_tenure_band,
    src.new_effective_start_date,
    NULL,
    'Y'
  );
  
COMMIT;
--- Department_dim expire records
MERGE INTO department_dim tgt
USING (
  SELECT
    kd.department_id    AS new_department_id,
    kd.department_name  AS new_department_name,
    kd.location_id      AS new_location_id,
    kd.manager_id       AS new_manager_id,
    SYSDATE             AS new_effective_start_date
  FROM kw_departments kd
) src
ON (tgt.department_id = src.new_department_id
    AND tgt.is_current = 'Y')
WHEN MATCHED AND (
       tgt.department_name <> src.new_department_name
    OR NVL(tgt.location_id,0) <> NVL(src.new_location_id,0)
    OR NVL(tgt.manager_id,0)  <> NVL(src.new_manager_id,0)
) THEN
  UPDATE SET
    tgt.effective_end_date = src.new_effective_start_date - INTERVAL '1' DAY,
    tgt.is_current         = 'N';
COMMIT;
--- Department_dim insert new records

MERGE INTO department_dim tgt
USING (
  SELECT
    kd.department_id    AS new_department_id,
    kd.department_name  AS new_department_name,
    kd.location_id      AS new_location_id,
    kd.manager_id       AS new_manager_id,
    SYSDATE             AS new_effective_start_date
  FROM kw_departments kd
) src
ON (tgt.department_id = src.new_department_id
    AND tgt.is_current = 'Y')
WHEN NOT MATCHED THEN
  INSERT (
    surrogate_department_id,
    department_id,
    department_name,
    location_id,
    manager_id,
    effective_start_date,
    effective_end_date,
    is_current
  ) VALUES (
    department_dim_seq.NEXTVAL,
    src.new_department_id,
    src.new_department_name,
    src.new_location_id,
    src.new_manager_id,
    src.new_effective_start_date,
    NULL,
    'Y'
  );
COMMIT;
--- job_dim expire records

MERGE INTO job_dim tgt
USING (
  SELECT
    kj.job_id           AS new_job_id,
    kj.job_title        AS new_job_title,
    kj.min_salary       AS new_min_salary,
    kj.max_salary       AS new_max_salary,
    CASE
      WHEN kj.job_title IN (
        'Accounting Manager','Purchasing Manager','Sales Manager','Stock Manager',
        'Administration Vice President','Marketing Manager','Finance Manager','President'
      ) THEN 'Management'
      WHEN kj.job_title IN (
        'Programmer','Public Accountant','Accountant','Public Relations Representative',
        'Human Resources Representative','Marketing Representative'
      ) THEN 'Technical/Professional'
      WHEN kj.job_title IN (
        'Administration Assistant','Purchasing Clerk','Shipping Clerk','Stock Clerk'
      ) THEN 'Clerical/Support'
      ELSE 'Other'
    END AS new_job_category,
    SYSDATE             AS new_effective_start_date
  FROM kw_jobs kj
) src
ON (tgt.job_id = src.new_job_id
    AND tgt.is_current = 'Y')
WHEN MATCHED AND (
       tgt.job_title           <> src.new_job_title
    OR NVL(tgt.min_salary,0)  <> NVL(src.new_min_salary,0)
    OR NVL(tgt.max_salary,0)  <> NVL(src.new_max_salary,0)
    OR NVL(tgt.job_category,'')<> NVL(src.new_job_category,'')
) THEN
  UPDATE SET
    tgt.effective_end_date = src.new_effective_start_date - INTERVAL '1' DAY,
    tgt.is_current         = 'N';
COMMIT;
--- Job_dim insert new records

MERGE INTO job_dim tgt
USING (
  SELECT
    kj.job_id           AS new_job_id,
    kj.job_title        AS new_job_title,
    kj.min_salary       AS new_min_salary,
    kj.max_salary       AS new_max_salary,
    CASE
      WHEN kj.job_title IN (
        'Accounting Manager','Purchasing Manager','Sales Manager','Stock Manager',
        'Administration Vice President','Marketing Manager','Finance Manager','President'
      ) THEN 'Management'
      WHEN kj.job_title IN (
        'Programmer','Public Accountant','Accountant','Public Relations Representative',
        'Human Resources Representative','Marketing Representative'
      ) THEN 'Technical/Professional'
      WHEN kj.job_title IN (
        'Administration Assistant','Purchasing Clerk','Shipping Clerk','Stock Clerk'
      ) THEN 'Clerical/Support'
      ELSE 'Other'
    END AS new_job_category,
    SYSDATE             AS new_effective_start_date
  FROM kw_jobs kj
) src
ON (tgt.job_id = src.new_job_id
    AND tgt.is_current = 'Y')
WHEN NOT MATCHED THEN
  INSERT (
    surrogate_job_id,
    job_id,
    job_title,
    min_salary,
    max_salary,
    job_category,
    effective_start_date,
    effective_end_date,
    is_current
  ) VALUES (
    job_dim_seq.NEXTVAL,
    src.new_job_id,
    src.new_job_title,
    src.new_min_salary,
    src.new_max_salary,
    src.new_job_category,
    src.new_effective_start_date,
    NULL,
    'Y'
  );
COMMIT;  
--- Location_dim expire records

MERGE INTO location_dim tgt
USING (
  SELECT
    kl.location_id     AS new_location_id,
    TRIM(kl.street_address) AS new_street_address,
    TRIM(kl.postal_code)    AS new_postal_code,
    TRIM(kl.city)           AS new_city,
    CASE 
      WHEN TRIM(kl.city) = 'London' THEN 'Greater London'
      ELSE TRIM(kl.state_province)
    END AS new_state_province,
    CASE WHEN kl.country_id = 'UK' THEN 'GB' ELSE kl.country_id END AS new_country_id,
    COALESCE(c.country_name, 'UNKNOWN') AS new_country_name,
    COALESCE(c.region_id, 0)            AS new_region_id,
    COALESCE(r.region_name, 'UNKNOWN')  AS new_region_name,
    SYSDATE                            AS new_effective_start_date
  FROM kw_locations kl
  LEFT JOIN hr.countries c
    ON (CASE WHEN kl.country_id = 'UK' THEN 'GB' ELSE kl.country_id END) = c.country_id
  LEFT JOIN hr.regions r
    ON c.region_id = r.region_id
) src
ON (tgt.location_id = src.new_location_id
    AND tgt.is_current = 'Y')
WHEN MATCHED AND (
       tgt.street_address     <> src.new_street_address
    OR NVL(tgt.postal_code,'')    <> NVL(src.new_postal_code,'')
    OR NVL(tgt.city,'')           <> NVL(src.new_city,'')
    OR NVL(tgt.state_province,'') <> NVL(src.new_state_province,'')
    OR NVL(tgt.country_id,'')     <> NVL(src.new_country_id,'')
    OR NVL(tgt.country_name,'')   <> NVL(src.new_country_name,'')
    OR NVL(tgt.region_id,0)       <> NVL(src.new_region_id,0)
    OR NVL(tgt.region_name,'')    <> NVL(src.new_region_name,'')
) THEN
  UPDATE SET
    tgt.effective_end_date = src.new_effective_start_date - INTERVAL '1' DAY,
    tgt.is_current         = 'N';
COMMIT;	
--- Location_dim insert new records

MERGE INTO location_dim tgt
USING (
  SELECT
    kl.location_id     AS new_location_id,
    TRIM(kl.street_address) AS new_street_address,
    TRIM(kl.postal_code)    AS new_postal_code,
    TRIM(kl.city)           AS new_city,
    CASE 
      WHEN TRIM(kl.city) = 'London' THEN 'Greater London'
      ELSE TRIM(kl.state_province)
    END AS new_state_province,
    CASE WHEN kl.country_id = 'UK' THEN 'GB' ELSE kl.country_id END AS new_country_id,
    COALESCE(c.country_name, 'UNKNOWN') AS new_country_name,
    COALESCE(c.region_id, 0)            AS new_region_id,
    COALESCE(r.region_name, 'UNKNOWN')  AS new_region_name,
    SYSDATE                            AS new_effective_start_date
  FROM kw_locations kl
  LEFT JOIN hr.countries c
    ON (CASE WHEN kl.country_id = 'UK' THEN 'GB' ELSE kl.country_id END) = c.country_id
  LEFT JOIN hr.regions r
    ON c.region_id = r.region_id
) src
ON (tgt.location_id = src.new_location_id
    AND tgt.is_current = 'Y')
WHEN NOT MATCHED THEN
  INSERT (
    surrogate_location_id,
    location_id,
    street_address,
    postal_code,
    city,
    state_province,
    country_id,
    country_name,
    region_id,
    region_name,
    effective_start_date,
    effective_end_date,
    is_current
  ) VALUES (
    location_dim_seq.NEXTVAL,
    src.new_location_id,
    src.new_street_address,
    src.new_postal_code,
    src.new_city,
    src.new_state_province,
    src.new_country_id,
    src.new_country_name,
    src.new_region_id,
    src.new_region_name,
    src.new_effective_start_date,
    NULL,
    'Y'
  );
 
COMMIT;
END;

--- SCD2 check queries for one row per NK

SELECT employee_id,
       COUNT(*) AS cnt_current
FROM employee_dim
WHERE is_current = 'Y'
GROUP BY employee_id
HAVING COUNT(*) <> 1;

--- Department
       COUNT(*) AS cnt_current
FROM department_dim
WHERE effective_end_date IS NULL
GROUP BY department_id
HAVING COUNT(*) <> 1;

--- Job
SELECT job_id,
       COUNT(*) AS cnt_current
FROM job_dim
WHERE effective_end_date IS NULL
GROUP BY job_id
HAVING COUNT(*) <> 1;

--- Location
SELECT location_id,
       COUNT(*) AS cnt_current
FROM location_dim
WHERE effective_end_date IS NULL
GROUP BY location_id
HAVING COUNT(*) <> 1;

--- check queries for SCD2 values ingestion

--- Ensure exactly one current record per business key in each dimension
--- Employee_Dim
SELECT 'Employee_Dim' AS dimension, employee_id, COUNT(*) AS current_count
FROM employee_dim
WHERE is_current = 'Y'
GROUP BY employee_id
HAVING COUNT(*) <> 1;

--- Department_Dim
SELECT 'Department_Dim' AS dimension, department_id, COUNT(*) AS current_count
FROM department_dim
WHERE is_current = 'Y'
GROUP BY department_id
HAVING COUNT(*) <> 1;

--- Job_Dim
SELECT 'Job_Dim' AS dimension, job_id, COUNT(*) AS current_count
FROM job_dim
WHERE is_current = 'Y'
GROUP BY job_id
HAVING COUNT(*) <> 1;

--- Location_Dim
SELECT 'Location_Dim' AS dimension, location_id, COUNT(*) AS current_count
FROM location_dim
WHERE is_current = 'Y'
GROUP BY location_id
HAVING COUNT(*) <> 1;


---Check query for is_current = 'N'

SELECT 'EMPLOYEE_DIM' AS dimension,
       COUNT(*) AS expired_count
FROM   employee_dim
WHERE  is_current = 'N'

UNION ALL

SELECT 'DEPARTMENT_DIM'  AS dimension,
       COUNT(*) AS expired_count
FROM   department_dim
WHERE  effective_end_date IS NOT NULL

UNION ALL

SELECT 'JOB_DIM' AS dimension,
       COUNT(*) AS expired_count
FROM   job_dim
WHERE  effective_end_date IS NOT NULL

UNION ALL

SELECT 'LOCATION_DIM' AS dimension,
       COUNT(*) AS expired_count
FROM   location_dim
WHERE  effective_end_date IS NOT NULL

ORDER BY dimension;




---SQL queries from Fact table

---Find the Total Compensation by Region
  
SELECT ld.region_name    AS Region, SUM(esf.total_compensation) AS total_compensation
FROM employee_salary_fact esf
JOIN location_dim ld
  ON esf.surrogate_location_id = ld.surrogate_location_id
GROUP BY ld.region_name
ORDER BY total_compensation DESC;


---Retrieve the total compensation (salary + bonus) for each employee for the latest year in the time_dim.

WITH
  latest_year AS (
    SELECT MAX(td.year) AS yr
    FROM   employee_salary_fact f
    JOIN   time_dim td
      ON f.surrogate_time_id = td.surrogate_time_id
  ),
  latest_times AS (
    SELECT td.surrogate_time_id
    FROM   time_dim td
    JOIN   latest_year ly
      ON td.year = ly.yr
  )
SELECT
  ed.employee_id,
  ed.full_name,
  SUM(esf.total_compensation) AS compensation_latest_year
FROM   employee_salary_fact esf
JOIN   latest_times lt
  ON esf.surrogate_time_id = lt.surrogate_time_id
JOIN   employee_dim ed
  ON esf.surrogate_employee_id = ed.surrogate_employee_id
GROUP BY
  ed.employee_id,
  ed.full_name
ORDER BY
  compensation_latest_year DESC;
  
---Calculate the average salary for each job category.

SELECT jd.job_category, ROUND(AVG(esf.salary)) as average_salary
FROM employee_salary_fact esf
JOIN job_dim jd
ON esf.surrogate_job_id = jd.surrogate_job_id
WHERE jd.is_current = 'Y'
GROUP BY jd.job_category
ORDER BY average_salary DESC

---List employees who have changed departments between 2005 and 2018

WITH relevant_history AS (
  SELECT
    jh.employee_id,
    jh.department_id
  FROM   kw_job_history jh
  WHERE
    (jh.start_date BETWEEN DATE '2005-01-01' AND DATE '2018-12-31')
    OR (jh.end_date   BETWEEN DATE '2005-01-01' AND DATE '2018-12-31')
    OR (jh.start_date <= DATE '2005-01-01'
        AND jh.end_date   >= DATE '2018-12-31')
),
change_counts AS (
  SELECT
    employee_id,
    COUNT(DISTINCT department_id) AS dept_count
  FROM   relevant_history
  GROUP BY
    employee_id
  HAVING
    COUNT(DISTINCT department_id) > 1
)
SELECT
  ed.employee_id,
  ed.full_name
FROM   change_counts cc
JOIN   employee_dim ed
  ON cc.employee_id = ed.employee_id
ORDER BY
  ed.employee_id;

SELECT employee_id
FROM   kw_job_history
MINUS
SELECT employee_id

---Identify the top 5 highest paid employees within each department.

WITH rank_per_dept AS(
    SELECT
    esf.surrogate_employee_id,
    esf.surrogate_department_id,
    esf.total_compensation,
    ROW_NUMBER() OVER (PARTITION BY esf.surrogate_department_id ORDER BY esf.total_compensation DESC
) as dept_rank
    FROM employee_salary_fact esf),

top5_per_dept  as (
        SELECT
      surrogate_employee_id,
      surrogate_department_id,
      total_compensation,
      dept_rank
    FROM
      rank_per_dept
    WHERE
      dept_rank <= 5
  )

SELECT
ed.employee_id,
ed.full_name,
dd.department_name,
tpd.total_compensation,
tpd.dept_rank
FROM
top5_per_dept tpd
JOIN employee_dim ed
ON tpd.surrogate_employee_id = ed.surrogate_employee_id
JOIN department_dim dd
ON tpd.surrogate_department_id = dd.surrogate_department_id
ORDER BY
  dd.department_name,
  tpd.dept_rank,
  tpd.total_compensation DESC