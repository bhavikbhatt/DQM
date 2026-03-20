-- Databricks notebook source
-- DBTITLE 1,Cell 1
-- Create Schema
USE CATALOG uhg_demos;
CREATE SCHEMA IF NOT EXISTS cdo_data_quality;
USE SCHEMA cdo_data_quality;

-- COMMAND ----------

-- DBTITLE 1,Cell 2
-- =============================================================
-- CREATE SAMPLE TABLE: MEMBER_INFO
-- =============================================================
-- Contains 5 key field types: FirstName, MiddleName, LastName, DOB, Email
-- Mix of conforming and non-conforming data for demo purposes

CREATE OR REPLACE TABLE MEMBER_INFO (
    member_id       VARCHAR(20),
    first_name      VARCHAR(50),
    middle_name     VARCHAR(50),
    last_name       VARCHAR(100),
    date_of_birth   DATE,
    email_address   VARCHAR(255),
    phone_number    VARCHAR(20),
    city            VARCHAR(100),
    state_code      VARCHAR(2),
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP
);

-- =============================================================
-- INSERT SAMPLE DATA
-- =============================================================

INSERT INTO MEMBER_INFO (member_id, first_name, middle_name, last_name, date_of_birth, email_address, phone_number, city, state_code) VALUES
    -- Good records (conforming data)
    ('M001', 'John', 'Robert', 'Smith', '1985-03-15', 'john.smith@email.com', '555-123-4567', 'Chicago', 'IL'),
    ('M002', 'Sarah', 'Jane', 'Johnson', '1990-07-22', 'sarah.johnson@company.org', '555-234-5678', 'New York', 'NY'),
    ('M003', 'Michael', 'David', 'Williams', '1978-11-08', 'mwilliams@healthcare.net', '555-345-6789', 'Los Angeles', 'CA'),
    ('M004', 'Emily', NULL, 'Brown', '1995-01-30', 'emily.brown@gmail.com', '555-456-7890', 'Houston', 'TX'),
    ('M005', 'James', 'Thomas', 'Davis', '1982-09-12', 'jdavis@outlook.com', '555-567-8901', 'Phoenix', 'AZ'),
    ('M006', 'Jennifer', 'Marie', 'Miller', '1988-04-25', 'jennifer.miller@yahoo.com', '555-678-9012', 'Philadelphia', 'PA'),
    ('M007', 'Robert', 'Lee', 'Wilson', '1975-12-03', 'rwilson@medical.com', '555-789-0123', 'San Antonio', 'TX'),
    ('M008', 'Lisa', 'Ann', 'Moore', '1992-06-18', 'lisa.moore@health.org', '555-890-1234', 'San Diego', 'CA'),
    ('M009', 'William', 'James', 'Taylor', '1980-02-28', 'wtaylor@provider.com', '555-901-2345', 'Dallas', 'TX'),
    ('M010', 'Amanda', 'Rose', 'Anderson', '1987-08-14', 'amanda.anderson@clinic.net', '555-012-3456', 'San Jose', 'CA'),

    -- Bad first names (numbers, special chars, empty)
    ('M011', 'J0hn123', 'Mark', 'Thomas', '1991-05-20', 'jthomas@email.com', '555-111-2222', 'Austin', 'TX'),
    ('M012', '!!!Bob', NULL, 'Jackson', '1983-10-10', 'bjackson@test.org', '555-222-3333', 'Boston', 'MA'),
    ('M013', '', 'Paul', 'White', '1979-03-05', 'pwhite@sample.com', '555-333-4444', 'Seattle', 'WA'),

    -- Bad last names
    ('M014', 'Kevin', 'Scott', '12345', '1986-07-15', 'kscott@email.com', '555-444-5555', 'Denver', 'CO'),
    ('M015', 'Nancy', NULL, '@#$%Harris', '1994-11-25', 'nharris@company.net', '555-555-6666', 'Atlanta', 'GA'),

    -- Bad emails
    ('M016', 'Patricia', 'Lynn', 'Martin', '1981-04-08', 'not-an-email', '555-666-7777', 'Miami', 'FL'),
    ('M017', 'Charles', 'Edward', 'Garcia', '1976-09-30', 'missing@domain', '555-777-8888', 'Portland', 'OR'),
    ('M018', 'Elizabeth', NULL, 'Martinez', '1989-12-12', '@invalid.com', '555-888-9999', 'Detroit', 'MI'),
    ('M019', 'Daniel', 'Ray', 'Robinson', '1993-02-14', '', '555-999-0000', 'Minneapolis', 'MN'),
    ('M020', 'Margaret', 'Sue', 'Clark', '1984-06-28', NULL, '555-000-1111', 'Cleveland', 'OH'),

    -- NULL records
    ('M021', NULL, NULL, 'Lewis', '1977-08-08', 'mlewis@email.com', '555-123-0000', 'Tampa', 'FL'),
    ('M022', 'Steven', NULL, NULL, '1990-01-01', 'stest@email.com', '555-234-0000', 'Pittsburgh', 'PA'),
    ('M023', 'Dorothy', 'Mae', 'Walker', NULL, 'dwalker@email.com', '555-345-0000', 'Cincinnati', 'OH'),
    ('M024', 'Anthony', 'Joseph', 'Hall', '1985-05-05', NULL, NULL, 'Orlando', 'FL'),
    ('M025', NULL, NULL, NULL, NULL, NULL, NULL, 'Unknown', 'XX');

-- COMMAND ----------

-- =============================================================
-- CREATE SECOND TABLE: EMPLOYEE_RECORD
-- =============================================================
-- Different column naming conventions to demonstrate schema-wide classification

CREATE OR REPLACE TABLE EMPLOYEE_RECORD (
    emp_id          VARCHAR(20) PRIMARY KEY,
    fname           VARCHAR(50),    -- Different name: fname vs first_name
    mname           VARCHAR(50),    -- Different name: mname vs middle_name
    lname           VARCHAR(100),   -- Different name: lname vs last_name
    birth_date      DATE,           -- Different name: birth_date vs date_of_birth
    email           VARCHAR(255),   -- Different name: email vs email_address
    mobile_phone    VARCHAR(20),    -- Different name: mobile_phone vs phone_number
    hire_date       DATE,
    department      VARCHAR(100),
    job_title       VARCHAR(100),
    created_ts      TIMESTAMP_NTZ,
    modified_ts     TIMESTAMP_NTZ
);

INSERT INTO EMPLOYEE_RECORD (emp_id, fname, mname, lname, birth_date, email, mobile_phone, hire_date, department, job_title) VALUES

    -- Good records
    ('E001', 'Alice', 'Marie', 'Thompson', '1988-05-12', 'alice.thompson@company.com', '555-100-1001', '2020-01-15', 'Engineering', 'Software Engineer'),
    ('E002', 'Brian', 'James', 'Mitchell', '1975-09-23', 'brian.mitchell@company.com', '555-100-1002', '2019-03-20', 'Finance', 'Financial Analyst'),
    ('E003', 'Carol', NULL, 'Peterson', '1992-11-30', 'carol.peterson@company.com', '555-100-1003', '2021-06-01', 'Marketing', 'Marketing Manager'),
    ('E004', 'David', 'Lee', 'Richardson', '1983-02-14', 'david.richardson@company.com', '555-100-1004', '2018-09-10', 'Engineering', 'Tech Lead'),
    ('E005', 'Emma', 'Rose', 'Sullivan', '1990-07-08', 'emma.sullivan@company.com', '555-100-1005', '2022-02-28', 'HR', 'HR Specialist'),

    -- Bad records for DQ testing
    ('E006', '123Invalid', NULL, 'Cooper', '1985-04-18', 'bad-email-format', '555-100-1006', '2017-11-05', 'Sales', 'Sales Rep'),
    ('E007', '', 'Paul', 'Barnes', '1979-08-25', 'missing@domain', '555-100-1007', '2016-04-12', 'Operations', 'Operations Lead'),
    ('E008', NULL, NULL, '!!!InvalidLast', '1991-12-03', NULL, '555-100-1008', '2023-01-09', 'Engineering', 'Junior Developer'),
    ('E009', 'Frank', 'William', NULL, NULL, 'frank@company.com', NULL, '2020-08-17', 'Finance', 'Accountant'),
    ('E010', NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'Unknown', 'Unknown');



-- COMMAND ----------

-- =============================================================
-- CREATE THIRD TABLE: CUSTOMER_CONTACT
-- =============================================================
-- Yet another naming convention

CREATE OR REPLACE TABLE CUSTOMER_CONTACT (
    customer_id     VARCHAR(20) PRIMARY KEY,
    given_name      VARCHAR(50),    -- Another variation: given_name
    middle_initial  VARCHAR(5),     -- Variation: middle_initial (shorter)
    family_name     VARCHAR(100),   -- Variation: family_name vs last_name
    dob             DATE,           -- Abbreviation: dob
    contact_email   VARCHAR(255),   -- Variation: contact_email
    primary_phone   VARCHAR(20),
    mailing_address VARCHAR(200),
    account_created TIMESTAMP_NTZ
);

INSERT INTO CUSTOMER_CONTACT (customer_id, given_name, middle_initial, family_name, dob, contact_email, primary_phone, mailing_address)
VALUES
    ('C001', 'George', 'H', 'Washington', '1965-03-10', 'gwashington@email.com', '555-200-2001', '100 Cherry Lane'),
    ('C002', 'Martha', 'A', 'Jefferson', '1972-06-22', 'mjefferson@email.com', '555-200-2002', '200 Liberty Ave'),
    ('C003', 'Thomas', NULL, 'Adams', '1980-01-05', 'tadams@email.com', '555-200-2003', '300 Freedom Blvd'),
    ('C004', 'Abigail', 'M', 'Madison', '1988-09-17', 'amadison@email.com', '555-200-2004', '400 Constitution Way'),
    ('C005', 'James', 'K', 'Monroe', '1995-12-28', 'jmonroe@email.com', '555-200-2005', '500 Independence Dr'),
    -- Bad records
    ('C006', 'Invalid123', NULL, 'Polk', '1978-04-15', 'not-valid-email', '555-200-2006', '600 Test St'),
    ('C007', NULL, NULL, NULL, NULL, NULL, NULL, NULL),
    ('C008', '', 'X', '@#$%BadName', '1982-07-30', '', '555-200-2008', '800 Bad Data Rd');

-- COMMAND ----------

-- =============================================================
-- VERIFY SETUP
-- =============================================================

SELECT 'MEMBER_INFO' AS table_name, COUNT(*) AS record_count FROM MEMBER_INFO
UNION ALL
SELECT 'EMPLOYEE_RECORD', COUNT(*) FROM EMPLOYEE_RECORD
UNION ALL
SELECT 'CUSTOMER_CONTACT', COUNT(*) FROM CUSTOMER_CONTACT;



-- COMMAND ----------

-- Preview tables
-- SELECT 'MEMBER_INFO' AS source, member_id AS id, first_name, last_name, email_address FROM MEMBER_INFO LIMIT 5;
-- SELECT 'EMPLOYEE_RECORD' AS source, emp_id AS id, fname, lname, email FROM EMPLOYEE_RECORD LIMIT 5;
-- SELECT 'CUSTOMER_CONTACT' AS source, customer_id AS id, given_name, family_name, contact_email FROM CUSTOMER_CONTACT LIMIT 5;