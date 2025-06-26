-- Task #1: Import data into the database 
-- Author: Alena Dutova
-- DB: PostgreSQL

-- ===================================
-- STEP 1: Preparation — drop existing objects if present
-- Ensures clean execution on repeated runs (e.g., during local testing or CI)
-- ===================================

DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS customer_balance;
DROP TABLE IF EXISTS customer_orders;
DROP TABLE IF EXISTS invalid_traffic_source;
DROP TABLE IF EXISTS invalid_operation_type;

DROP TABLE IF EXISTS balance;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS users;

DROP TYPE IF EXISTS traffic_source_enum;
DROP TYPE IF EXISTS operation_enum;

-- ===================================
-- STEP 2: Begin transactional execution
-- Ensures all statements succeed together (atomicity)
-- ===================================

BEGIN;

-- ===================================
-- STEP 3: Temporary staging tables for raw CSV import
-- These tables contain unvalidated data and will be cleaned before insertion
-- ===================================

CREATE TABLE customers (
	user_id INT, 
	country_code VARCHAR,
	registration_time TIMESTAMP,
	traffic_source VARCHAR
);

CREATE TABLE customer_balance (
	user_id INT, 
	operation_time TIMESTAMP,
	operation_type VARCHAR,
	operation_amount_usd FLOAT
);

CREATE TABLE customer_orders (
	user_id INT, 
	symbol VARCHAR,
	open_time TIMESTAMP,
	close_time TIMESTAMP,
	profit_usd FLOAT
);

-- ===================================
-- STEP 4: Create domain model tables and load cleaned data
-- ENUMs are used to ensure categorical data integrity
-- ===================================

-- Define allowed values for traffic source field
CREATE TYPE traffic_source_enum AS ENUM ('organic', 'google', 'referral');

-- Create cleaned users table with normalized and validated traffic_source
CREATE TABLE users (
    user_id INTEGER PRIMARY KEY,
    country_code VARCHAR,
    registration_time TIMESTAMP,
    traffic_source traffic_source_enum
);

-- Insert cleaned users, deduplicated by user_id (latest registration kept)
-- Normalize case and whitespace, enforce enum integrity via dynamic validation
WITH enum_values_traffic_source AS (
  SELECT enumlabel
  FROM pg_enum
  JOIN pg_type ON pg_enum.enumtypid = pg_type.oid
  WHERE typname = 'traffic_source_enum'
)

INSERT INTO users (user_id, country_code, registration_time, traffic_source)

SELECT DISTINCT ON (user_id)
    user_id,
    TRIM(country_code),
    registration_time,
    LOWER(TRIM(traffic_source))::traffic_source_enum
FROM customers

WHERE LOWER(TRIM(traffic_source)) IN (SELECT enumlabel FROM enum_values_traffic_source)

ORDER BY user_id, registration_time DESC;

-- Define allowed values for balance operation types
CREATE TYPE operation_enum AS ENUM ('debit', 'credit', 'withdrawal', 'deposit');

-- Create cleaned balance table with normalized enums and FK to users
CREATE TABLE balance (
    balance_id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(user_id) ON DELETE CASCADE,
    operation_time TIMESTAMP,
    operation_type operation_enum,
    operation_amount_usd FLOAT
);

-- Insert validated transactions only:
--   - Existing users only (foreign key)
--   - Allowed operation types
--   - Non-null timestamps
--   - Non-negative amounts
WITH enum_values_operation AS (
  SELECT enumlabel
  FROM pg_enum
  JOIN pg_type ON pg_enum.enumtypid = pg_type.oid
  WHERE typname = 'operation_enum'
)

INSERT INTO balance (user_id, operation_time, operation_type, operation_amount_usd)

SELECT
    user_id,
    operation_time,
    LOWER(TRIM(operation_type))::operation_enum,
    operation_amount_usd
FROM customer_balance

WHERE
    user_id IN (SELECT user_id FROM users) AND
    LOWER(TRIM(operation_type)) IN (SELECT enumlabel FROM enum_values_operation) AND
    operation_time IS NOT NULL AND
    operation_amount_usd >= 0;

-- ===================================
-- STEP 5: Create and load cleaned order data
-- Orders are inserted only if valid and linked to known users
-- `EXPLAIN ANALYZE` is used to demonstrate performance awareness
-- ===================================

CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(user_id) ON DELETE CASCADE,
    symbol VARCHAR,
    open_time TIMESTAMP,
    close_time TIMESTAMP,
    profit_usd FLOAT
);

-- Analyze performance of the insert on large datasets
-- Normalizes symbol values and filters invalid time spans
EXPLAIN ANALYZE
INSERT INTO orders (user_id, symbol, open_time, close_time, profit_usd)

SELECT
    user_id,
    UPPER(TRIM(symbol)),
    open_time,
    close_time,
    profit_usd
FROM customer_orders
WHERE
    user_id IN (SELECT user_id FROM users) AND
    open_time IS NOT NULL AND
    open_time <= close_time;

-- ===================================
-- STEP 6: Store rejected records for audit / manual QA
-- Helps trace issues in source data and supports robust validation
-- ===================================

CREATE TABLE invalid_traffic_source AS
SELECT * FROM customers

WHERE LOWER(TRIM(traffic_source)) NOT IN (
  SELECT enumlabel
  FROM pg_enum
  JOIN pg_type ON enumtypid = pg_type.oid
  WHERE typname = 'traffic_source_enum'
);

CREATE TABLE invalid_operation_type AS
SELECT * FROM customer_balance

WHERE LOWER(TRIM(operation_type)) NOT IN (
  SELECT enumlabel
  FROM pg_enum
  JOIN pg_type ON enumtypid = pg_type.oid
  WHERE typname = 'operation_enum'
);

-- ===================================
-- STEP 7: End transaction
-- Ensures atomic execution — all changes are committed together
-- ===================================

COMMIT;