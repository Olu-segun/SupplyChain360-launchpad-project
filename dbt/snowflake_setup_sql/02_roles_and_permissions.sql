-- Roles and Permissions Setup for SupplyChain360 Snowflake Environment

-- 1. Create a role for dbt and users to interact with the database
CREATE ROLE IF NOT EXISTS SUPPLYCHAIN360_ROLE;

-- 2. Warehouse Access: Grant usage on the warehouse to the role
GRANT USAGE 
ON WAREHOUSE SUPPLYCHAIN360_WH 
TO ROLE SUPPLYCHAIN360_ROLE;

-- 3. Database Access: Grant usage on the database to the role. This allows 
-- the role to see the database and its schemas.
GRANT USAGE 
ON DATABASE SUPPLYCHAIN360_DB 
TO ROLE SUPPLYCHAIN360_ROLE;

-- Allow the role to create schemas (required for dbt to manage staging and marts schemas)
GRANT CREATE SCHEMA 
ON DATABASE SUPPLYCHAIN360_DB 
TO ROLE SUPPLYCHAIN360_ROLE;

-- 4. Raw Schema (Ingestion Layer). Immutable raw data 
-- (ingested from s3 bucket) that serves as the source for all 
-- transformations. This schema is read-only for dbt models to ensure data integrity.

GRANT USAGE 
ON SCHEMA SUPPLYCHAIN360_DB.RAW 
TO ROLE SUPPLYCHAIN360_ROLE;

-- Read-only access to raw data for dbt models
GRANT SELECT 
ON ALL TABLES IN SCHEMA SUPPLYCHAIN360_DB.RAW 
TO ROLE SUPPLYCHAIN360_ROLE;

-- Grant select on future tables to ensure new raw tables are 
-- accessible without manual intervention
GRANT SELECT 
ON FUTURE TABLES IN SCHEMA SUPPLYCHAIN360_DB.RAW 
TO ROLE SUPPLYCHAIN360_ROLE;

-- 5. Staging Schema (Transformation Layer) Cleaned & standardized data (dbt models)
GRANT USAGE 
ON SCHEMA SUPPLYCHAIN360_DB.STAGING 
TO ROLE SUPPLYCHAIN360_ROLE;

-- Allow dbt to create models in staging
GRANT CREATE TABLE 
ON SCHEMA SUPPLYCHAIN360_DB.STAGING 
TO ROLE SUPPLYCHAIN360_ROLE;

-- Allow dbt to create views in staging
GRANT CREATE VIEW 
ON SCHEMA SUPPLYCHAIN360_DB.STAGING 
TO ROLE SUPPLYCHAIN360_ROLE;

-- Required for dbt incremental + updates
GRANT INSERT, UPDATE, DELETE 
ON ALL TABLES IN SCHEMA SUPPLYCHAIN360_DB.STAGING 
TO ROLE SUPPLYCHAIN360_ROLE;

-- Grant permissions on future tables to ensure new staging tables are 
--manageable without manual intervention
GRANT INSERT, UPDATE, DELETE 
ON FUTURE TABLES IN SCHEMA SUPPLYCHAIN360_DB.STAGING 
TO ROLE SUPPLYCHAIN360_ROLE;


-- 6. MARTS SCHEMA  (ANALYTICS LAYER) 
-- Purpose: Final business-ready tables and views for analysis and reporting

GRANT USAGE 
ON SCHEMA SUPPLYCHAIN360_DB.MARTS 
TO ROLE SUPPLYCHAIN360_ROLE;

GRANT CREATE TABLE 
ON SCHEMA SUPPLYCHAIN360_DB.MARTS 
TO ROLE SUPPLYCHAIN360_ROLE;

GRANT CREATE VIEW 
ON SCHEMA SUPPLYCHAIN360_DB.MARTS 
TO ROLE SUPPLYCHAIN360_ROLE;

-- Required for dbt incremental + updates
GRANT INSERT, UPDATE, DELETE 
ON ALL TABLES IN SCHEMA SUPPLYCHAIN360_DB.MARTS 
TO ROLE SUPPLYCHAIN360_ROLE;

GRANT INSERT, UPDATE, DELETE 
ON FUTURE TABLES IN SCHEMA SUPPLYCHAIN360_DB.MARTS 
TO ROLE SUPPLYCHAIN360_ROLE;

-- 7. Grant the role to your user so you can interact with the 
--database using dbt and other tools
GRANT ROLE SUPPLYCHAIN360_ROLE TO USER ERIC;