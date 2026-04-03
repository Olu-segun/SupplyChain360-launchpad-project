-- Storage Integration (S3 → SNOWFLAKE)

-- This step is crucial for enabling Snowflake to securely access to 
-- S3 bucket where the raw data is stored. The storage integration defines the 
-- permissions and connection details needed for Snowflake to read from S3.
-- Run this with ACCOUNTADMIN role

-- 1. CREATE STORAGE INTEGRATION
CREATE STORAGE INTEGRATION IF NOT EXISTS SUPPLYCHAIN360_S3_INTEGRATION
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = '<AWS_ROLE_ARN>'
STORAGE_ALLOWED_LOCATIONS = ('<S3_BUCKET_PATH>');

-- 2. Verify the storage integration details to ensure it's set up correctly
DESC INTEGRATION SUPPLYCHAIN360_S3_INTEGRATION;

-- 3. Grant usage on the storage integration to the role so dbt models can access it
GRANT USAGE 
ON INTEGRATION SUPPLYCHAIN360_S3_INTEGRATION 
TO ROLE SUPPLYCHAIN360_ROLE;