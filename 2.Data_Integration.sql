-- the S3 bucket in AWS for raw data dumping
CREATE OR REPLACE STORAGE INTEGRATION MIDTERM_S3_LOADING_INTEGRATION
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::622392631460:role/midterm-snowflake-stage-role'
STORAGE_ALLOWED_LOCATIONS = ('s3://midterm-data-dump-sk');

-- to get integration id
DESC STORAGE INTEGRATION MIDTERM_S3_LOADING_INTEGRATION;

USE database midterm_db;

--grant necessary permissions to integration and file format
GRANT CREATE STAGE ON SCHEMA RAW to ROLE accountadmin;
GRANT USAGE ON INTEGRATION MIDTERM_S3_LOADING_INTEGRATION to ROLE accountadmin;

-- file format ingested
create or replace file format csv_comma_skip1_format
type = 'CSV'
field_delimiter = ','
skip_header = 1;

-- create stage for the integration
CREATE OR REPLACE STAGE S3_WCD_MIDTERM_LOADING_STAGE
STORAGE_INTEGRATION = MIDTERM_S3_LOADING_INTEGRATION
URL='s3://midterm-data-dump-sk'
FILE_FORMAT = csv_comma_skip1_format;

DESC STAGE S3_WCD_MIDTERM_LOADING_STAGE;

list @S3_WCD_MIDTERM_LOADING_STAGE;
