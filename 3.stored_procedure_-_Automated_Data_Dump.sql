CREATE OR REPLACE PROCEDURE COPY_INTO_S3()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'snowflaketoS3'
AS
$$
import time
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col
from snowflake.snowpark import Session

def snowflaketoS3(session):
    date = time.strftime("%Y%m%d")
    # inventory
    inv = session.sql(f"COPY INTO '@S3_WCD_MIDTERM_LOADING_STAGE/inventory_{date}.csv' FROM (select * from midterm_db.raw.inventory where cal_dt <=current_date()) file_format=(TYPE=CSV, COMPRESSION='None') SINGLE=TRUE HEADER=TRUE MAX_FILE_SIZE=107772160 OVERWRITE=TRUE").collect()
    
  # sales
    sales = session.sql(f"COPY INTO '@S3_WCD_MIDTERM_LOADING_STAGE/sales_{date}.csv' FROM (select * from midterm_db.raw.sales where trans_dt <= current_date()) file_format=(TYPE=CSV, COMPRESSION='None') SINGLE=TRUE HEADER=TRUE MAX_FILE_SIZE=107772160 OVERWRITE=TRUE").collect()

  # store
    store = session.sql(f"COPY INTO '@S3_WCD_MIDTERM_LOADING_STAGE/store_{date}.csv' FROM (select * from midterm_db.raw.store) file_format=(TYPE=CSV, COMPRESSION='None') SINGLE=TRUE HEADER=TRUE MAX_FILE_SIZE=107772160 OVERWRITE=TRUE").collect()
    
  # product
    product = session.sql(f"COPY INTO '@S3_WCD_MIDTERM_LOADING_STAGE/product_{date}.csv' FROM (select * from midterm_db.raw.product) file_format=(TYPE=CSV, COMPRESSION='None') SINGLE=TRUE HEADER=TRUE MAX_FILE_SIZE=107772160 OVERWRITE=TRUE").collect()

  # calendar
    calendar = session.sql(f"COPY INTO '@S3_WCD_MIDTERM_LOADING_STAGE/calendar_{date}.csv' FROM (select * from midterm_db.raw.calendar) file_format=(TYPE=CSV, COMPRESSION='None') SINGLE=TRUE HEADER=TRUE MAX_FILE_SIZE=107772160 OVERWRITE=TRUE").collect()

  # 
    return "SUCCESS"
$$;

--Step 2. Create a task to run the job. Here we use cron to set job at 2am EST everyday. 
CREATE OR REPLACE TASK load_data_to_s3
WAREHOUSE = COMPUTE_WH 
SCHEDULE = 'USING CRON 0 2 * * * UTC'--18 22 * * * UTC' --USING CRON 0 2 * * * UTC'
AS
CALL COPY_INTO_S3();

--Step 3. Activate the task
ALTER TASK load_data_to_s3 resume;

--Step 4. Check if the task state is 'started'
DESCRIBE TASK load_data_to_s3;

--Step 5. Suspend the task
ALTER TASK load_data_to_s3 suspend;
--CALL COPY_INTO_S3();

describe PROCEDURE COPY_INTO_S3()