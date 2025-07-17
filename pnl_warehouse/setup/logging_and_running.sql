SHOW DBT PROJECTS;

/* Updated the dbt project object from TASTY_BYTES to PNL_WAREHOUSE - No need to re-run this!! */
-- ALTER DBT PROJECT BLUECREST_TRANSFORM.DBT.TASTY_BYTES RENAME TO BLUECREST_TRANSFORM.DBT.PNL_WAREHOUSE;

/* Setting log, trace and metric levels for the dbt project */
ALTER SCHEMA bluecrest_transform.dbt SET LOG_LEVEL = 'INFO';
ALTER SCHEMA bluecrest_transform.dbt SET TRACE_LEVEL = 'ALWAYS';
ALTER SCHEMA bluecrest_transform.dbt SET METRIC_LEVEL = 'ALL';

-- Compile the dbt project
EXECUTE DBT PROJECT PNL_WAREHOUSE args='compile --target dev';

-- Run the dbt project
EXECUTE DBT PROJECT PNL_WAREHOUSE args='run --target dev';

