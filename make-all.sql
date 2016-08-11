-- This function runs all the scripts necessary to generate the ALINE dataset.

-- As the script is generating many materialized views, it may take some time.

BEGIN;
-- Generate the views
\i sql/cohort.sql
\i sql/echo-data.sql
\i sql/icd.sql
\i sql/labs.sql
\i sql/sofa-aline
\i sql/vitals.sql
\i sql/vaso-flg.sql
\i sql/angus.sql

COMMIT;
