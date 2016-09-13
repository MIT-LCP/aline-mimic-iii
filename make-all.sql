-- This function runs all the scripts necessary to generate the ALINE dataset.

-- As the script is generating many materialized views, it may take some time.

BEGIN;
-- Generate the views
\i docs/cohort.sql
\i docs/echo-data.sql
\i docs/icd.sql
\i docs/labs.sql
\i docs/sofa-aline.sql
\i docs/vitals.sql
\i docs/vaso-flg.sql
\i docs/bmi.sql
\i docs/angus.sql

COMMIT;
