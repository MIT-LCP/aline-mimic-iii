-- Create a table of durations for a subset of vasopressors, specifically:
-- norepinephrine - 30047,30120,221906
-- epinephrine - 30044,30119,30309,221289
-- phenylephrine - 30127,30128,221749
-- vasopressin - 30051,222315
-- dopamine - 30043,30307,221662
-- Isuprel - 30046,227692

-- outputs:
--  vaso_start_day - number of fractional days before start of vasopressors
--  vaso_free_day - number of fractional days after discontinuation of vasopressors
--  vaso_duration - total duration of vasopressor usage

-- Note also that "vaso_free_day" is the number of days in the ICU after
-- the last vasopressor is discontinued. It excludes periods between vasopressor
-- doses, and so (vaso_start_day + vaso_free_day + vaso_duration) != ICU_LOS.

DROP MATERIALIZED VIEW IF EXISTS ALINE_VASO_FLG;
CREATE MATERIALIZED VIEW ALINE_VASO_FLG as
with io_cv as
(
  select
    icustay_id, charttime, itemid, stopped, rate, amount
  from mimiciii.inputevents_cv
  where itemid in
  (
    30047,30120 -- norepinephrine
    ,30044,30119,30309 -- epinephrine
    ,30127,30128 -- phenylephrine
    ,30051 -- vasopressin
    ,30043,30307,30125 -- dopamine
    ,30046 -- isuprel
  )
  and rate is not null
  and rate > 0
)
-- select only the ITEMIDs from the inputevents_mv table related to vasopressors
, io_mv as
(
  select
    icustay_id, linkorderid, starttime, endtime
  from mimiciii.inputevents_mv io
  -- Subselect the vasopressor ITEMIDs
  where itemid in
  (
  221906 -- norepinephrine
  ,221289 -- epinephrine
  ,221749 -- phenylephrine
  ,222315 -- vasopressin
  ,221662 -- dopamine
  ,227692 -- isuprel
  )
  and rate is not null
  and rate > 0
  and statusdescription != 'Rewritten' -- only valid orders
)
select
  co.subject_id, co.hadm_id, co.icustay_id
  , max(case when coalesce(io_mv.icustay_id, io_cv.icustay_id) is not null then 1 else 0 end) as vaso_flg
from aline_cohort co
left join io_mv
  on co.icustay_id = io_mv.icustay_id
left join io_cv
  on co.icustay_id = io_cv.icustay_id
group by co.subject_id, co.hadm_id, co.icustay_id
order by icustay_id;
