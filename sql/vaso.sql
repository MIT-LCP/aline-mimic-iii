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

DROP MATERIALIZED VIEW IF EXISTS ALINE_VASO;
CREATE MATERIALIZED VIEW ALINE_VASO as
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
  and statusdescription != 'Rewritten' -- only valid orders
)
, vasocv1 as
(
  select
    icustay_id, charttime, itemid
    -- case statement determining whether the ITEMID is an instance of vasopressor usage
    , 1 as vaso

    -- the 'stopped' column indicates if a vasopressor has been disconnected
    , max(case when stopped in ('Stopped','D/C''d') then 1
          else 0 end) as vaso_stopped

    , max(case when rate is not null then 1 else 0 end) as vaso_null
    , max(rate) as vaso_rate
    , max(amount) as vaso_amount

  from io_cv
  group by icustay_id, charttime, itemid
)
, vasocv2 as
(
  select v.*
    , sum(vaso_null) over (partition by icustay_id, itemid order by charttime) as vaso_partition
  from
    vasocv1 v
)
, vasocv3 as
(
  select v.*
    , first_value(vaso_rate) over (partition by icustay_id, itemid, vaso_partition order by charttime) as vaso_prevrate_ifnull
  from
    vasocv2 v
)
, vasocv4 as
(
select
    icustay_id
    , charttime
    , itemid
    -- , (CHARTTIME - (LAG(CHARTTIME, 1) OVER (partition by icustay_id, vaso order by charttime))) AS delta

    , vaso
    , vaso_rate
    , vaso_amount
    , vaso_stopped
    , vaso_prevrate_ifnull

    -- We define start time here
    , case
        when vaso = 0 then null

        -- if this is the first instance of the vasoactive drug
        when vaso_rate > 0 and
          LAG(vaso_prevrate_ifnull,1)
          OVER
          (
          partition by icustay_id, itemid, vaso, vaso_null
          order by charttime
          )
          is null
          then 1

        -- you often get a string of 0s
        -- we decide not to set these as 1, just because it makes vasonum sequential
        when vaso_rate = 0 and
          LAG(vaso_prevrate_ifnull,1)
          OVER
          (
          partition by icustay_id, itemid, vaso
          order by charttime
          )
          = 0
          then 0

        -- sometimes you get a string of NULL, associated with 0 volumes
        -- same reason as before, we decide not to set these as 1
        -- vaso_prevrate_ifnull is equal to the previous value *iff* the current value is null
        when vaso_prevrate_ifnull = 0 and
          LAG(vaso_prevrate_ifnull,1)
          OVER
          (
          partition by icustay_id, itemid, vaso
          order by charttime
          )
          = 0
          then 0

        -- If the last recorded rate was 0, newvaso = 1
        when LAG(vaso_prevrate_ifnull,1)
          OVER
          (
          partition by icustay_id, itemid, vaso
          order by charttime
          ) = 0
          then 1

        -- If the last recorded vaso was D/C'd, newvaso = 1
        when
          LAG(vaso_stopped,1)
          OVER
          (
          partition by icustay_id, itemid, vaso
          order by charttime
          )
          = 1 then 1

        -- ** not sure if the below is needed
        --when (CHARTTIME - (LAG(CHARTTIME, 1) OVER (partition by icustay_id, vaso order by charttime))) > (interval '4 hours') then 1
      else null
      end as vaso_start

FROM
  vasocv3
)
-- propagate start/stop flags forward in time
, vasocv5 as
(
  select v.*
    , SUM(vaso_start) OVER (partition by icustay_id, itemid, vaso order by charttime) as vaso_first
FROM
  vasocv4 v
)
, vasocv6 as
(
  select v.*
    -- We define end time here
    , case
        when vaso = 0
          then null

        -- If the recorded vaso was D/C'd, this is an end time
        when vaso_stopped = 1
          then vaso_first

        -- If the rate is zero, this is the end time
        when vaso_rate = 0
          then vaso_first

        -- the last row in the table is always a potential end time
        -- this captures patients who die/are discharged while on vasopressors
        -- in principle, this could add an extra end time for the vasopressor
        -- however, since we later group on vaso_start, any extra end times are ignored
        when LEAD(CHARTTIME,1)
          OVER
          (
          partition by icustay_id, itemid, vaso
          order by charttime
          ) is null
          then vaso_first

        else null
        end as vaso_stop
    from vasocv5 v
)

-- -- if you want to look at the results of the table before grouping:
-- select
--   icustay_id, charttime, vaso, vaso_rate, vaso_amount
--     , case when vaso_stopped = 1 then 'Y' else '' end as stopped
--     , vaso_start
--     , vaso_first
--     , vaso_stop
-- from vasocv6 order by charttime;


, vasocv as
(
-- below groups together vasopressor administrations into groups
select
  icustay_id
  , itemid
  -- the first non-null rate is considered the starttime
  , min(case when vaso_rate is not null then charttime else null end) as starttime
  -- the *first* time the first/last flags agree is the stop time for this duration
  , min(case when vaso_first = vaso_stop then charttime else null end) as endtime
from vasocv6
where
  vaso_first is not null -- bogus data
and
  vaso_first != 0 -- sometimes *only* a rate of 0 appears, i.e. the drug is never actually delivered
and
  icustay_id is not null -- there are data for "floating" admissions, we don't worry about these
group by icustay_id, itemid, vaso_first
having -- ensure start time is not the same as end time
 min(charttime) != min(case when vaso_first = vaso_stop then charttime else null end)
and
  max(vaso_rate) > 0 -- if the rate was always 0 or null, we consider it not a real drug delivery
)
-- we do not group by ITEMID in below query
-- this is because we want to collapse all vasopressors together
, vasocv_grp as
(
SELECT
  s1.icustay_id,
  s1.starttime,
  MIN(t1.endtime) AS endtime
FROM vasocv s1
INNER JOIN vasocv t1
  ON  s1.icustay_id = t1.icustay_id
  AND s1.starttime <= t1.endtime
  AND NOT EXISTS(SELECT * FROM vasocv t2
                 WHERE t1.icustay_id = t2.icustay_id
                 AND t1.endtime >= t2.starttime
                 AND t1.endtime < t2.endtime)
WHERE NOT EXISTS(SELECT * FROM vasocv s2
                 WHERE s1.icustay_id = s2.icustay_id
                 AND s1.starttime > s2.starttime
                 AND s1.starttime <= s2.endtime)
GROUP BY s1.icustay_id, s1.starttime
ORDER BY s1.icustay_id, s1.starttime
)
-- now we extract the associated data for metavision patients
-- do not need to group by itemid because we group by linkorderid
, vasomv as
(
  select
    icustay_id, linkorderid
    , min(starttime) as starttime, max(endtime) as endtime
  from io_mv
  group by icustay_id, linkorderid
)
, vasomv_grp as
(
SELECT
  s1.icustay_id,
  s1.starttime,
  MIN(t1.endtime) AS endtime
FROM vasomv s1
INNER JOIN vasomv t1
  ON  s1.icustay_id = t1.icustay_id
  AND s1.starttime <= t1.endtime
  AND NOT EXISTS(SELECT * FROM vasomv t2
                 WHERE t1.icustay_id = t2.icustay_id
                 AND t1.endtime >= t2.starttime
                 AND t1.endtime < t2.endtime)
WHERE NOT EXISTS(SELECT * FROM vasomv s2
                 WHERE s1.icustay_id = s2.icustay_id
                 AND s1.starttime > s2.starttime
                 AND s1.starttime <= s2.endtime)
GROUP BY s1.icustay_id, s1.starttime
ORDER BY s1.icustay_id, s1.starttime
)
, vd as
(
  select
    icustay_id
    -- generate a sequential integer for convenience
    , ROW_NUMBER() over (partition by icustay_id order by starttime) as vasonum
    , starttime, endtime
  from
    vasocv_grp

  UNION

  select
    icustay_id
    , ROW_NUMBER() over (partition by icustay_id order by starttime) as vasonum
    , starttime, endtime
  from
    vasomv_grp
)
, vaso_flg as
(
  -- join to cohort and get various flags
  select
    co.subject_id, co.hadm_id, co.icustay_id
    , extract(epoch from (min(vd.starttime - ie.intime)))/60.0/60.0/24.0 as vaso_start_day
    , extract(epoch from (min(ie.outtime - vd.endtime)))/60.0/60.0/24.0 as vaso_free_day
    , extract(epoch from (sum(vd.endtime - vd.starttime)))/60.0/60.0/24.0 as vaso_duration
    , extract(epoch from (max(ie.outtime - ie.intime)))/60.0/60.0/24.0 as icu_los
    , co.ALINE_FLG
    , co.INITIAL_ALINE_FLG
    , co.starttime_aline
  from aline_cohort co
  inner join icustays ie
    on co.icustay_id = ie.icustay_id
  left join vd
    on co.icustay_id = vd.icustay_id
  group by co.subject_id, co.hadm_id, co.icustay_id
    , co.ALINE_FLG, co.INITIAL_ALINE_FLG, co.starttime_aline
)
select
  subject_id, hadm_id, icustay_id
  , 1 as vaso_flg
  , v.vaso_start_day
  , v.vaso_free_day -- days free of vaso *after* last dose was given
  , v.vaso_duration as vaso_day -- days on vasopressors
  , v.icu_los - v.vaso_duration as vaso_off_day -- days *not* on vasopressors
  , case when v.vaso_start_day<=0.125 then 1 else 0 end as vaso_1st_3hr_flg
  , case when v.vaso_start_day<=0.25 then 1 else 0 end as vaso_1st_6hr_flg
  , case when v.vaso_start_day<=0.5 then 1 else 0 end as vaso_1st_12hr_flg
  , case
        -- if vaso started before aline
        when ALINE_FLG = 1 and INITIAL_ALINE_FLG = 0 and vaso_start_day<=starttime_aline then 1
        -- vaso started after aline
        when ALINE_FLG = 1 and INITIAL_ALINE_FLG = 0 and vaso_start_day>starttime_aline then 0
        -- no aline on admission, but vaso started on admission
        when ALINE_FLG = 0 and INITIAL_ALINE_FLG = 0 and v.vaso_start_day<=(2/24) then 1
        -- no aline on admission and no vaso on admission
        when ALINE_FLG = 0 and INITIAL_ALINE_FLG = 0 and v.vaso_start_day>(2/24) then 0
    else NULL end as vaso_b4_aline
from vaso_flg v
order by icustay_id;
