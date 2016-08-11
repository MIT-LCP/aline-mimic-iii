-- Extract durations of anaesthetic medication, defined as:
-- Fentanyl: 30118, 30149 (Conc), 30150 (Base), 30308 (Drip), 221744, 225972 (Push), 225942 (Conc)
-- Propofol: 30131, 227210 (Intub), 222168
-- Midazolam: 30124, 221668
-- Dilaudid: 30163, 221833

DROP MATERIALIZED VIEW IF EXISTS ALINE_ANAESTHESIA;
CREATE MATERIALIZED VIEW ALINE_ANAESTHESIA as
with io_cv as
(
  select
    icustay_id, charttime, itemid, stopped, rate, amount
  from mimiciii.inputevents_cv
  where itemid in
  (
    30118, 30149, 30150, 30308 -- Fentanyl
    , 30131 -- Propofol
    , 30124 -- Midazolam
    , 30163 -- Dilaudid
  )
)
-- select only the ITEMIDs from the inputevents_mv table related to medications
, io_mv as
(
  select
    icustay_id, linkorderid, starttime, endtime
  from mimiciii.inputevents_mv io
  -- Subselect the medication ITEMIDs
  where itemid in
  (
    222168, --Propofol
    227210, --Propofol (Intubation)
    221668, --Midazolam (Versed)
    221833, --Hydromorphone (Dilaudid)
    221744, --Fentanyl
    225972, --Fentanyl (Push)
    225942  --Fentanyl (Concentrate)
  )
  and statusdescription != 'Rewritten' -- only valid orders
)
, drugcv1 as
(
  select
    icustay_id, charttime, itemid
    -- case statement determining whether the ITEMID is an instance of medication usage
    , 1 as drug

    -- the 'stopped' column indicates if a medication has been disconnected
    , max(case when stopped in ('Stopped','D/C''d') then 1
          else 0 end) as drug_stopped

    , max(case when rate is not null then 1 else 0 end) as drug_null
    , max(rate) as drug_rate
    , max(amount) as drug_amount

  from io_cv
  group by icustay_id, charttime, itemid
)
, drugcv2 as
(
  select v.*
    , sum(drug_null) over (partition by icustay_id, itemid order by charttime) as drug_partition
  from
    drugcv1 v
)
, drugcv3 as
(
  select v.*
    , first_value(drug_rate) over (partition by icustay_id, itemid, drug_partition order by charttime) as drug_prevrate_ifnull
  from
    drugcv2 v
)
, drugcv4 as
(
select
    icustay_id
    , charttime
    , itemid
    -- , (CHARTTIME - (LAG(CHARTTIME, 1) OVER (partition by icustay_id, drug order by charttime))) AS delta

    , drug
    , drug_rate
    , drug_amount
    , drug_stopped
    , drug_prevrate_ifnull

    -- We define start time here
    , case
        when drug = 0 then null

        -- if this is the first instance of the drug
        when drug_rate > 0 and
          LAG(drug_prevrate_ifnull,1)
          OVER
          (
          partition by icustay_id, itemid, drug, drug_null
          order by charttime
          )
          is null
          then 1

        -- you often get a string of 0s
        -- we decide not to set these as 1, just because it makes drugnum sequential
        when drug_rate = 0 and
          LAG(drug_prevrate_ifnull,1)
          OVER
          (
          partition by icustay_id, itemid, drug
          order by charttime
          )
          = 0
          then 0

        -- sometimes you get a string of NULL, associated with 0 volumes
        -- same reason as before, we decide not to set these as 1
        -- drug_prevrate_ifnull is equal to the previous value *iff* the current value is null
        when drug_prevrate_ifnull = 0 and
          LAG(drug_prevrate_ifnull,1)
          OVER
          (
          partition by icustay_id, itemid, drug
          order by charttime
          )
          = 0
          then 0

        -- If the last recorded rate was 0, newdrug = 1
        when LAG(drug_prevrate_ifnull,1)
          OVER
          (
          partition by icustay_id, itemid, drug
          order by charttime
          ) = 0
          then 1

        -- If the last recorded drug was D/C'd, newdrug = 1
        when
          LAG(drug_stopped,1)
          OVER
          (
          partition by icustay_id, itemid, drug
          order by charttime
          )
          = 1 then 1

        -- ** not sure if the below is needed
        --when (CHARTTIME - (LAG(CHARTTIME, 1) OVER (partition by icustay_id, drug order by charttime))) > (interval '4 hours') then 1
      else null
      end as drug_start

FROM
  drugcv3
)
-- propagate start/stop flags forward in time
, drugcv5 as
(
  select v.*
    , SUM(drug_start) OVER (partition by icustay_id, itemid, drug order by charttime) as drug_first
FROM
  drugcv4 v
)
, drugcv6 as
(
  select v.*
    -- We define end time here
    , case
        when drug = 0
          then null

        -- If the recorded drug was D/C'd, this is an end time
        when drug_stopped = 1
          then drug_first

        -- If the rate is zero, this is the end time
        when drug_rate = 0
          then drug_first

        -- the last row in the table is always a potential end time
        -- this captures patients who die/are discharged while on medications
        -- in principle, this could add an extra end time for the medication
        -- however, since we later group on drug_start, any extra end times are ignored
        when LEAD(CHARTTIME,1)
          OVER
          (
          partition by icustay_id, itemid, drug
          order by charttime
          ) is null
          then drug_first

        else null
        end as drug_stop
    from drugcv5 v
)

-- -- if you want to look at the results of the table before grouping:
-- select
--   icustay_id, charttime, drug, drug_rate, drug_amount
--     , case when drug_stopped = 1 then 'Y' else '' end as stopped
--     , drug_start
--     , drug_first
--     , drug_stop
-- from drugcv6 order by charttime;


, drugcv as
(
-- below groups together medication administrations into groups
select
  icustay_id
  , itemid
  -- the first non-null rate is considered the starttime
  , min(case when drug_rate is not null then charttime else null end) as starttime
  -- the *first* time the first/last flags agree is the stop time for this duration
  , min(case when drug_first = drug_stop then charttime else null end) as endtime
from drugcv6
where
  drug_first is not null -- bogus data
and
  drug_first != 0 -- sometimes *only* a rate of 0 appears, i.e. the drug is never actually delivered
and
  icustay_id is not null -- there are data for "floating" admissions, we don't worry about these
group by icustay_id, itemid, drug_first
having -- ensure start time is not the same as end time
 min(charttime) != min(case when drug_first = drug_stop then charttime else null end)
and
  max(drug_rate) > 0 -- if the rate was always 0 or null, we consider it not a real drug delivery
)
-- we do not group by ITEMID in below query
-- this is because we want to collapse all medications together
, drugcv_grp as
(
SELECT
  s1.icustay_id,
  s1.starttime,
  MIN(t1.endtime) AS endtime
FROM drugcv s1
INNER JOIN drugcv t1
  ON  s1.icustay_id = t1.icustay_id
  AND s1.starttime <= t1.endtime
  AND NOT EXISTS(SELECT * FROM drugcv t2
                 WHERE t1.icustay_id = t2.icustay_id
                 AND t1.endtime >= t2.starttime
                 AND t1.endtime < t2.endtime)
WHERE NOT EXISTS(SELECT * FROM drugcv s2
                 WHERE s1.icustay_id = s2.icustay_id
                 AND s1.starttime > s2.starttime
                 AND s1.starttime <= s2.endtime)
GROUP BY s1.icustay_id, s1.starttime
ORDER BY s1.icustay_id, s1.starttime
)
-- now we extract the associated data for metavision patients
-- do not need to group by itemid because we group by linkorderid
, drugmv as
(
  select
    icustay_id, linkorderid
    , min(starttime) as starttime, max(endtime) as endtime
  from io_mv
  group by icustay_id, linkorderid
)
, drugmv_grp as
(
SELECT
  s1.icustay_id,
  s1.starttime,
  MIN(t1.endtime) AS endtime
FROM drugmv s1
INNER JOIN drugmv t1
  ON  s1.icustay_id = t1.icustay_id
  AND s1.starttime <= t1.endtime
  AND NOT EXISTS(SELECT * FROM drugmv t2
                 WHERE t1.icustay_id = t2.icustay_id
                 AND t1.endtime >= t2.starttime
                 AND t1.endtime < t2.endtime)
WHERE NOT EXISTS(SELECT * FROM drugmv s2
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
    , ROW_NUMBER() over (partition by icustay_id order by starttime) as drugnum
    , starttime, endtime
  from
    drugcv_grp

  UNION

  select
    icustay_id
    , ROW_NUMBER() over (partition by icustay_id order by starttime) as drugnum
    , starttime, endtime
  from
    drugmv_grp
)
, drug_flg as
(
  -- join to cohort and get various flags
  select
    co.subject_id, co.hadm_id, co.icustay_id
    , extract(epoch from (min(vd.starttime - ie.intime)))/60.0/60.0/24.0 as drug_start_day
    , extract(epoch from (min(ie.outtime - vd.endtime)))/60.0/60.0/24.0 as drug_free_day
    , extract(epoch from (sum(vd.endtime - vd.starttime)))/60.0/60.0/24.0 as drug_duration
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
  , 1 as anes_flg
  , v.drug_start_day as anes_start_day
  , v.drug_free_day as anes_free_day -- days free of anaesthesia *after* the last dose is given
  , v.drug_duration as anes_day -- number of fractional days on anaesthesia
  , v.icu_los - v.drug_duration as anes_off_day -- number of fractional days *not* on anaesthesia
  , case
        -- if drug started before aline
        when ALINE_FLG = 1 and INITIAL_ALINE_FLG = 0 and drug_start_day<=starttime_aline then 1
        -- drug started after aline
        when ALINE_FLG = 1 and INITIAL_ALINE_FLG = 0 and drug_start_day>starttime_aline then 0
        -- no aline on admission, but drug started on admission
        when ALINE_FLG = 0 and INITIAL_ALINE_FLG = 0 and drug_start_day<=(2/24) then 1
        -- no aline on admission and no drug on admission
        when ALINE_FLG = 0 and INITIAL_ALINE_FLG = 0 and drug_start_day>(2/24) then 0
    else NULL end as anes_b4_aline
from drug_flg v
order by icustay_id;
