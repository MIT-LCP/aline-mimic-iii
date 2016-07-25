-- This query defines the cohort used for the ALINE study.
-- Exclusion criteria: non-adult patients, first ICU admission
-- TODO: medical or surgical ICU admission, in ICU for at least 24 hours

-- This query also extracts demographics, and necessary preliminary flags needed
-- for data extraction. For example, since all data is extracted before
-- ventilation, we need to extract start times of ventilation

-- Definition of arterial line insertion:
--  First measurement of invasive blood pressure

DROP MATERIALIZED VIEW IF EXISTS ALINE_COHORT CASCADE;
CREATE MATERIALIZED VIEW ALINE_COHORT as
with a as
(
  select icustay_id
  , min(charttime) as starttime_aline
  from chartevents
  where icustay_id is not null
  and valuenum is not null
  and itemid in
  (
    51, --	Arterial BP [Systolic]
    6701, --	Arterial BP #2 [Systolic]
    220050, --	Arterial Blood Pressure systolic

    8368, --	Arterial BP [Diastolic]
    8555, --	Arterial BP #2 [Diastolic]
    220051, --	Arterial Blood Pressure diastolic

    52, --"Arterial BP Mean"
    6702, --	Arterial BP Mean #2
    220052, --"Arterial Blood Pressure mean"
    225312 --"ART BP mean"
  )
  group by icustay_id
)
, ve as
(
  select adm.subject_id, adm.hadm_id, ie.icustay_id
    , min(starttime) as starttime_first
    , max(endtime) as endtime_last
    , ROW_NUMBER() over (partition by adm.hadm_id order by ie.intime) as rn
  from ventdurations vd
  inner join icustays ie
    on vd.icustay_id = ie.icustay_id
  inner join admissions adm
    on ie.hadm_id = adm.hadm_id
  group by adm.subject_id, adm.hadm_id, ie.icustay_id, ie.intime
)
, ve_grp as
(
  select subject_id, hadm_id, icustay_id
    , starttime_first
    , endtime_last
  from ve
  where rn = 1
)
-- cohort view - used to define other concepts
, co as
(
  select
    ie.subject_id, ie.hadm_id, ie.icustay_id
    , ROW_NUMBER() over (partition by ie.subject_id order by adm.admittime, ie.intime) as stay_num
    , extract(epoch from (ie.intime - pat.dob))/365.242/24.0/60.0/60.0 as age
    , pat.gender

    -- -- weight/height
    -- , height_first
    -- , weight_first
    -- , bmi
    --
    -- service

    -- collapse ethnicity into fixed categories

    -- time of a-line
    , case when a.starttime_aline is not null then 1 else 0 end as aline_flg
    , extract(epoch from (a.starttime_aline - ie.intime))/365.242/24.0/60.0/60.0 as starttime_aline
    , case
        when a.starttime_aline is not null
         and a.starttime_aline <= ie.intime + interval '1' hour
          then 1
        else 0
      end as initial_aline_flg

    -- ventilation
    , ve_grp.starttime_first as vent_starttime
    , ve_grp.endtime_last as vent_endtime

    -- cohort flags // demographics
    , extract(epoch from (ie.outtime - ie.intime))/365.242/24.0/60.0/60.0 as icu_los
    , extract(epoch from (adm.dischtime - adm.admittime))/365.242/24.0/60.0/60.0 as hospital_los
    , extract('dow' from intime) as intime_dayofweek
    , extract('hour' from intime) as intime_hour


    -- outcome
    , case when adm.deathtime is not null then 1 else 0 end as death_in_hospital
    , case when adm.deathtime <= ie.outtime then 1 else 0 end as death_in_icu
    , case when pat.dod <= (adm.admittime + interval '28' day) then 1 else 0 end as death_in_28days
    , extract(epoch from (pat.dod - adm.admittime))/365.242/24.0/60.0/60.0 as death_offset
    -- TODO: censored definition

  from icustays ie
  inner join admissions adm
    on ie.hadm_id = adm.hadm_id
  inner join patients pat
    on ie.subject_id = pat.subject_id
  left join a
    on ie.icustay_id = a.icustay_id
  left join ve_grp
    on ie.icustay_id = ve_grp.icustay_id
  where ie.intime > (pat.dob + interval '16' year) -- only adults
)
select
  co.*
from co
where stay_num = 1;
