-- This query defines the cohort used for the ALINE study.

-- Inclusion criteria:
--  adult patients
--  In ICU for at least 24 hours
--  First ICU admission
--  mechanical ventilation within the first 12 hours
--  medical or surgical ICU admission

-- Exclusion criteria:
--  **Angus sepsis
--  **On vasopressors (?is this different than on dobutamine)
--  IAC placed before admission
--  CSRU patients

-- **These exclusion criteria are applied in the data.sql file.

-- This query also extracts demographics, and necessary preliminary flags needed
-- for data extraction. For example, since all data is extracted before
-- ventilation, we need to extract start times of ventilation


-- This query requires the following tables:
--  ventdurations - extracted by mimic-code/etc/ventilation-durations.sql


DROP MATERIALIZED VIEW IF EXISTS ALINE_COHORT CASCADE;
CREATE MATERIALIZED VIEW ALINE_COHORT as

-- get start time of arterial line
-- Definition of arterial line insertion:
--  First measurement of invasive blood pressure
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
-- first time ventilation was started
-- last time ventilation was stopped
, ve as
(
  select icustay_id
    , sum(extract(epoch from endtime-starttime))/24.0/60.0/60.0 as vent_day
    , min(starttime) as starttime_first
    , max(endtime) as endtime_last
  from ventdurations vd
  group by icustay_id
)
, serv as
(
    select ie.icustay_id, se.curr_service
    , ROW_NUMBER() over (partition by ie.icustay_id order by se.transfertime DESC) as rn
    from icustays ie
    inner join services se
      on ie.hadm_id = se.hadm_id
      and se.transfertime < ie.intime + interval '2' hour
)
-- cohort view - used to define other concepts
, co as
(
  select
    ie.subject_id, ie.hadm_id, ie.icustay_id
    , ie.intime as icustay_intime
    , to_char(ie.intime, 'day') as day_icu_intime
    , extract(dow from ie.intime) as day_icu_intime_num
    , extract(hour from ie.intime) as hour_icu_intime
    ,
    , ie.outtime as icustay_outtime

    , ROW_NUMBER() over (partition by ie.subject_id order by adm.admittime, ie.intime) as stay_num
    , extract(epoch from (ie.intime - pat.dob))/365.242/24.0/60.0/60.0 as age
    , pat.gender
    , case when pat.gender = 'M' then 1 else 0 end as gender_num

    -- TODO: weight_first, height_first, bmi
    --  bmi

    -- service

    -- collapse ethnicity into fixed categories

    -- time of a-line
    , case when a.starttime_aline is not null then 1 else 0 end as aline_flg
    , extract(epoch from (a.starttime_aline - ie.intime))/24.0/60.0/60.0 as aline_time_day
    , case
        when a.starttime_aline is not null
         and a.starttime_aline <= ie.intime + interval '1' hour
          then 1
        else 0
      end as initial_aline_flg

    -- ventilation
    , case when ve.icustay_id is not null then 1 else 0 end as vent_flg
    , case when ve.starttime_first < ie.intime + interval '12' hour then 1 else 0 end as vent_1st_12hr
    , case when ve.starttime_first < ie.intime + interval '24' hour then 1 else 0 end as vent_1st_24hr

    -- binary flag: were they ventilated before a-line insertion?
    , case
        when a.starttime_aline is not null and a.starttime_aline > ie.intime + interval '1' hour and ve.starttime_first<=a.starttime_aline then 1
        when a.starttime_aline is not null and a.starttime_aline > ie.intime + interval '1' hour and ve.starttime_first>a.starttime_aline then 0
        when a.starttime_aline is null and v.vent_start_day<=(2/24) then 1
        when a.starttime_aline is null and v.vent_start_day>(2/24) then 0
        else NULL
      end as vent_b4_aline

    -- number of days *not* on a ventilator
    , extract(epoch from (ie.outtime - ie.intime))/24.0/60.0/60.0 - vent_day as vent_free_day

    , ve.starttime_first as vent_starttime
    , ve.endtime_last as vent_endtime
    , ve.vent_day

    -- cohort flags // demographics
    , extract(epoch from (ie.outtime - ie.intime))/24.0/60.0/60.0 as icu_los_day
    , extract(epoch from (adm.dischtime - adm.admittime))/24.0/60.0/60.0 as hospital_los_day
    , extract('dow' from intime) as intime_dayofweek
    , extract('hour' from intime) as intime_hour

    -- will be used to exclude patients in CSRU
    -- also only include those in CMED or SURG
    , s.curr_service as service_unit
    , case when s.curr_service like '%SURG' or s.curr_service like '%ORTHO%' then 1
          when s.curr_service = 'CMED' then 2
          when s.curr_service in ('CSURG','VSURG','TSURG') then 3
          else 0
        end
      as service_num

    -- outcome
    , case when adm.deathtime is not null then 1 else 0 end as hosp_exp_flg
    , case when adm.deathtime <= ie.outtime then 1 else 0 end as icu_exp_flg
    , case when pat.dod <= (ie.intime + interval '28' day) then 1 else 0 end as day_28_flg
    , extract(epoch from (pat.dod - adm.admittime))/24.0/60.0/60.0 as mort_day

    , case when pat.dod is null
        then 150 -- patient deaths are censored 150 days after admission
        else extract(epoch from (pat.dod - adm.admittime))/24.0/60.0/60.0
      end as mort_day_censored
    , case when pat.dod is null then 1 else 0 end as censor_flg

  from icustays ie
  inner join admissions adm
    on ie.hadm_id = adm.hadm_id
  inner join patients pat
    on ie.subject_id = pat.subject_id
  left join a
    on ie.icustay_id = a.icustay_id
  left join ve_grp
    on ie.icustay_id = ve_grp.icustay_id
  left join serv s
    on ie.icustay_id = s.icustay_id
    and s.rn = 1
  where ie.intime > (pat.dob + interval '16' year) -- only adults
)
select
  co.*
from co
where stay_num = 1 -- first ICU stay
and icu_los_day > 1 -- one day in the ICU
and initial_aline_flg = 0 -- aline placed later than admission
and vent_starttime is not null -- were ventilated
and vent_starttime < intime + interval '12' hour -- ventilated within first 12 hours
and service_unit not in
(
  'CSURG','VSURG','TSURG' -- cardiac/vascular/thoracic surgery
  ,'NB'
  ,'NBB'
);
--  TODO: can't define medical or surgical ICU admission using ICU service type


-- Recall, two exclusion criteria are applied in data.sql:
--  **Angus sepsis
--  **On vasopressors (?is this different than on dobutamine)
