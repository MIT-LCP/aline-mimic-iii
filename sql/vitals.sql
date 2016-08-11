
DROP MATERIALIZED VIEW IF EXISTS ALINE_VITALS;
CREATE MATERIALIZED VIEW ALINE_VITALS as

-- first, group together ITEMIDs for the same vital sign
with vitals_stg0 as
(
  select
    co.subject_id, co.hadm_id, charttime
    , case
        -- MAP, Temperature, HR, CVP, SpO2,
        when itemid in (456,52,6702,443,220052,220181,225312) then 'MAP'
        when itemid in (223762,676,223761,678) then 'Temperature'
        when itemid in (211,220045) then 'HeartRate'
        when itemid in (113,220074) then 'CVP'
        when itemid in (646,220277) then 'SpO2'
      else null end as label
    -- convert F to C
    , case when itemid in (223761,678) then (valuenum-32)/1.8 else valuenum end as valuenum
  from chartevents ce
  inner join ALINE_COHORT co
    on ce.subject_id = co.subject_id
    and ce.charttime <= co.vent_starttime
    and ce.charttime >= co.vent_starttime - interval '1' day
)
-- next, assign an integer where rn=1 is the vital sign just preceeding vent
, vitals_stg1 as
(
  select
    subject_id, hadm_id, label
    , case when label = 'MAP' then valuenum else null end as MAP
    , case when label = 'Temperature' then valuenum else null end as Temperature
    , case when label = 'HeartRate' then valuenum else null end as HeartRate
    , case when label = 'CVP' then valuenum else null end as CVP
    , case when label = 'SpO2' then valuenum else null end as SpO2
    , ROW_NUMBER() over (partition by hadm_id, label order by charttime DESC) as rn
  from vitals_stg0
)
-- now aggregate where rn=1 to give the vital sign just before the vent starttime
, vitals as
(
  select
    subject_id, hadm_id, rn
    , min(MAP) as MAP
    , min(Temperature) as Temperature
    , min(HeartRate) as HeartRate
    , min(CVP) as CVP
    , min(SpO2) as SpO2
  from vitals_stg1
  group by subject_id, hadm_id, rn
  having rn = 1
)
select
  co.subject_id, co.hadm_id
  , v.MAP, v.Temperature, v.HeartRate, v.CVP, v.SpO2
from ALINE_COHORT co
left join vitals v
  on co.hadm_id = v.hadm_id
