-- This query extracts:
--    i) a patient's first code status
--    ii) whether the patient ever had another code status

DROP MATERIALIZED VIEW IF EXISTS ALINE_CODESTATUS;
CREATE MATERIALIZED VIEW ALINE_CODESTATUS AS
with t1 as
(
  select icustay_id, charttime
  -- coalesce the values
  , case
      when value = 'Full Code' then 'FullCode'
      when value = 'Comfort Measures' then 'CMO'
      when value in ('CPR Not Indicate','CPR Not Indicated') then 'DNCPR'
      when value in ('Do Not Intubate','Do Not Intubate') then 'DNI'
      when value in ('Do Not Resuscita','Do Not Resuscitate') then 'DNR'
    else null end CodeStatus
  , ROW_NUMBER() over (partition by icustay_id order by charttime ASC) as rnFirst
  , ROW_NUMBER() over (partition by icustay_id order by charttime DESC) as rnLast
  from chartevents
  where itemid in (128, 223758)
  and value is not null
  and value != 'Other/Remarks'
)
, dnr_stg as
(
  select t1.icustay_id
    , max(case when rnFirst = 1 then CodeStatus else null end)
        as FirstCodeStatus
    , max(case when  rnLast = 1 then CodeStatus else null end)
        as LastCodeStatus
    -- were they ever DNR/CMO
    , max(case when CodeStatus = 'DNR' then 1 else 0 end) as DNR
    , max(case when CodeStatus = 'CMO' then 1 else 0 end) as CMO
    , max(case when CodeStatus = 'DNCPR' then 1 else 0 end) as DNCPR
    , max(case when CodeStatus = 'DNI' then 1 else 0 end) as DNI
    , max(case when CodeStatus = 'FullCode' then 1 else 0 end) as FullCode
  from t1
  group by t1.icustay_id
)
select
  co.subject_id, co.hadm_id, co.icustay_id
  , FirstCodeStatus, LastCodeStatus
  , DNR, CMO, DNCPR, DNI, FullCode
  , case when FirstCodeStatus = 'DNR' then 1 else 0 end as dnr_adm_flg
  , case when FirstCodeStatus != 'DNR' and DNR = 1 then 1 else 0 end as dnr_switch_flg
  , case when FirstCodeStatus != 'CMO' and CMO = 1 then 1 else 0 end as cmo_switch_flg
  , case
      when FirstCodeStatus != 'DNR' and DNR = 1 then 1
      when FirstCodeStatus != 'CMO' and CMO = 1 then 1
    else 0 end as dnr_cmo_switch_flg
from aline_cohort co
left join dnr_stg dnr
  on co.icustay_id = dnr.icustay_id
order by co.subject_id, co.hadm_id, co.icustay_id;
