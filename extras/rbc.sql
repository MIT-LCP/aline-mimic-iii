

DROP MATERIALIZED VIEW IF EXISTS ALINE_RBC;
CREATE MATERIALIZED VIEW ALINE_RBC AS
with rbc_cv as
(
  select
    cv.icustay_id
    , sum(case when itemid in (30001,30004,30179) then amount else null end) as RBC
    , sum(case when itemid in (30001,30004,30179)
            and charttime < ie.intime + interval '1' day then amount else null end) as RBC_day1

    , sum(case when itemid = 30104 then amount else null end) as RBC_OR
    , sum(case when itemid = 30104
            and charttime < ie.intime + interval '1' day then amount else null end) as RBC_OR_day1
  from inputevents_cv cv
  inner join icustays ie
    on cv.icustay_id = ie.icustay_id
  where cv.itemid in
  (
    30001,--Packed RBC's
    30004,--Washed PRBC's
    30104,--OR Packed RBC's
    30179 --PRBC's
  )
  group by cv.icustay_id, ie.intime
)
, rbc_mv_day1_stg1 as
(
  select
    mv.icustay_id
    , case when itemid in (220996,225168) then 'RBC'
          when itemid in (226368,227070) then 'RBC_OR'
        else null end as label
      -- if endtime < intime + 1 day, use the entire entry
    , case when endtime <= intime + interval '1' day
        then amount
      -- otherwise, use the fraction of the value which occurred before day1
      else (extract(epoch from (intime + interval '1' day - starttime))::NUMERIC)
          / extract(epoch from (endtime - starttime)) * amount
        end as amount
  from inputevents_mv mv
  inner join icustays ie
    on mv.icustay_id = ie.icustay_id
  where itemid in
  (
    220996, -- Packed Red Cells
    225168, -- Packed Red Blood Cells
    226368, -- OR Packed RBC Intake
    227070  -- PACU Packed RBC Intake
  )
  and statusdescription != 'Rewritten'
)
, rbc_mv_day1 as
(
  select
    icustay_id
    , sum(case when label = 'RBC' then amount else null end) as RBC_day1
    , sum(case when label = 'RBC_OR' then amount else null end) as RBC_OR_day1
  from rbc_mv_day1_stg1
  group by icustay_id
)
, rbc_mv as
(
  select
    ie.icustay_id
    , sum(case when itemid in (220996,225168) then amount else null end) as RBC
    , sum(case when itemid in (226368,227070) then amount else null end) as RBC_OR
  from icustays ie
  inner join inputevents_mv mv
    on ie.icustay_id = mv.icustay_id
    and mv.starttime <= ie.intime + interval '1' day
    and mv.itemid in (220996,225168,226368,227070)
    and mv.statusdescription != 'Rewritten'
  group by ie.icustay_id
)
select
  co.subject_id, co.hadm_id, co.icustay_id
  , case when cv.RBC is not null and mv.RBC is not null then cv.RBC+mv.RBC
      else coalesce(cv.RBC, mv.RBC) end as RBC
  , case when cv.RBC_day1 is not null and mvd1.RBC_day1 is not null then cv.RBC_day1+mvd1.RBC_day1
      else coalesce(cv.RBC_day1, mvd1.RBC_day1) end as RBC_day1

  , case when cv.RBC_OR is not null and mv.RBC_OR is not null then cv.RBC_OR+mv.RBC_OR
      else coalesce(cv.RBC_OR, mv.RBC_OR) end as RBC_OR
  , case when cv.RBC_OR_day1 is not null and mvd1.RBC_OR_day1 is not null then cv.RBC_OR_day1+mvd1.RBC_OR_day1
      else coalesce(cv.RBC_OR_day1, mvd1.RBC_OR_day1) end as RBC_OR_day1
from aline_cohort co
left join rbc_cv cv
  on co.icustay_id = cv.icustay_id
left join rbc_mv_day1 mvd1
  on co.icustay_id = mvd1.icustay_id
left join rbc_mv mv
  on co.icustay_id = mv.icustay_id;
