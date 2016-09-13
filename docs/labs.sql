

DROP MATERIALIZED VIEW IF EXISTS ALINE_LABS CASCADE;
CREATE MATERIALIZED VIEW ALINE_LABS as

-- count the number of blood gas measurements
-- abg_count - number of labs with pH/PCO2/PO2
-- vbg_count - number of times VBG appears in chartevents
with bg as
(
  select * from labevents limit 5
  -- abg_count
  -- vbg_count
)
-- we would like the *last* lab preceeding mechanical ventilation
, labs_preceeding as
(
  select co.subject_id, co.hadm_id
    , l.valuenum, l.charttime
    , case
            when itemid = 51006 then 'BUN'
            when itemid = 50806 then 'CHLORIDE'
            when itemid = 50902 then 'CHLORIDE'
            when itemid = 50912 then 'CREATININE'
            when itemid = 50811 then 'HEMOGLOBIN'
            when itemid = 51222 then 'HEMOGLOBIN'
            when itemid = 51265 then 'PLATELET'
            when itemid = 50822 then 'POTASSIUM'
            when itemid = 50971 then 'POTASSIUM'
            when itemid = 50824 then 'SODIUM'
            when itemid = 50983 then 'SODIUM'
            when itemid = 50803 then 'TOTALCO2' -- actually is 'BICARBONATE'
            when itemid = 50882 then 'TOTALCO2' -- actually is 'BICARBONATE'
            when itemid = 50804 then 'TOTALCO2'
            when itemid = 51300 then 'WBC'
            when itemid = 51301 then 'WBC'
          else null
        end as label
  from labevents l
  inner join ALINE_COHORT co
    on l.subject_id = co.subject_id
    and l.charttime <= co.vent_starttime
    and l.charttime >= co.vent_starttime - interval '1' day
  where l.itemid in
  (
     51300,51301 -- wbc
    ,50811,51222 -- hgb
    ,51265 -- platelet
    ,50824, 50983 -- sodium
    ,50822, 50971 -- potassium
    ,50804 -- Total CO2 or ...
    ,50803, 50882  -- bicarbonate
    ,50806,50902 -- chloride
    ,51006 -- bun
    ,50912 -- creatinine
  )
)
, labs_rn as
(
  select
    subject_id, hadm_id, valuenum, label
    , ROW_NUMBER() over (partition by hadm_id, label order by charttime DESC) as rn
  from labs_preceeding
)
, labs_grp as
(
  select
    subject_id, hadm_id
    , max(case when label = 'BUN' then valuenum else null end) as BUN
    , max(case when label = 'CHLORIDE' then valuenum else null end) as CHLORIDE
    , max(case when label = 'CREATININE' then valuenum else null end) as CREATININE
    , max(case when label = 'HEMOGLOBIN' then valuenum else null end) as HEMOGLOBIN
    , max(case when label = 'PLATELET' then valuenum else null end) as PLATELET
    , max(case when label = 'POTASSIUM' then valuenum else null end) as POTASSIUM
    , max(case when label = 'SODIUM' then valuenum else null end) as SODIUM
    , max(case when label = 'TOTALCO2' then valuenum else null end) as TOTALCO2
    , max(case when label = 'WBC' then valuenum else null end) as WBC

  from labs_rn
  where rn = 1
  group by subject_id, hadm_id
)
select co.subject_id, co.hadm_id
  , lg.bun as bun_first
  , lg.chloride as chloride_first
  , lg.creatinine as creatinine_first
  , lg.HEMOGLOBIN as hgb_first
  , lg.platelet as platelet_first
  , lg.potassium as potassium_first
  , lg.sodium as sodium_first
  , lg.TOTALCO2 as tco2_first
  , lg.wbc as wbc_first

from ALINE_COHORT co
left join labs_grp lg
  on co.hadm_id = lg.hadm_id
