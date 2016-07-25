

DROP MATERIALIZED VIEW IF EXISTS ALINE_LABS;
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
            when itemid = 50862 then 'ALBUMIN'
            when itemid = 50878 then 'AST'
            when itemid = 50861 then 'ALT'
            when itemid = 50863 then 'ALP' -- alkaline phosphatase
            when itemid = 50803 then 'TOTALCO2' -- actually is 'BICARBONATE'
            when itemid = 50882 then 'TOTALCO2' -- actually is 'BICARBONATE'
            when itemid = 50804 then 'TOTALCO2'
            when itemid = 50885 then 'BILIRUBIN'
            when itemid = 51006 then 'BUN'
            when itemid = 50808 then 'CALCIUM'
            when itemid = 50912 then 'CREATININE'
            when itemid = 50806 then 'CHLORIDE'
            when itemid = 50902 then 'CHLORIDE'
            when itemid = 50910 then 'CK' -- CK
            when itemid = 50809 then 'GLUCOSE'
            when itemid = 50931 then 'GLUCOSE'
            when itemid = 50810 then 'HEMATOCRIT'
            when itemid = 51221 then 'HEMATOCRIT'
            when itemid = 50811 then 'HEMOGLOBIN'
            when itemid = 51222 then 'HEMOGLOBIN'
            when itemid = 50813 then 'LACTATE'
            when itemid = 50954 then 'LDH' -- lactate dehydrogenase
            when itemid = 50960 then 'MAGNESIUM'
            when itemid = 50963 then 'NTBNP' -- BNP
            when itemid = 50970 then 'PHOSPHATE'
            when itemid = 51265 then 'PLATELET'
            when itemid = 50822 then 'POTASSIUM'
            when itemid = 50971 then 'POTASSIUM'
            when itemid = 50824 then 'SODIUM'
            when itemid = 50983 then 'SODIUM'
            when itemid = 51003 then 'TROPT' -- troponin t
            when itemid = 51300 then 'WBC'
            when itemid = 51301 then 'WBC'
          else null
        end as label
  from labevents l
  inner join ALINE_COHORT co
    on l.subject_id = co.subject_id
    and l.charttime <= co.vent_starttime
    and l.charttime >= co.vent_starttime - interval '7' day
  where l.itemid in
  (
     50810,51221 -- hcrit
    ,51300,51301 -- wbc
    ,50811,51222 -- hgb
    ,51265 -- platelet
    ,50824, 50983 -- sodium
    ,50822, 50971 -- potassium
    ,50804 -- Total CO2 or ...
    ,50803, 50882  -- bicarbonate
    ,50806,50902 -- chloride
    ,51006 -- bun
    ,50912 -- creatinine
    ,50809,50931 -- glucose
    ,50808 -- calcium
    ,50960 -- magnesium
    ,50970 -- phosphate
    ,50878 -- ast
    ,50861 -- alt
    ,50863 -- alkaline phosphatase
    -- 51241 | LEUKOCYTE ALKALINE PHOSPHATASE ???
    ,50954 -- lactate dehydrogenase
    ,50885 -- bili
    ,50862 -- albumin
    ,51003 -- troponin t
    ,50910 -- CK
    ,50963 -- BNP
    ,50813 -- lactate
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
    , max(case when label = 'ALBUMIN' then valuenum else null end) as ALBUMIN
    , max(case when label = 'AST' then valuenum else null end) as AST
    , max(case when label = 'ALT' then valuenum else null end) as ALT
    , max(case when label = 'ALP' then valuenum else null end) as ALP
    , max(case when label = 'TOTALCO2' then valuenum else null end) as TOTALCO2
    , max(case when label = 'BILIRUBIN' then valuenum else null end) as BILIRUBIN
    , max(case when label = 'BUN' then valuenum else null end) as BUN
    , max(case when label = 'CALCIUM' then valuenum else null end) as CALCIUM
    , max(case when label = 'CREATININE' then valuenum else null end) as CREATININE
    , max(case when label = 'CHLORIDE' then valuenum else null end) as CHLORIDE
    , max(case when label = 'CK' then valuenum else null end) as CK
    , max(case when label = 'GLUCOSE' then valuenum else null end) as GLUCOSE
    , max(case when label = 'HEMATOCRIT' then valuenum else null end) as HEMATOCRIT
    , max(case when label = 'HEMOGLOBIN' then valuenum else null end) as HEMOGLOBIN
    , max(case when label = 'LACTATE' then valuenum else null end) as LACTATE
    , max(case when label = 'LDH' then valuenum else null end) as LDH
    , max(case when label = 'MAGNESIUM' then valuenum else null end) as MAGNESIUM
    , max(case when label = 'NTBNP' then valuenum else null end) as NTBNP
    , max(case when label = 'PHOSPHATE' then valuenum else null end) as PHOSPHATE
    , max(case when label = 'PLATELET' then valuenum else null end) as PLATELET
    , max(case when label = 'POTASSIUM' then valuenum else null end) as POTASSIUM
    , max(case when label = 'SODIUM' then valuenum else null end) as SODIUM
    , max(case when label = 'TROPT' then valuenum else null end) as TROPT
    , max(case when label = 'WBC' then valuenum else null end) as WBC
  from labs_rn
  where rn = 1
  group by subject_id, hadm_id
)
-- OTHER LABS
, hct as
(
  select subject_id, hadm_id
  , percentile_cont(0.5) WITHIN GROUP (ORDER BY valuenum) as hct_med
  , min(valuenum) as HCT_lowest
  , max(valuenum) as HCT_highest
  from labs_rn
  where label = 'HEMATOCRIT'
  group by subject_id, hadm_id
)
-- TODO: last arterial blood gas preceeding
-- ,50820 -- pH
-- ,50821 -- po2
-- ,50818 -- pco2
-- when itemid = 50820 then 'PH' -- pH
-- when itemid = 50821 then 'PO2' -- po2
-- when itemid = 50818 then 'PCO2' -- pco2

-- labs from charts
, labs_from_chart as
(
  select co.subject_id, co.hadm_id
  , valuenum
  , ROW_NUMBER() over (partition by co.hadm_id order by charttime DESC) as rn
  from chartevents ce
  inner join ALINE_COHORT co
    on ce.subject_id = co.subject_id
    and ce.charttime <= co.vent_starttime
    and ce.charttime >= co.vent_starttime - interval '7' day
  where itemid in
  (
    664,--	Swan SVO2
    838,--	SvO2
    223772 --	SvO2
  )
  and valuenum is not null
)
-- combine labs
select co.subject_id, co.hadm_id
  -- labs
  -- , coalesce(abg.abg_count,0) as abg_count
  -- , coalesce(vbg.vbg_count,0) as vbg_count
  -- , coalesce(abg.abg_count,0)+coalesce(vbg.vbg_count,0) as bg_total
  , hct.hct_med
  , hct.hct_lowest
  , hct.hct_highest

  , lg.HEMATOCRIT
  , lg.wbc
  , lg.HEMOGLOBIN
  , lg.platelet
  , lg.sodium
  , lg.potassium
  , lg.TOTALCO2
  , lg.bun
  , lg.creatinine
  , lg.glucose
  , lg.calcium
  , lg.magnesium
  , lg.phosphate
  , lg.AST
  , lg.ALT
  , lg.LDH
  , lg.bilirubin
  , lg.ALP
  , lg.albumin
  , lg.tropt
  , lg.CK
  , lg.NTBNP
  , lg.lactate
  , lc.valuenum as svo2

  -- , ph.ph
  -- , po2.po2
  -- , pco2.pco2
from ALINE_COHORT co
left join labs_grp lg
  on co.hadm_id = lg.hadm_id
left join hct
  on co.hadm_id = hct.hadm_id
left join labs_from_chart lc
  on co.hadm_id = lc.hadm_id
  and lc.rn = 1
