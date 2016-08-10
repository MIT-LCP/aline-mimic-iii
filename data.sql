
--FINAL QUERY (ish)
select
  co.*
  , so.sofa
  , s1.saps
  , s2.sapsii -- NOTE: wasn't used in original mimic-ii study

  , case when vaso.icustay_id is not null then 1 else 0 end as vaso_flg
  , vaso.vaso_start_day
  , vaso.vaso_free_day
  , vaso.vaso_duration
  , vaso.vaso_1st_3hr_flg
  , vaso.vaso_1st_6hr_flg
  , vaso.vaso_1st_12hr_flg
  , vaso.vaso_b4_aline

  , an.anaesthesia_start_day
  , an.anaesthesia_free_day
  , an.anaesthesia_duration
  , an.anaesthesia_b4_aline

  -- TODO: are individual flags for each anaesthetic necessary?

  , angus.angus as sepsis_flg

  , icd.endocarditis
  , icd.chf
  , icd.afib
  , icd.renal
  , icd.liver
  , icd.copd
  , icd.cad
  , icd.stroke
  , icd.malignancy
  , icd.respfail
  , icd.ards
  , icd.pneumonia

  -- vital sign just preceeding ventilation
  , vi.map
  , vi.temperature
  , vi.heartrate
  , vi.cvp
  , vi.spo2

  -- labs!
  , labs.hct_med
  , labs.hct_lowest
  , labs.hct_highest
  , labs.hematocrit
  , labs.wbc
  , labs.hemoglobin
  , labs.platelet
  , labs.sodium
  , labs.potassium
  , labs.totalco2
  , labs.bun
  , labs.creatinine
  , labs.glucose
  , labs.calcium
  , labs.magnesium
  , labs.phosphate
  , labs.ast
  , labs.alt
  , labs.ldh
  , labs.bilirubin
  , labs.alp
  , labs.albumin
  , labs.tropt
  , labs.ck
  , labs.ntbnp
  , labs.lactate
  , labs.svo2

  -- TODO:
  -- , bg.ph_first
  -- , bg.po2_first
  -- , bg.pco2_first
--
-- , coalesce(abg.abg_count,0) as abg_count
-- , coalesce(vbg.vbg_count,0) as vbg_count
-- , coalesce(abg.abg_count,0)+coalesce(vbg.vbg_count,0) as bg_total

  -- code status flags
  , cs.dnr
  , cs.cmo
  , cs.dncpr
  , cs.dni
  , cs.fullcode
  -- switched from something else to DNR
  , cs.dnr_switch
  -- switched from something else to CMO
  , cs.cmo_switch
  -- switched from something else to either DNR or CMO
  , cs.dnr_cmo_switch


  -- TODO: total fluid balance at the end of day 1, etc
  --   , fluid.fluid_day_1
  --   , fluid.fluid_day_2
  --   , fluid.fluid_day_3
  --   , fluid.fluid_3days_raw
  --   , fluid.fluid_3days_clean
  --   , IV.IV_day_1
  --   , IV.IV_day_2
  --   , IV.IV_day_3
  --   , IV.IV_3days_raw
  --   , IV.IV_3days_clean
from aline_cohort co
left join saps s1
  on co.icustay_id = s1.icustay_id
left join sapsii s2
  on co.icustay_id = s2.icustay_id
left join sofa so
  on co.icustay_id = so.icustay_id
left join aline_vaso vaso
  on co.icustay_id = vaso.icustay_id
left join aline_anaesthesia an
  on co.icustay_id = an.icustay_id
left join angus
  on co.hadm_id = angus.hadm_id
left join aline_icd icd
  on co.hadm_id = icd.hadm_id
left join aline_vitals vi
  on co.hadm_id = vi.hadm_id
left join aline_labs labs
  on co.hadm_id = labs.hadm_id
left join aline_codestatus cs
  on co.icustay_id = cs.icustay_id
where angus.angus = 0 -- no septic patients
and vaso.icustay_id is null -- never given vasopressors in the ICU
order by co.icustay_id;

-- The remaining exclusion criteria are applied in cohort.sql
--  **Angus sepsis
--  **On vasopressors (?is this different than on dobutamine)
