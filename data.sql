
--FINAL QUERY
DROP MATERIALIZED VIEW ALINE_DATA CASCADE;
CREATE MATERIALIZED VIEW ALINE_DATA AS
select
  co.subject_id, co.hadm_id, co.icustay_id

  -- static variables from patient tracking tables
  , co.age
  , co.gender_num
  , co.intime as icustay_intime
  -- , co.day_icu_intime -- day of week, text, commented out as redundant to below
  , co.day_icu_intime_num -- day of week, numeric (0=Sun, 6=Sat)
  , co.hour_icu_intime -- hour of ICU admission (24 hour clock)
  , co.outtime as icustay_outtime

  -- outcome variables
  , co.icu_los_day
  , co.hospital_los_day
  , co.hosp_exp_flg -- 1/0 patient died within current hospital stay
  , co.icu_exp_flg -- 1/0 patient died within current ICU stay
  , co.mort_day -- days from ICU admission to mortality, if they died
  , co.day_28_flg -- 1/0 whether the patient died 28 days after *ICU* admission
  , co.mort_day_censored -- days until patient died *or* 150 days (150 days is our censor time)
  , co.censor_flg -- 1/0 did this patient have 150 imputed in mort_day_censored

  -- aline flags
  -- , co.initial_aline_flg -- always 0, we remove patients admitted w/ aline
  , co.aline_flg -- 1/0 did the patient receive an aline
  , co.aline_time_day -- if the patient received aline, fractional days until aline put in

  -- demographics extracted using regex + echos
  , bmi.weight as weight_first
  , bmi.height as height_first
  , bmi.bmi

  -- service patient was admitted to the ICU under
  , co.service_unit

  -- severity of illness just before ventilation
  , so.sofa as sofa_first

  -- vital sign value just preceeding ventilation
  , vi.map as map_first
  , vi.heartrate as hr_first
  , vi.temperature as temp_first
  , vi.spo2 as spo2_first
  , vi.cvp as cvp_first

  -- labs!
  , labs.bun_first
  , labs.creatinine_first
  , labs.chloride_first
  , labs.hgb_first
  , labs.platelet_first
  , labs.potassium_first
  , labs.sodium_first
  , labs.tco2_first
  , labs.wbc_first

  -- comorbidities extracted using ICD-9 codes
  , icd.chf as chf_flg
  , icd.afib as afib_flg
  , icd.renal as renal_flg
  , icd.liver as liver_flg
  , icd.copd as copd_flg
  , icd.cad as cad_flg
  , icd.stroke as stroke_flg
  , icd.malignancy as mal_flg
  , icd.respfail as resp_flg
  , icd.endocarditis as endocarditis_flg
  , icd.ards as ards_flg
  , icd.pneumonia as pneumonia_flg

from aline_cohort co
left join angus_sepsis angus
  on co.hadm_id = angus.hadm_id
-- The following tables are generated by code within this repository
left join aline_sofa so
on co.icustay_id = so.icustay_id
left join aline_bmi bmi
  on co.icustay_id = bmi.icustay_id
left join aline_vaso_flg vaso_flg
  on co.icustay_id = vaso_flg.icustay_id
left join aline_icd icd
  on co.hadm_id = icd.hadm_id
left join aline_vitals vi
  on co.hadm_id = vi.hadm_id
left join aline_labs labs
  on co.hadm_id = labs.hadm_id
-- Exclusion criteria
where coalesce(angus.angus,0) = 0 -- no septic patients
and coalesce(vaso_flg.vaso_flg,0) = 0 -- never given vasopressors in the ICU
-- The remaining exclusion criteria are applied in cohort.sql
order by co.icustay_id
