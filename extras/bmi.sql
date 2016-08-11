
DROP MATERIALIZED VIEW IF EXISTS ALINE_BMI;
CREATE MATERIALIZED VIEW ALINE_BMI as

with ce_wt as
(
    SELECT
      co.icustay_id
      -- we take the median value from their stay
      -- TODO: eliminate obvious outliers if there is a reasonable weight
      -- (e.g. weight of 180kg and 90kg would remove 180kg instead of taking the median)
      , percentile_cont(0.5) WITHIN GROUP (ORDER BY valuenum) as Weight_Admit
    FROM aline_cohort co
    inner join chartevents c
        on c.subject_id = co.subject_id
        and c.charttime between co.vent_starttime - interval '1' day and co.vent_starttime
    WHERE c.valuenum IS NOT NULL
    AND c.itemid in (762,226512) -- Admit Wt
    AND c.valuenum != 0
    group by co.icustay_id
)
, dwt as
(
    SELECT
      co.icustay_id
      , percentile_cont(0.5) WITHIN GROUP (ORDER BY valuenum) as Weight_Daily
    FROM aline_cohort co
    inner join chartevents c
        on c.subject_id = co.subject_id
        and c.charttime between co.vent_starttime - interval '1' day and co.vent_starttime
    WHERE c.valuenum IS NOT NULL
    AND c.itemid in (763,224639) -- Daily Weight
    AND c.valuenum != 0
    group by co.icustay_id
)
, ce_ht0 as
(
    SELECT
      co.icustay_id
      , case
        -- convert inches to centimetres
          when itemid in (920, 1394, 4187, 3486)
              then valuenum * 2.54
            else valuenum
        end as Height
    FROM aline_cohort co
    inner join chartevents c
        on c.subject_id = co.subject_id
        and c.charttime <= co.outtime
    WHERE c.valuenum IS NOT NULL
    AND c.itemid in (226730,920, 1394, 4187, 3486,3485,4188) -- height
    AND c.valuenum != 0
)
, ce_ht as
(
    SELECT
        icustay_id
        -- extract the median height from the chart to add robustness against outliers
        , percentile_cont(0.5) WITHIN GROUP (ORDER BY height) as Height_chart
    from ce_ht0
    group by icustay_id
)
, echo as
(
    select icustay_id
        , 2.54*height_first as height_first
        , 0.453592*weight_first as weight_first
    from aline_echodata ec
)
, bmi as
(
select
    co.icustay_id
    -- weight in kg
    , round(cast(
          coalesce(ce_wt.Weight_Admit, dwt.Weight_Daily, ec.weight_first)
        as numeric), 2)
    as Weight

    -- height in metres
    , coalesce(ce_ht.Height_chart, ec.height_first)/100.0 as Height

    -- components
    , ce_wt.Weight_Admit
    , dwt.Weight_Daily
    , ec.Weight_first
    , ec.Height_first

from aline_cohort co

-- admission weight
left join ce_wt
    on co.icustay_id = ce_wt.icustay_id

-- daily weights
left join dwt
    on co.icustay_id = dwt.icustay_id

-- height
left join ce_ht
    on co.icustay_id = ce_ht.icustay_id

-- echo data
left join echo ec
    on co.subject_id = ec.icustay_id
)
select
    icustay_id
    , case
        when weight is not null and height is not null
            then (weight / (height*height))
        else null
    end as BMI
    , height
    , weight
from bmi
order by icustay_id;
