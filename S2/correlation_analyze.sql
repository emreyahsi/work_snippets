SELECT *,
       sqrt(chi2 / (total * least(k-1,r-1)))
FROM(
SELECT sum(observed_frequency) as total,
       sum(power((observed_frequency - ((row_total * column_total)/total)),2)/((row_total * 
column_total)/total)) as chi2,
       count(distinct car_brand) as k,
       count(distinct car_model) as r
FROM(
SELECT *,
       sum(observed_frequency) OVER(PARTITION BY car_brand) AS row_total,
       sum(observed_frequency) OVER(PARTITION BY car_model) AS column_total,
       sum(observed_frequency) OVER() as total
from(
SELECT 
    GEC_BAS car_brand,
    GEC_BIT car_model,
    COUNT(*) AS observed_frequency
FROM 
    SAY_DWH.D_BRD_OZEL_HIZMET a
GROUP BY 
    1,
    2)));