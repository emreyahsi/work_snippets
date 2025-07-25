SELECT WITH(leaf_pushdown=true) SUM(c) rows, PARTITION_ID() 
FROM (SELECT count(*) c FROM SAY_DWH.F_MUH_MUHASEBE GROUP BY MALI_YIL,SKN,ILISKILI_KAYIT_NO) reshuffle GROUP BY PARTITION_ID();

show create table SAY_DWH.F_MUH_TAHAKKUK;
select * from SAY_DWH.F_MUH_MUHASEBE;

SELECT
    DATABASE_NAME,
    TABLE_NAME,
    ORDINAL AS PARTITION_ID,
    ROWS,
    MEMORY_USE
FROM INFORMATION_SCHEMA.TABLE_STATISTICS
WHERE TABLE_NAME = ''
AND DATABASE_NAME='';


SELECT
    DATABASE_NAME,
    TABLE_NAME,
    sum(rows),
    FLOOR(AVG(ROWS)) AS avg_rows,
    MIN(ROWS),
    MAX(ROWS),
    ROUND(STDDEV(ROWS)/AVG(ROWS),3) * 100 AS row_skew,
    FLOOR(AVG(MEMORY_USE)) AS avg_memory,
    ROUND(STDDEV(MEMORY_USE)/AVG(MEMORY_USE),3) * 100 AS memory_skew
FROM INFORMATION_SCHEMA.TABLE_STATISTICS
GROUP BY 1, 2
HAVING SUM(ROWS) > 10000000 AND ROUND(STDDEV(ROWS)/AVG(ROWS),3) * 100>10
ORDER BY row_skew DESC;
