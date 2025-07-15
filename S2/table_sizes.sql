SELECT `database_name`, `table_name`, FORMAT(SUM(`memory_use`) / 1024/1024, 0):>int MB
FROM `information_schema`.`table_statistics`
GROUP BY 1, 2
order by 3 desc;

SELECT `database_name`, `table_name`, 
    FORMAT(SUM(`uncompressed_size`) / 1024/1024, 0):>int `uncompressed_mb`, 
    FORMAT(SUM(`compressed_size`) / 1024/1024, 0):>int `compressed_mb`
FROM `information_schema`.`columnar_segments`
GROUP BY 1, 2
order by 3 desc ;

SELECT
    DATABASE_NAME,
    TABLE_NAME,
    ORDINAL AS PARTITION_ID,
    ROWS,
    MEMORY_USE
FROM INFORMATION_SCHEMA.TABLE_STATISTICS
order by 5 desc;

SELECT
    DATABASE_NAME,
    TABLE_NAME,
    FLOOR(AVG(ROWS)) AS avg_rows,
    ROUND(STDDEV(ROWS)/AVG(ROWS),3) * 100 AS row_skew,
    FLOOR(AVG(MEMORY_USE)) AS avg_memory,
    ROUND(STDDEV(MEMORY_USE)/AVG(MEMORY_USE),3) * 100 AS memory_skew
FROM INFORMATION_SCHEMA.TABLE_STATISTICS
GROUP BY 1, 2
HAVING SUM(ROWS) > 10000
ORDER BY row_skew DESC;


SELECT ROUND(STDDEV(id)/AVG(id),3)*100 AS group_skew,
            PARTITION_ID()
FROM  (SELECT id,
       domain_id,
       count(*)             
       FROM urls      
       GROUP BY 1, 2) sub
GROUP BY PARTITION_ID();

DROP ALL FROM PLANCACHE;