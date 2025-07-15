SELECT anchor_table_schema, 
       anchor_table_name, 
       PROJECTION_NAME,
       SUM(used_bytes) / ( 1024^3 ) AS used_compressed_gb 
FROM   v_monitor.projection_storage 
where projection_name like '%WIFI%'
GROUP  BY anchor_table_schema, 
          anchor_table_name ,
          PROJECTION_NAME
ORDER  BY SUM(used_bytes) DESC;

select node_name
     , size_pp(sum(used_bytes)) as size
from storage_containers 
group by 1 
order by sum(used_bytes);

SELECT schema_name
     , projection_name
     , total
     , n
FROM
(SELECT schema_name
        , projection_name
        , size_pp(sum(used_bytes)) total
        , sum(used_bytes) total_hidden
        , count(*) n
FROM storage_containers 
GROUP BY  1,2) foo 
ORDER BY total_hidden desc limit 10; 