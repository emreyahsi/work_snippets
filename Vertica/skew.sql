/**
  Desc: Skew projection analysis.
  Author: husnu.sensoy@globalmaksimum.com
  DML: No
  DDL: Yes
  Notes:
    Orginal script is taken from https://www.vertica.com/blog/identifying-projection-skew/ and
    modified accordingly.
*/
CREATE LOCAL TEMPORARY TABLE IF NOT EXISTS projection_storage_tmp ON COMMIT PRESERVE ROWS as
SELECT projection_schema ,
       projection_name ,
       node_name ,
       projection_id,
       sum(used_bytes) AS used_bytes
FROM projection_storage
GROUP BY 1,
         2,
         3,
         4
ORDER BY projection_schema,
         projection_name,
         node_name
SEGMENTED BY HASH(projection_schema, projection_name, node_name) ALL NODES;

select analyze_statistics('projection_storage_tmp');

drop table skewed_projections_tmp;
CREATE LOCAL TEMPORARY TABLE skewed_projections_tmp ON COMMIT PRESERVE ROWS as
SELECT DISTINCT ps.projection_schema
      , ps.projection_name
      --, ps.node_name
      , ps.projection_id
      , first_value(size_pp(used_bytes)) OVER (w ORDER BY used_bytes ASC) AS min_used_MB
      , first_value(size_pp(used_bytes)) OVER (w ORDER BY used_bytes DESC) AS max_used_MB
      , to_char((first_value(used_bytes) OVER (w ORDER BY used_bytes ASC) / first_value(used_bytes) OVER (w ORDER BY used_bytes DESC)-1) *-1*100, '99.9999%') AS skew_pct
FROM
projection_storage_tmp AS ps
JOIN projections p USING (projection_id)
WHERE p.is_segmented
  AND ps.used_bytes > 500000000    -- 500 MB per node
WINDOW w AS (PARTITION BY ps.projection_schema,ps.projection_name)
ORDER BY 6 DESC
LIMIT 30 ;

/**
weblog            | _extr_binden_categoryMeta_v1_b0                | 45036008063354014 | 1.66 GB     | 18.75 GB    |  91.1235%
 weblog            | _extr_binden_categoryMeta_v1_b1                | 45036008063354028 | 1.67 GB     | 18.75 GB    |  91.1143%
 records           | NativeAdDeliveryNew_b0                         | 58546800238563662 | 34.88 GB    | 318.28 GB   |  89.0402%
 records           | NativeAdDeliveryNew_b1                         | 58546800238563750 | 34.88 GB    | 318.27 GB   |  89.0398%
 weblog            | _extr_binden_searchResultMeta_v1_b1            | 45036008063346118 | 28.02 GB    | 135.41 GB   |  79.3098%
 weblog            | _extr_binden_searchResultMeta_v1_b0            | 45036008063346104 | 28.02 GB    | 135.41 GB   |  79.3085%
 **/

select * from skewed_projections_tmp ORDER BY 6 DESC;

-- How much storage per node consumed due to this skewness ?
SELECT node_name,
       size_pp(sum(used_bytes))
FROM projection_storage_tmp
WHERE projection_id IN (45036008063354014,
                        45036008063354028,
                        58546800238563662,
                        58546800238563750,
                        45036008063346118,
                        45036008063346104)
GROUP BY node_name
ORDER BY node_name;


-- % of issue can be solved by modifying the relevant projection.
SELECT projection_schema,
       projection_name,
       node_name,
       projection_id,
       size_pp(used_bytes),
       used_bytes/sum(used_bytes) over(PARTITION BY node_name)
FROM projection_storage_tmp
WHERE projection_id IN (45036008063354014,
                        45036008063354028,
                        58546800238563662,
                        58546800238563750,
                        45036008063346118,
                        45036008063346104)
  AND node_name IN ('v_vertica_node0002',
                    'v_vertica_node0003')
ORDER BY node_name,
         6 desc
         ;