SELECT 'ANALYZE TABLE '||database_name||'.'||table_name||' columns '||group_concat(distinct column_name)||' enable;'
 from information_schema.mv_query_prospective_histograms
 where database_name<>'information_schema'
 group by database_name,table_name;