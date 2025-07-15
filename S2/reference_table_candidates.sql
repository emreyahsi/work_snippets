with t_reference as 
(select DATABASE_NAME,TABLE_NAME,(sum(rows)/2):>bigint from table_statistics a   where EXISTS  (
select DATABASE_NAME,TABLE_NAME from table_statistics b where 
 a.database_name=b.database_name and a.table_name=b.table_name
group by 1,2
having sum(rows)/2<20000)
and partition_type<>'Reference'
group by 1,2),
t_usage as 
(select database_name,table_name,sum(QUERY_APPEARANCES) from MV_AGGREGATED_COLUMN_USAGE
group by 1,2
order by 3 desc)
select * from t_reference join t_usage using(database_name,table_name)
order by 4 desc;