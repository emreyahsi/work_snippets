SELECT * FROM information_schema.PIPELINES_BATCHES_SUMMARY
where 1=1
order by START_TIME desc;

select from_unixtime(BATCH_START_UNIX_TIMESTAMP) ts,* from  information_schema.PIPELINES_BATCHES order by from_unixtime(BATCH_START_UNIX_TIMESTAMP) desc ;

show create table SAY_DWH.rep_F_MUH_MUHASEBE;

ndx_f_muh_muhasebe_hpyev;

select * from information_schema.mv_columnstore_merge_status 
where table_name like '%' and state<>'Idle' ;

select database_name,pipeline_name,
min(from_unixtime(BATCH_START_UNIX_TIMESTAMP)) start_ts,
max(from_unixtime(BATCH_START_UNIX_TIMESTAMP)) end_ts,
sum(BATCH_PARTITION_PARSED_ROWS) total_rows,
timestampdiff(minute,min(from_unixtime(BATCH_START_UNIX_TIMESTAMP)),max(from_unixtime(BATCH_START_UNIX_TIMESTAMP))) dur_sec,
sum(BATCH_PARTITION_PARSED_ROWS) / timestampdiff(SECOND,min(from_unixtime(BATCH_START_UNIX_TIMESTAMP)),current_timestamp) avg_rps,
count(1) run_cnt
from information_schema.PIPELINES_BATCHES
where from_unixtime(BATCH_START_UNIX_TIMESTAMP)>current_timestamp - interval 20 hour
group by 1,2;

8.989.057.145

select from_unixtime(BATCH_START_UNIX_TIMESTAMP),* from information_schema.PIPELINES_BATCHES_SUMMARY order by 1 desc;

select from_unixtime(ERROR_UNIX_TIMESTAMP),a.* from information_schema.PIPELINES_ERRORS a  order by 3 desc;

select pipeline_name,sum(rows_streamed) from information_schema.PIPELINES_BATCHES
where 1=1
  and batch_state='In Progress'
group by 1;


select database_name,pipeline_name,file_state,count(1) from information_schema.PIPELINES_FILES
group by 1,2,3
order by 1,2,3;

select now(),TABLE_NAME,sum(changed) from information_schema.MV_ROW_CHANGE_COUNTS
group by 2
order by 3 desc; 

SELECT sum(row_locks) FROM information_schema.MV_ACTIVE_DISTRIBUTED_TRANSACTIONS;

select * from  information_schema.MV_ROW_CHANGE_COUNTS;


select * from information_schema.MV_ROW_CHANGE_COUNTS
;

CALL ETL_CONFIG.MOVE_TABLE_TO_SINGLESTORE('TEMP.F_MUH_TAHAKKUK_MUHASEBE_02','SAY_DWH.F_MUH_TAHAKKUK_MUHASEBE','MALI_YIL=2025');

