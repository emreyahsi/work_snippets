drop event trace Query_completion;

SET GLOBAL trace_events_queue_size = 320*1024*1024;

CREATE EVENT TRACE Query_completion WITH (Query_text = on, Duration_threshold_ms = 1,Trace_all_failed_executions= on);


select * from information_schema.mv_trace_events
where 1=1
-- event_type='Query_completion'
and details like '%say%'
-- and details like '%"success":1%'
order by time desc;


select
json_extract_string(details,'activity_name') activity_name,
case when json_extract_string(details,'error_message') is not null then 1 else 0 end error_flag,
json_extract_string(details,'user_name') user_name,
json_extract_string(details,'start_time') start_time,
(json_extract_string(details,'duration_ms')/1000/60):>decimal(10,2) duration_min,
json_extract_string(details,'plan_id') plan_id,
  AVERAGE_MEMORY_USE/1024/1024/1024*1000 AVERAGE_MEMORY_USE,
  AVERAGE_MAX_MEMORY_USE/1024/1024/1024*1000 AVERAGE_MAX_MEMORY_USE,
json_extract_string(OPTIMIZER_NOTES,'num_thread_per_leaf'):>bigint num_threads,
json_extract_string(OPTIMIZER_NOTES,'num_connections_per_leaf') num_connections,
json_extract_string(OPTIMIZER_NOTES,'num_reshuffles') num_reshuffles,
  json_extract_string(PLAN_VARIABLES,'flexible_parallelism_enabled')  flexible_parallelism_enabled,
  json_extract_string(PLAN_VARIABLES,'sql_select_limit')  sql_select_limit,
json_extract_string(details,'query_text') query_text,
json_extract_string(details,'error_message') error_message,
* from information_schema.mv_trace_events left join plancache on json_extract_string(details,'plan_id') =PLAN_ID
where 1=1
 and json_extract_string(details,'error_message') is  not null
and json_extract_string(details,'user_name') like '%say_reporting%'
-- and json_extract_string(details,'user_name')='henkan'
-- and json_extract_string(details,'start_time'):>date='2025-03-11'
-- and lower(json_extract_string(details,'query_text')) like '%2024-12-31%'
and details not like '%SELECT @@max_allowed_packet, @@aggregator_id%'
order by 4 desc;

select DISK_SPILLING_B, id as process_id,plan_id,1000*memory_bs/elapsed_time_ms/1024/1024/1024 as avg_memory_used_gb,memory_bs/1024/1024/1024 memory_usage,
activity_name,
json_extract_string(OPTIMIZER_NOTES,'num_thread_per_leaf') num_threads,
json_extract_string(OPTIMIZER_NOTES,'num_connections_per_leaf') num_connections,
json_extract_string(OPTIMIZER_NOTES,'num_reshuffles') num_reshuffles,num_reshuffles,
json_extract_string(PLAN_VARIABLES,'flexible_parallelism_enabled')  flexible_parallelism_enabled,
a.*,p.* from information_schema.mv_activities a left join information_schema.plancache p using(activity_name)
left join mv_processlist using (plan_id)
where activity_type='Query'
-- and user='henkan'
order by 4 desc;

SHOW PROFILE JSON plan  6309;

SHOW PROFILE JSON process  18656;

drop 62 from plancache;
