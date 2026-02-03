selectjson_extract_string(details, 'activity_name') activity_name,
case
    when json_extract_string(details, 'error_message') is not null then 1
    else 0
end error_flag,
json_extract_string(details, 'user_name') user_name,
json_extract_string(details, 'start_time') start_time,
(
    json_extract_string(details, 'duration_ms') / 1000 / 60
): > decimal(10, 2) duration_min,
json_extract_string(details, 'plan_id') plan_id,
AVERAGE_MEMORY_USE / 1024 / 1024 / 1024 * 1000 AVERAGE_MEMORY_USE,
AVERAGE_MAX_MEMORY_USE / 1024 / 1024 / 1024 * 1000 AVERAGE_MAX_MEMORY_USE,
json_extract_string(OPTIMIZER_NOTES, 'num_thread_per_leaf'): > bigint num_threads,
json_extract_string(OPTIMIZER_NOTES, 'num_connections_per_leaf') num_connections,
json_extract_string(OPTIMIZER_NOTES, 'num_reshuffles') num_reshuffles,
json_extract_string(PLAN_VARIABLES, 'flexible_parallelism_enabled') flexible_parallelism_enabled,
json_extract_string(PLAN_VARIABLES, 'sql_select_limit') sql_select_limit,
json_extract_string(details, 'query_text') query_text,
json_extract_string(details, 'error_message') error_message,
*
from
    information_schema.mv_trace_events
    left join information_schema.mv_plancache on json_extract_string(details, 'plan_id') = PLAN_IDwhere 1 = 1
    and json_extract_string(details, 'error_message') is not nulland json_extract_string(details, 'user_name') like '%say_reporting%' 
    -- and json_extract_string(details,'user_name')='henkan'
    -- and json_extract_string(details,'start_time'):>date='2025-03-11'
    -- and lower(json_extract_string(details,'query_text')) like '%2024-12-31%'
    and details not like '%SELECT @@max_allowed_packet, @@aggregator_id%'order by 4 desc;