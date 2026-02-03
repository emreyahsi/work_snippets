select
    DISK_SPILLING_B,
    id as process_id,
    plan_id,
    1000 * memory_bs / elapsed_time_ms / 1024 / 1024 / 1024 as avg_memory_used_gb,
    memory_bs / 1024 / 1024 / 1024 memory_usage,
    activity_name,
    json_extract_string(OPTIMIZER_NOTES, 'num_thread_per_leaf') num_threads,
    json_extract_string(OPTIMIZER_NOTES, 'num_connections_per_leaf') num_connections,
    json_extract_string(OPTIMIZER_NOTES, 'num_reshuffles') num_reshuffles,
    num_reshuffles,
    json_extract_string(PLAN_VARIABLES, 'flexible_parallelism_enabled') flexible_parallelism_enabled,
    a.*,
    p.*
from
    information_schema.mv_activities a
    left join information_schema.mv_plancache p using(activity_name)
    left join information_schema.mv_processlist using (plan_id)
where
    activity_type = 'Query' -- and user='henkan'order by 4 desc;show profile json process XX ;