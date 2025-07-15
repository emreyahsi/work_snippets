select time::date dy,
    user_name,
    plan_type,
    min(time) as start_at,
    max(time) - min(time) as span,
    count(distinct session_id) n_session,
    count(
        distinct session_id || transaction_id || statement_id || request_id
    ) n_statement,
    size_pp(sum(network_bytes_received)) network_recv,
    size_pp(sum(network_bytes_sent)) network_sent,
    size_pp(sum(data_bytes_read)) disk_read,
    size_pp(sum(data_bytes_written)) disk_write,
    size_pp(sum(data_bytes_loaded)) disk_load_write,
    size_pp(sum(bytes_spilled)) disk_spill_write,
    sum(input_rows) input_rows,
    sum(input_rows_processed) input_rows_processed
from dc_execution_summaries
group by 1,
    rollup(2, 3)
order by 1,
    2,
    3;