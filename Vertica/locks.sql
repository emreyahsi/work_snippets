
SELECT user_name,
       node_names,
       t.session_id,
       object_name,
       lock_mode,
       sysdate- grant_timestamp AS holding_time,
       grant_timestamp-request_timestamp AS wait_to_get
FROM locks l
LEFT JOIN transactions t USING(transaction_id)
ORDER BY 6 DESC;