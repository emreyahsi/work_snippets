set global table_name_case_sensitivity = 0;
set global sql_mode='ANSI';
set global regexp_format=advanced;
set cluster collation_server = 'utf8mb4_bin';
set global columnstore_segment_rows=9999999; -- small segment files oluşmaması için set edildi biz zaten batch data işliyoruz selectivity açısından bir dezavantaj gözlemlemedik küçük tablolara zaten pek bir etkisi yok.
set global optimizer_not_null_filter_derivation=ON;
set global java_pipelines_java11_path='/usr/lib/jvm/java-11-openjdk-11.0.24.0.8-2.el9.x86_64/bin/java'; --indirip kurulmalı
sdb-admin update-config --all --set-global --key "java_pipelines_java11_path" --value "/usr/lib/jvm/java-11-openjdk-11.0.24.0.8-2.el9.x86_64/bin/java"
set global enable_iceberg_ingest=ON;
set data_conversion_compatibility_level=6;

-- 13-05-2025 
set global enable_auto_profile = 1;
set global  spilling_node_memory_threshold_ratio=0.50;

select @@spilling_node_memory_threshold_ratio;
---11-06-2025
select @@maximum_blob_cache_size_percent; -- 0.85
set global maximum_blob_cache_size_percent=0.80;
select @@spilling_maximum_disk_percent; -- -1
set global spilling_maximum_disk_percent=0.15;

select @@spilling_query_operator_memory_threshold; -- 104857600

select @@optimizer_not_null_filter_derivation;

set global columnstore_flush_bytes= 67108864 ;-- 67108864

set global max_autostats_update_workers=4;

set global num_background_merger_threads=8;

select @@num_background_merger_threads;

select 67108864/2; 33554432.0000


select * from information_schema.mv_columnstore_merge_status where table_name like 'rep%%TA%HA%MUH%' and state<>'Idle' ;

select @@columnstore_flush_bytes;
select @@columnstore_small_blob_combination_threshold;

CREATE DATABASE SAY_DWH_HIST ON S3 'singlestore/SAY_DWH_HIST'
CONFIG '{"endpoint_url":"http://192.168.41.157"}'
CREDENTIALS '{"aws_access_key_id": "PSFBSAZRMGCNBMHHILGJIADHNMCPEMNLJKNFBDBIF", 
              "aws_secret_access_key": "3B8D15DA9bdc4f2c+c572+F54EBC542be1c3dcNOIM"}';

;

set global columnstore_flush_bytes=268435456;

select @@columnstore_flush_bytes;

detach database SAY_DWH_HIST;

attach database SAY_DWH_HIST ON S3 'singlestore/'
CONFIG '{"endpoint_url":"http://192.168.41.157"}'
CREDENTIALS '{"aws_access_key_id": "", 
              "aws_secret_access_key": "3B8D15DA9bdc4f2c+c572+F54EBC542be1c3dcNOIM"}';


set global java_pipelines_java11_path='/opt/jdk-11.0.24/bin/java';

set global java_pipelines_java11_path='/opt/jdk-11.0.24';

select @@java_pipelines_java11_path;


show create table spark_catalog.extr_bvas.bel_csv_kurumsal_bilgiler_vs_129
