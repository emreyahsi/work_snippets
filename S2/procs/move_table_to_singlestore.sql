CREATE TABLE `MOVE_TABLE_TO_SINGLESTORE_LOG` (
    `ID` bigint(20) NOT NULL,
    `SOURCE` varchar(300) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
    `TARGET` varchar(300) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
    `FILTER` varchar(200) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
    `START_TIME` datetime(6) DEFAULT NULL,
    `END_TIME` datetime(6) DEFAULT NULL,
    `ROW_COUNT` bigint(20) DEFAULT NULL,
    PRIMARY KEY (`ID`),
    SHARD KEY `__SHARDKEY` (`ID`),
    SORT KEY `__UNORDERED` ()
) AUTOSTATS_CARDINALITY_MODE = INCREMENTAL AUTOSTATS_HISTOGRAM_MODE = CREATE AUTOSTATS_SAMPLING = ON SQL_MODE = 'PIPES_AS_CONCAT,ANSI_QUOTES,ONLY_FULL_GROUP_BY,STRICT_ALL_TABLES';

CREATE PROCEDURE `ETL_CONFIG`.`MOVE_TABLE_TO_SINGLESTORE`(
    source varchar(300) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    target varchar(300) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    filter varchar(200) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT null
) RETURNS void DEFINER = 'root' @'%' ASdeclare row_cnt bigint;

q_check_row_cnt longtext;

ppl_create longtext;

t_schema_name varchar(200);

t_table_name varchar(200);

s_schema_name varchar(200);

s_table_name varchar(200);

p_id bigint;

BEGINt_schema_name = ETL_CONFIG.SPLIT_PART(target, '.', 1);

t_table_name = ETL_CONFIG.SPLIT_PART(target, '.', 2);

s_schema_name = ETL_CONFIG.SPLIT_PART(source, '.', 1);

s_table_name = ETL_CONFIG.SPLIT_PART(source, '.', 2);

p_id = to_char(current_timestamp(6), 'YYYYMMDDHH24MISSFF3');

INSERT INTO
    ETL_CONFIG.MOVE_TABLE_TO_SINGLESTORE_LOG
values
    (p_id, source, target, filter, now(), null, null);

if filter is null then execute immediate 'create table if not exists ' || t_schema_name || '.rep_' || t_table_name || ' like ' || t_schema_name || '.' || t_table_name;

execute immediate 'DROP PIPELINE IF EXISTS ' || t_schema_name || '.' || t_table_name || '_ppl';

ppl_create = ETL_CONFIG.PIPELINE_GENERATOR(source, target);

execute immediate ppl_create;

execute immediate 'truncate table ' || t_schema_name || '.rep_' || t_table_name;

execute immediate 'start pipeline ' || t_schema_name || '.' || t_table_name || '_ppl foreground';

q_check_row_cnt = 'select count(1) from ' || t_schema_name || '.rep_' || t_table_name;

execute immediate q_check_row_cnt into row_cnt;

if row_cnt > 0 then execute immediate 'drop table if exists ' || t_schema_name || '.rnm_' || t_table_name;

/*restartable olması için eklendi*/
execute immediate 'detach pipeline ' || t_schema_name || '.' || t_table_name || '_ppl';

execute immediate 'alter table ' || t_schema_name || '.' || t_table_name || ' rename to ' || t_schema_name || '.rnm_' || t_table_name;

execute immediate 'alter table ' || t_schema_name || '.rep_' || t_table_name || ' rename to ' || t_schema_name || '.' || t_table_name;

execute immediate 'alter table ' || t_schema_name || '.rnm_' || t_table_name || ' rename to ' || t_schema_name || '.rep_' || t_table_name;

execute immediate 'DROP PIPELINE IF EXISTS ' || t_schema_name || '.' || t_table_name || '_ppl';

execute immediate 'drop table ' || t_schema_name || '.rep_' || t_table_name;

execute immediate 'optimize table ' || t_schema_name || '.' || t_table_name;

else -- raise user_exception ('rep_table is empty please check the data lake table..') ;execute immediate 'DROP PIPELINE IF EXISTS '||t_schema_name||'.'||t_table_name||'_ppl';execute immediate 'drop table '||t_schema_name||'.rep_'||t_table_name;END IF ;else execute immediate 'create table if not exists '||t_schema_name||'.rep_'||t_table_name||' like '||t_schema_name||'.'||t_table_name;execute immediate 'DROP PIPELINE IF EXISTS '||t_schema_name||'.'||t_table_name||'_ppl';ppl_create=ETL_CONFIG.PIPELINE_GENERATOR(source,target);execute immediate ppl_create;execute immediate 'truncate table '||t_schema_name||'.rep_'||t_table_name;execute immediate 'start pipeline '||t_schema_name||'.'||t_table_name||'_ppl foreground';q_check_row_cnt='select count(1) from '||t_schema_name||'.rep_'||t_table_name;execute immediate q_check_row_cnt into row_cnt;if row_cnt>0 then execute immediate 'delete from  '||t_schema_name||'.'||t_table_name||' a where '||filter;execute immediate 'insert into  '||t_schema_name||'.'||t_table_name||' select * from '||t_schema_name||'.rep_'||t_table_name;execute immediate 'commit';execute immediate 'DROP PIPELINE IF EXISTS '||t_schema_name||'.'||t_table_name||'_ppl';execute immediate 'drop table '||t_schema_name||'.rep_'||t_table_name;execute immediate 'optimize table '||t_schema_name||'.'||t_table_name;else -- raise user_exception ('rep_table is empty please check the data lake table..') ;execute immediate 'DROP PIPELINE IF EXISTS '||t_schema_name||'.'||t_table_name||'_ppl';execute immediate 'drop table '||t_schema_name||'.rep_'||t_table_name;END IF ;END IF;UPDATE ETL_CONFIG.MOVE_TABLE_TO_SINGLESTORE_LOG SET END_TIME=NOW(6), ROW_COUNT=row_cnt WHERE ID=p_id;END;