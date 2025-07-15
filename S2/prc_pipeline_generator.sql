CREATE PROCEDURE `ETL_CONFIG`.`PIPELINE_GENERATOR`(source varchar(300) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL, target varchar(300) CHARACTER SET utf8 COLLATE utf8_general_ci NULL) RETURNS text CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFINER = 'root'@'%' AS
declare
cstr text default '';
temp_cstr text;
trn text default '';
temp_trn text;
s_str text ;
t_schema_name varchar(200);
t_table_name  varchar(200);
s_schema_name varchar(200);
s_table_name  varchar(200);
ccolumns query(owner varchar(200),table_name varchar(200),column_name varchar(100), data_type varchar(100))=
select table_schema,TABLE_NAME,'"'||column_name||'"' as column_name,data_type from information_schema.columns 
where 1=1
and table_schema=ETL_CONFIG.SPLIT_PART(target,'.',1)
and table_name=ETL_CONFIG.SPLIT_PART(target,'.',2)
order by ORDINAL_POSITION;

BEGIN

t_schema_name=ETL_CONFIG.SPLIT_PART(target,'.',1);
t_table_name=ETL_CONFIG.SPLIT_PART(target,'.',2);

s_schema_name=ETL_CONFIG.SPLIT_PART(source,'.',1);
s_table_name=ETL_CONFIG.SPLIT_PART(source,'.',2);

cstr=
 /*'DROP PIPELINE IF EXISTS '
            || p_schema_name
            || '.'
            || p_table_name
            || '_ppl;'||char(10) 
            ||*/'CREATE PIPELINE '
            || t_schema_name
            || '.'
            || t_table_name
            || '_ppl '
            || char(10)
            || 'AS LOAD DATA LINK iceberg_one_time_link '
            || ''''||s_schema_name
            || '.'
            || s_table_name||''''
            || char(10)
            || 'INTO TABLE '
            || t_schema_name
            || '.rep_'
            || t_table_name
            || char(10)||'('||char(10);
        

for clist in collect(ccolumns)
loop
if clist.data_type='datetime' then 
temp_cstr='@'||clist.column_name||' <- '||clist.column_name||','||char(10);
temp_trn= clist.column_name||' = DATE_ADD(''1970-01-01'', INTERVAL @'|| clist.column_name||' MICROSECOND)'||','||char(10);
else
temp_cstr=clist.column_name ||' <- '||clist.column_name||','||char(10);
temp_trn='';
end if;
cstr=cstr||temp_cstr;
trn=trn||temp_trn;
end loop;

if trn<>'' then
s_str='SET'||char(10)||trim(trailing','||char(10) from trn)||char(10)||';';
else 
s_str=char(10);
end if ;


cstr = trim(trailing','||char(10) from cstr)||char(10)||')'||char(10)||'FORMAT ICEBERG' ||char(10)||s_str;

 return cstr;

END;