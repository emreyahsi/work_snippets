


CREATE PROCEDURE edw.trim_table_columns(v_schema_name varchar(100), v_table_name varchar(100))
LANGUAGE 'PL/vSQL'
SECURITY INVOKER
AS '
declare

v_columns varchar(65000);
v_temp_statement varchar(65000);
v_statement varchar(65000);
v_statement2 varchar(65000);
v_cols varchar(65000);
v_size int;
c_col_list CURSOR (v_schema_name varchar(100),v_table_name varchar(100)) FOR 
select column_name,
''max(octet_length(''||column_name||''))*1.3+(5-mod(''||''max(octet_length(''||column_name||''))*1.3,5)) ''||column_name||'',''
 from (
select * from columns
where true
and lower(table_name) = lower(v_table_name)
and lower(table_schema)= lower(v_schema_name)
and data_type like ''%varchar%''
) cols
 order by ordinal_position;
 


begin 

v_statement:='''';

FOR v_cols,v_temp_statement IN CURSOR  c_col_list (v_schema_name,v_table_name)

LOOP

v_statement:=v_statement||v_temp_statement;

END LOOP;

v_statement2:=''drop table if exists change_data_types; create local temporary table  change_data_types on commit preserve rows as select '' ||trim(trailing '','' from v_statement)||'' from ''||v_schema_name||''.''||v_table_name;

execute v_statement2;

execute ''commit'';

v_statement:='''';

FOR v_cols,v_temp_statement IN CURSOR  c_col_list (v_schema_name,v_table_name)

LOOP

v_size:= execute ''select coalesce(''||v_cols|| '',-99) from change_data_types'';
v_statement2:=''alter table ''||v_schema_name||''.''||v_table_name||'' alter column ''||v_cols||'' set data type varchar(''||v_size||'');'';
v_statement:=v_statement||chr(10)||v_statement2;
execute ''commit'';



END LOOP;

execute ''drop table if exists trimmed; create local temporary table trimmed on commit preserve rows as select ''''''||v_statement||'''''' as c1'';

 
 end;
 ';

SELECT MARK_DESIGN_KSAFE(1);
