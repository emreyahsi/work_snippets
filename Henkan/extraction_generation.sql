select distinct
'{"template_type":"direct",' ,
'"source":"old_edw",' as source,
'"target":"hive",' as target,
 '"source_table":"'||lower(a.owner)||'.'||lower(table_name)||'",' as source,
'"target_table":"'||lower(a.owner)||'.'||lower(table_name)||'",' as target ,
'"path": "migration/bvas_tables",' as path,
'"depends_on":"'||case when mod(row_number() over (order by a.owner,table_name),round(count(1) over ()/12))=0 then '' else lag(lower(a.owner)||'.'||lower(table_name)) over (order by a.owner,table_name) end||'",' as depends_on,
'"filter":"1=1"'--||case when b.COLUMN_NAME like '%YIL%' then COLUMN_NAME||'=2024 and rownum<20000000"' else '1=1 and rownum<20000000"' end
||',' filter,
'"flow_name": "extr-bvas-big-tables"},' as flow_name
from all_tables a left join dba_part_key_columns b  on a.owner=b.owner and a.table_name=b.name
where 
1=1
and a.owner||'.'||table_name in (
'SAY_DWH_HIST.F_DSR_DIPNOT'
);


--select * from SAY_POC.TABLE_ROW_COUNTS;