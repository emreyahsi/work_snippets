/*
select * from SAY_POC.all_tab_subpartıtıons
where table_name='F_MUH_MUHASEBE' and table_owner='SAY_DWH_HIST'order by 3 asc;
create table say_poc.partition_rc (schema_name varchar(100),table_name varchar(100),partition_name varchar(100),rc int,calc_ts date default sysdate)
drop table say_poc.partition_rc;
*/
declare 
p_name varchar(100);
p_statement varchar(1000);
schema_name varchar(100);
table_name varchar(100);

cursor sp is select * from SAY_POC.all_tab_subpartıtıons
where table_name='F_MUH_MUHASEBE' and table_owner='SAY_DWH_HIST' order by 3 asc;

begin
for pname in sp 
loop
p_statement:='insert into say_poc.partition_rc select '''||pname.table_owner||''','''||pname.table_name||''','''||pname.SUBPARTITION_NAME||''', count(1),sysdate from 
SAY_DWH_HIST.F_MUH_MUHASEBE subpartition('||pname.SUBPARTITION_NAME||')';
execute immediate p_statement;
commit;
end loop;

end;


declare 
p_name varchar(100);
p_statement varchar(1000);
schema_name varchar(100);
table_name varchar(100);

cursor sp is select * from SAY_POC.all_tab_subpartıtıons
where table_name='BEL_BIRLES_VERI_DEFTERI' and table_owner='SAY_ODS' order by 3 asc;

begin
for pname in sp 
loop
p_statement:='insert into say_poc.partition_rc select '''||pname.table_owner||''','''||pname.table_name||''','''||pname.SUBPARTITION_NAME||''', count(1),sysdate from 
SAY_ODS.BEL_BIRLES_VERI_DEFTERI subpartition('||pname.SUBPARTITION_NAME||')';
execute immediate p_statement;
commit;
end loop;

end;

        