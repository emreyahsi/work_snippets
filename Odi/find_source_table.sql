select m.ı_mappıng,
    m.name mapping_name,
    mr.qualified_name,
    mc.name datastore_alias,
    t.table_name target_table,
    mdl.cod_mod model_code, cp.direction,ADAPTER_INTF_TYPE
from snp_mapping m inner join snp_map_comp mc on m.i_mapping = mc.i_owner_mapping
    left join snp_map_cp cp on mc.i_map_comp = cp.i_owner_map_comp -- and cp.direction = 'I'
    left join snp_map_ref mr on mc.i_map_ref = mr.i_map_ref
    left join snp_table t on mr.i_ref_id = t.i_table
    left join snp_model mdl on t.i_mod = mdl.i_mod
where m.i_mapping=182 and 
cp.i_map_cp not in
        (select i_start_map_cp from snp_map_conn)
        and ADAPTER_INTF_TYPE='IDataStore' ;
;

select * from snp_map_ref;

select * from snp_map_conn;

select * from snp_mapping where name='MAP_SAY_ODS_MLY_REF_KURUM_HESAP_KOD';

select
    m.name mapping_name,
    mr.qualified_name,
    mc.name datastore_alias,
    t.table_name target_table,
    mdl.cod_mod model_code
from snp_mapping m inner join snp_map_comp mc on m.i_mapping = mc.i_owner_mapping
    inner join snp_map_cp cp on mc.i_map_comp = cp.i_owner_map_comp
    inner join snp_map_ref mr on mc.i_map_ref = mr.i_map_ref
    inner join snp_table t on mr.i_ref_id = t.i_table
    inner join snp_model mdl on t.i_mod = mdl.i_mod
where cp.direction = 'O' and --output connection point
    cp.i_map_cp not in
        (select i_start_map_cp from snp_map_conn) --not a starting connection point
        and m.name='MAP_SAY_ODS_MLY_REF_KURUM_HESAP_KOD'
;