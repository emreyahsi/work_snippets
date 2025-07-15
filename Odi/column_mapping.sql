SELECT
    m.name       AS mapping_name,
    t.table_name AS target_table,
    ma.name      AS target_column,
    me.txt       AS source_column
FROM
         snp_map_expr me
    INNER JOIN snp_map_expr_ref mer ON mer.i_owner_map_expr = me.i_map_expr
    INNER JOIN snp_map_attr     ma ON me.i_owner_map_attr = ma.i_map_attr
    INNER JOIN snp_map_cp       mc ON mer.i_scoping_map_cp = mc.i_map_cp
    INNER JOIN snp_map_comp     mcomp ON mc.i_owner_map_comp = mcomp.i_map_comp
    INNER JOIN snp_mapping      m ON m.i_mapping = mcomp.i_owner_mapping
    INNER JOIN snp_map_ref      mr ON mcomp.i_map_ref = mr.i_map_ref
    INNER JOIN snp_table        t ON mr.i_ref_id = t.i_table
WHERE
    m.name LIKE '%MAP_SAY_DWH_D_MUH_KURUM_HESAP_KOD_STG%';
    
    select * from dev_odı_repo.snp_model;
    
    select * from dev_odı_repo.snp_table;
    
select c.MOD_FOLDER_NAME,b.MOD_FOLDER_NAME,a.mod_name,a.tech_ınt_name,a.lschema_name from dev_odı_repo.snp_model a 
 join dev_odı_repo.snp_mod_folder b  on a.I_MOD_FOLDER=b.I_MOD_FOLDER
left join dev_odı_repo.snp_mod_folder c on b.PAR_I_MOD_FOLDER=c.I_MOD_FOLDER
left join dev_odı_repo.snp_table on I_MOD=;