WITH obj1 as(
SELECT i_package i_instance, i_folder, NULL i_project, pack_name obj_name,3200 obj_type, 'Package' obj_type_name, last_date, last_user FROM SNP_PACKAGE
 UNION ALL
SELECT i_mapping i_instance, i_folder, NULL i_project, name obj_name,3100 obj_type, 'Interface' obj_type_name, last_date, last_user FROM SNP_MAPPING
 UNION ALL
SELECT i_trt i_instance, i_folder, i_project, trt_name obj_name,3600 obj_type, CASE trt_type WHEN 'U' THEN 'Procedure' ELSE 'Knowledge Module' END obj_type_name, last_date, last_user FROM SNP_TRT
 UNION ALL
SELECT i_var i_instance,  NULL i_folder, i_project, var_name obj_name, 3500 obj_type, 'Variable' obj_type_name, last_date, last_user FROM SNP_VAR t
 UNION ALL
SELECT i_table i_instance, i_sub_model i_folder, i_mod i_project, table_name obj_name, 2400 obj_type, 'Table' obj_type_name, last_date, last_user from snp_table t
),
obj as (select project_name,i_instance,Pathh,folder_name,obj_type_name,obj_name,a.i_folder,a.last_date,a.last_user,a.i_project from obj1 a left  join  (SELECT I_FOLDER,I_PROJECT,FOLDER_NAME, CONNECT_BY_ISCYCLE "Cycle",
   LEVEL as lv, SYS_CONNECT_BY_PATH(FOLDER_NAME, '/') Pathh
   FROM SNP_FOLDER
   WHERE 1=1 --level <= 3 
   START WITH PAR_I_FOLDER is null
   CONNECT BY NOCYCLE PRIOR I_FOLDER = PAR_I_FOLDER) b  on a.i_folder=b.I_FOLDER left join SNP_PROJECT p on b.ı_project=p.i_project )
,fd (i_folder, i_project, folder_name, folder_path, lv) AS(
SELECT i_folder, i_project, folder_name, folder_name folder_path, 1 lv
  FROM snp_folder
 WHERE par_i_folder IS NULL
 UNION ALL
SELECT tf.i_folder, tf.i_project, tf.folder_name, fd.folder_path||''||tf.folder_name, fd.lv+1
  FROM snp_folder tf JOIN fd
    ON fd.i_folder = tf.par_i_folder
)
,mpl as (
SELECT sm.i_smod i_mc, 'sm' typemc, COALESCE(sm.i_smod_parent,sm.i_mod) i_mp, NVL2(sm.i_smod_parent,'sm','m') typemp, sm.smod_name name --, m.i_mod_folder
  FROM snp_sub_model sm
 UNION ALL
SELECT i_mod, 'm' typ, i_mod_folder, 'mf', mod_name
  FROM snp_model m
 UNION ALL
SELECT i_mod_folder, 'mf', par_i_mod_folder, 'mf', mod_folder_name FROM snp_mod_folder
)
,mp (i_mc, typemc, i_mp, typemp, model_tech, model_path, lv) AS(
SELECT i_mc, typemc, i_mp, typemp, name tname, name model_path, 1 lv
  FROM mpl
 WHERE i_mp IS NULL
 UNION ALL
SELECT mpl.i_mc, mpl.typemc, mpl.i_mp, mpl.typemp, mp.model_tech, mp.model_path||''||mpl.name model_path, mp.lv+1 lv
  FROM mpl JOIN mp
    ON mpl.i_mp = mp.i_mc AND mpl.typemp=mp.typemc
)
SELECT distinct  ag.last_date,obj.i_instance,p.project_name,pathh, OBJ_NAME, CASE WHEN COALESCE(p.project_name,mp.model_tech) IS NULL THEN 'Global ' || OBJ_TYPE_NAME ELSE OBJ_TYPE_NAME END obj_type_name
      ,obj.last_date
      ,obj.last_user
     -- ,COALESCE(project_name,mp.model_tech) project_model
      ,COALESCE(fd.folder_path,mp.model_path) path
      
  FROM obj
  LEFT OUTER
  JOIN fd
    ON fd.i_folder = obj.i_folder AND obj_type_name!='Table'
  LEFT OUTER
  JOIN mp
    ON mp.i_mc = obj.i_folder AND obj_type_name='Table'
  LEFT OUTER
  JOIN snp_project p
    ON p.i_project = COALESCE(obj.i_project, fd.i_project)
  LEFT OUTER
  JOIN snp_model m
    ON m.i_mod = obj.i_project
    left join snp_plan_agent ag on ag.scen_name=obj.obj_name and stat_plan not in ('I','D')
    where obj_name  like '%ETLUSER_BVASKURUM_TURU%'
    order by 4 
;

snp_plan_agent; -- schedules;


select * from snp_plan_agent where stat_plan not in ('I','D')
order by 2 ;

select * from snp_var_plan_agent;

SELECT i_table i_instance, i_sub_model i_folder, i_mod i_project, table_name obj_name, 2400 obj_type, 'Table' obj_type_name, last_date, last_user from snp_table t;

select * from snp_scen join snp_plan_agent using(scen_name);

select * from snp_scen where scen_no=969
group by scen_name
order by 2 desc;

SELECT *FROM SNP_MAPPING where name='ETLUSER_BVASKURUM_TURU';


(select project_name,i_instance,Pathh,folder_name,obj_type_name,obj_name,a.i_folder,a.last_date,a.last_user,a.i_project from obj1 a left  join  (SELECT I_FOLDER,I_PROJECT,FOLDER_NAME, CONNECT_BY_ISCYCLE "Cycle",
   LEVEL as lv, SYS_CONNECT_BY_PATH(FOLDER_NAME, '/') Pathh
   FROM SNP_FOLDER
   WHERE 1=1 --level <= 3 
   START WITH PAR_I_FOLDER is null
   CONNECT BY NOCYCLE PRIOR I_FOLDER = PAR_I_FOLDER) b  on a.i_folder=b.I_FOLDER left join SNP_PROJECT p on b.ı_project=p.i_project )