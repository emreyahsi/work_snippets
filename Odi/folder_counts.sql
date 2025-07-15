SELECT 
    proj.I_PROJECT AS PROJECT_ID,
    proj.PROJECT_NAME AS PROJECT_NAME,
    fold.I_FOLDER AS FOLDER_ID,
    "Path",
    fold.FOLDER_NAME AS FOLDER_NAME,
    map.I_MAPPING AS MAPPING_ID,
    map.NAME AS MAPPING_NAME,lv
FROM 
    SNP_PROJECT proj
JOIN 
   (SELECT I_FOLDER,I_PROJECT,FOLDER_NAME, CONNECT_BY_ISCYCLE "Cycle",
   LEVEL as lv, SYS_CONNECT_BY_PATH(FOLDER_NAME, '/') "Path"
   FROM SNP_FOLDER
   WHERE 1=1 --level <= 3 
   START WITH PAR_I_FOLDER is null
   CONNECT BY NOCYCLE PRIOR I_FOLDER = PAR_I_FOLDER) fold ON proj.I_PROJECT = fold.I_PROJECT
JOIN 
    SNP_MAPPING map ON fold.I_FOLDER = map.I_FOLDER
    where PROJECT_NAME='SAYISTAY_BI'
    order by 4;

select pt,count(1) from (SELECT 
    proj.I_PROJECT AS PROJECT_ID,
    proj.PROJECT_NAME AS PROJECT_NAME,
    fold.I_FOLDER AS FOLDER_ID,
    "Path" pt,
    fold.FOLDER_NAME AS FOLDER_NAME,
    map.TRT_NAME AS MAPPING_NAME,lv
FROM 
    SNP_PROJECT proj
JOIN 
   (SELECT I_FOLDER,I_PROJECT,FOLDER_NAME, CONNECT_BY_ISCYCLE "Cycle",
   LEVEL as lv, SYS_CONNECT_BY_PATH(FOLDER_NAME, '/') "Path"
   FROM SNP_FOLDER
   WHERE 1=1 --level <= 3 
   START WITH PAR_I_FOLDER is null
   CONNECT BY NOCYCLE PRIOR I_FOLDER = PAR_I_FOLDER) fold ON proj.I_PROJECT = fold.I_PROJECT
JOIN 
    SNP_TRT map ON fold.I_FOLDER = map.I_FOLDER
    where PROJECT_NAME='SAYISTAY_BI'
    and "Path" not like '%ESKI%' and TRT_NAME not like 'Copy%'
    order by 4,6)
    group by pt;
    
    
select * from snp_trt where trt_type='U'