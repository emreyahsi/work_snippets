CREATE or replace TEMPORARY VIEW TEMP_4 
USING org.apache.spark.sql.json OPTIONS ( path "s3a://datalake/external_data/webservice/kps/data",multiLine="true");