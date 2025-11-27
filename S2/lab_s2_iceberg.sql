DROP LINK  emre.iceberg_one_time_link;
CREATE LINK emre.iceberg_one_time_link AS S3
CONFIG '{"endpoint_url":"http://172.12.2.18:9000",
        "ingest_mode":"one_time",
        "region":"us-west-2",
        "catalog_type": "JDBC",
        "catalog_name": "iceberg",
        "catalog.warehouse": "s3://lakestore-dl365/iceberg_warehouse",
        "catalog.uri":"jdbc:postgresql://172.12.2.136:5432/icatalog",
        "catalog.jdbc.user": "icatalog",
        "catalog.jdbc.password": "icatalog_p"}'
CREDENTIALS '{"aws_access_key_id": "GkX2ORNX0xFUj8Epklao", 
              "aws_secret_access_key": "GkX2ORNX0xFUj8Epklao"}';
/*              
access_key: "henkan-dl365"
secret_key: "Gl0bal25!"
iceberg_warehouse_dir: "s3a://lakestore-dl365/iceberg_warehouse"
warehouse_dir: "s3a://lakestore-dl365/warehouse"
*/
;
CREATE TABLE emre.inventory (
  inv_date_sk INT ,
  inv_item_sk INT  ,
  inv_warehouse_sk INT ,
  inv_quantity_on_hand INT 
  );

CALL etl_config.MOVE_TABLE_TO_SINGLESTORE('tpcds_sf100.inventory','emre.inventory'); 

echo etl_config.PIPELINE_GENERATOR('tpcds_sf100.inventory','emre.inventory'); 

show variables like 'java_p%';

set global java_pipelines_java11_path='/usr/bin/java '; 

show variables like 'columnstore_segment_rows%';

select @@java_pipelines_java11_path;


