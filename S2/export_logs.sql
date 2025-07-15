-- exports 

select * from information_schema.PIPELINES
INTO s3 'datalake/ss_export/pipelines.csv' 
CONFIG '{"endpoint_url":"http://192.168.41.186"}'
CREDENTIALS '{"aws_access_key_id": "PSFBSAZREANMDBBAGFMIOMPLNMLNCEDFNGAHLOHJM", 
              "aws_secret_access_key": "ED1AB706d5342dbc+cf0d+649F5265aa87ff28DEAI"}'
              ENABLE_OVERWRITE
          ;
          
select * from information_schema.PIPELINES_ERRORS
INTO s3 'datalake/ss_export/pipelines_errors.csv' 
CONFIG '{"endpoint_url":"http://192.168.41.186"}'
CREDENTIALS '{"aws_access_key_id": "PSFBSAZREANMDBBAGFMIOMPLNMLNCEDFNGAHLOHJM", 
              "aws_secret_access_key": "ED1AB706d5342dbc+cf0d+649F5265aa87ff28DEAI"}'
              ENABLE_OVERWRITE
          ;
select * from information_schema.PIPELINES_BATCHES
INTO s3 'datalake/ss_export/pipelines_batches.csv' 
CONFIG '{"endpoint_url":"http://192.168.41.186"}'
CREDENTIALS '{"aws_access_key_id": "PSFBSAZREANMDBBAGFMIOMPLNMLNCEDFNGAHLOHJM", 
              "aws_secret_access_key": "ED1AB706d5342dbc+cf0d+649F5265aa87ff28DEAI"}'
              ENABLE_OVERWRITE
          ;
          
select * from information_schema.PIPELINES_BATCHES_SUMMARY
INTO s3 'datalake/ss_export/pipelines_batches_summary.csv' 
CONFIG '{"endpoint_url":"http://192.168.41.186"}'
CREDENTIALS '{"aws_access_key_id": "PSFBSAZREANMDBBAGFMIOMPLNMLNCEDFNGAHLOHJM", 
              "aws_secret_access_key": "ED1AB706d5342dbc+cf0d+649F5265aa87ff28DEAI"}'
              ENABLE_OVERWRITE
          ;
          
 192.168.40.178 --> rclone copy  ss://datalake/ss_export/ ./ss_exports