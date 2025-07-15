CREATE  PIPELINE GBM.test_pq_ppl AS
LOAD DATA 
s3 's3://datalake/warehouse/emre.db/test_pq/*.parquet' 
CONFIG '{"endpoint_url":"http://192.168.41.186"}'
CREDENTIALS '{"aws_access_key_id": "", 
              "aws_secret_access_key": "ED1AB706d5342dbc+cf0d+649F5265aa87ff28DEAI"}'
INTO TABLE GBM.test_pq  (
c1 <- c1,
c2 <- c2 )
FORMAT PARQUET;