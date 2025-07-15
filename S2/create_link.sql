drop link datalake;

CREATE LINK datalake AS S3
CREDENTIALS '{"aws_access_key_id": "", "aws_secret_access_key": ""}'
CONFIG '{"endpoint_url":"http://192"}'
DESCRIPTION 'Pure Storage S3 Access Link';