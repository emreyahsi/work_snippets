drop link datalake;

CREATE LINK datalake AS S3
CREDENTIALS '{"aws_access_key_id": "PSFBSAZRMGCNBMHHILGJIADHNMCPEMNLJKNFBDBIF", "aws_secret_access_key": "3B8D15DA9bdc4f2c+c572+F54EBC542be1c3dcNOIM"}'
CONFIG '{"endpoint_url":"http://192.168.41.157"}'
DESCRIPTION 'Pure Storage S3 Access Link';