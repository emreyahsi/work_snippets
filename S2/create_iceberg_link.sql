create link SYBS.iceberg_one_time_link as S3
CONFIG '{"endpoint_url":"http://192",
        "ingest_mode":"one_time",
        "region":"us-west-2",
        "catalog_type": "JDBC",
        "catalog_name": "iceberg",
        "catalog.warehouse": "s3://datalake/iceberg_warehouse",
        "catalog.uri":"jdbc:postgresql://henkan-postgresql.192.168.40.190.sslip.io:5432/icatalog",
        "catalog.jdbc.user": "icatalog",
        "catalog.jdbc.password": "icatalog_p"}'
CREDENTIALS '{"aws_access_key_id" : "1",
             "aws_secret_access_key": "1+cf0d+649F5265aa87ff28DEAI"}';
             