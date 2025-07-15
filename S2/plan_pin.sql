SET GLOBAL enable_plan_pinning = OFF;

drop database s2_plan_metadata;

BOOTSTRAP PLAN PIN DATABASE;

drop link cluster.plan_pinning_link;

CREATE LINK cluster.plan_pinning_link AS MYSQL 
CONFIG '{
    "host": "192.168.40.173","port":3306}' 
CREDENTIALS '{
    "username":"root","password":"!"}';
    
SET GLOBAL enable_plan_pinning = ON;