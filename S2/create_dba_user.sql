CREATE ROLE 'dba_role';
GRANT CREATE DATABASE, DROP DATABASE on *.* to ROLE 'dba_role';
-- GRANT RELOAD on *.* to ROLE 'dba_role';
GRANT ALL on *.* to ROLE 'dba_role' with grant option;
GRANT SUPER on *.* to ROLE 'dba_role'with grant option;
GRANT SHOW METADATA on *.* to ROLE 'dba_role' with grant option ;

CREATE GROUP 'dba_users';
GRANT ROLE 'dba_role' to 'dba_users';

GRANT USAGE ON *.* TO 'dba_user' IDENTIFIED BY 'ivMvfhDdCk';
GRANT GROUP 'dba_users' TO 'dba_user';

GRANT USAGE ON *.* TO 'dba_user';

GRANT SHOW METADATA ON *.* TO 'monitor'@'%' IDENTIFIED BY PASSWORD '*18B353919D967164D2590991000708A9E51728BA'


GRANT SHOW METADATA ON *.* TO 'monitor'@'%' IDENTIFIED BY PASSWORD '*18B353919D967164D2590991000708A9E51728BA';
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, INDEX, ALTER, LOCK TABLES, EXECUTE, CREATE VIEW, SHOW VIEW, CREATE ROUTINE, CREATE DATABASE, CREATE PIPELINE, DROP PIPELINE, START PIPELINE, ALTER PIPELINE, SHOW PIPELINE ON `metrics`.* TO monitor
;

CREATE ROLE 'reporting_role';
GRANT SELECT ON *.* TO role 'reporting_role';
grant show metadata on *.* to 'reporting_role';

create group 'reporting_users';
grant role 'reporting_role' to 'reporting_users';

create user '9609' identified by 'K7Ad3xhkk4'; -- ali bakir
create user '8467' identified by 'hIQueSZuGo'; -- gonca kayma
create user '9594' identified by 'hXCAtUW4tG'; -- simge ece aydin
create user '9718' identified by 'uhbTNPFeeI'; -- yasemin ilgaz
;

grant group 'reporting_users' to '9609';
grant group 'reporting_users' to '8467';
grant group 'reporting_users' to '9594';
grant group 'reporting_users' to '9718';

show grants for dba_user;

show groups for user 'dba_user'@'%';

show roles for group 'dba_users';

show grants for role 'dba_role';