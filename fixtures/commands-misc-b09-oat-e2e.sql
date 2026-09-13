-- bugs/batch-09-backend-commands-misc: InvokeObjectPost{Create,Alter}Hook
-- sites C reaches from foreigncmds.c (673/842/978/1073/1230/1323/276/418),
-- dbcommands.c (AlterDatabaseRefreshColl, AlterDatabaseOwner) and alter.c
-- AlterObjectOwner_internal (ALTER TABLESPACE OWNER), witnessed through the
-- test_oat_hooks recorder. The C oracle install carries no test modules, so
-- the expected file is captured from pgrust and the NOTICE shape is
-- test_oat_hooks.c's (see crates/contrib/test_oat_hooks).
LOAD 'test_oat_hooks';
CREATE ROLE b09_oat_owner SUPERUSER;
SET allow_in_place_tablespaces = on;
CREATE TABLESPACE b09_oat_ts LOCATION '';
SET test_oat_hooks.audit = on;
CREATE FOREIGN DATA WRAPPER b09_oat_fdw;
ALTER FOREIGN DATA WRAPPER b09_oat_fdw OPTIONS (ADD x '1');
ALTER FOREIGN DATA WRAPPER b09_oat_fdw OWNER TO b09_oat_owner;
CREATE SERVER b09_oat_srv FOREIGN DATA WRAPPER b09_oat_fdw;
ALTER SERVER b09_oat_srv OPTIONS (ADD host 'h');
ALTER SERVER b09_oat_srv OWNER TO b09_oat_owner;
CREATE USER MAPPING FOR postgres SERVER b09_oat_srv;
ALTER USER MAPPING FOR postgres SERVER b09_oat_srv OPTIONS (ADD user 'u');
ALTER TABLESPACE b09_oat_ts OWNER TO postgres;
ALTER DATABASE postgres REFRESH COLLATION VERSION;
ALTER DATABASE postgres OWNER TO postgres;
SET test_oat_hooks.audit = off;
DROP USER MAPPING FOR postgres SERVER b09_oat_srv;
DROP SERVER b09_oat_srv;
DROP FOREIGN DATA WRAPPER b09_oat_fdw;
DROP TABLESPACE b09_oat_ts;
DROP ROLE b09_oat_owner;
