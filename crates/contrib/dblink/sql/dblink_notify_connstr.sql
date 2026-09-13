CREATE EXTENSION dblink;

-- dblink_get_notify() with no unnamed connection: PQconsumeInput/PQnotifies
-- on a NULL conn yield no rows.
SELECT * FROM dblink_get_notify();
SELECT * FROM dblink_get_notify('no_such_conn');

-- dblink_connstr_check: PQconninfoParse rejects an unknown keyword, so the
-- password it carries does not count for a non-superuser.
CREATE ROLE regress_dblink_nsu LOGIN;
GRANT USAGE ON SCHEMA public TO regress_dblink_nsu;
SET ROLE regress_dblink_nsu;
SELECT dblink_connect('password=x nonexistent_dblink_option=y');
SELECT dblink_connect('nonexistent_dblink_option=y');
RESET ROLE;
DROP ROLE regress_dblink_nsu;
