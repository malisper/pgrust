CREATE EXTENSION dblink;
\set SHOW_CONTEXT always

CREATE TABLE foo(f1 int, f2 text, f3 text[], primary key (f1,f2));
INSERT INTO foo VALUES (0,'a','{"a0","b0","c0"}');
INSERT INTO foo VALUES (1,'b','{"a1","b1","c1"}');
INSERT INTO foo VALUES (2,'c','{"a2","b2","c2"}');
INSERT INTO foo VALUES (3,'d','{"a3","b3","c3"}');
INSERT INTO foo VALUES (4,'e','{"a4","b4","c4"}');
INSERT INTO foo VALUES (5,'f','{"a5","b5","c5"}');
INSERT INTO foo VALUES (6,'g','{"a6","b6","c6"}');
INSERT INTO foo VALUES (7,'h','{"a7","b7","c7"}');
INSERT INTO foo VALUES (8,'i','{"a8","b8","c8"}');
INSERT INTO foo VALUES (9,'j','{"a9","b9","c9"}');

-- loopback over the server's own unix socket; engine-agnostic. user= is
-- explicit: the harness initdb's the superuser as 'postgres', so libpq's
-- OS-username default would pick a role that does not exist in-pod.
CREATE FUNCTION connection_parameters() RETURNS text LANGUAGE SQL AS $f$
       SELECT $$host='$$||current_setting('unix_socket_directories')||$$' user='$$||current_user||$$' dbname='$$||current_database()||$$' port=$$||current_setting('port');
$f$;

-- transient-connection query
SELECT *
FROM dblink(connection_parameters(),'SELECT * FROM foo') AS t(a int, b text, c text[])
WHERE t.a > 7;

-- connect failure: the server-sent FATAL rides libpq's host-identity prefix
-- ("connection to server on socket ... failed: FATAL: ...")
SELECT dblink_connect('cfail', $$user=regress_dblink_no_such_user host='$$||current_setting('unix_socket_directories')||$$' dbname='$$||current_database()||$$' port=$$||current_setting('port'));

-- "connection not available"
SELECT *
FROM dblink('SELECT * FROM foo') AS t(a int, b text, c text[])
WHERE t.a > 7;

-- unnamed persistent connection
SELECT dblink_connect(connection_parameters());
SELECT *
FROM dblink('SELECT * FROM foo') AS t(a int, b text, c text[])
WHERE t.a > 7;

-- cursors: bad open with fail=false, abort, the full open/fetch/close cycle
SELECT dblink_open('rmt_foo_cursor','SELECT * FROM foobar',false);
SELECT dblink_exec('ABORT');
SELECT dblink_open('rmt_foo_cursor','SELECT * FROM foo');
SELECT dblink_close('rmt_foo_cursor',false);
SELECT dblink_open('rmt_foo_cursor','SELECT * FROM foo');
SELECT * FROM dblink_fetch('rmt_foo_cursor',4) AS t(a int, b text, c text[]);
SELECT * FROM dblink_fetch('rmt_foo_cursor',4) AS t(a int, b text, c text[]);
SELECT * FROM dblink_fetch('rmt_foo_cursor',4) AS t(a int, b text, c text[]);
SELECT * FROM dblink_fetch('rmt_foobar_cursor',4,false) AS t(a int, b text, c text[]);
SELECT dblink_exec('ABORT');
SELECT dblink_close('rmt_foobar_cursor',false);
SELECT * FROM dblink_fetch('rmt_foo_cursor',4) AS t(a int, b text, c text[]);
SELECT * FROM dblink_fetch('rmt_foo_cursor',4,false) AS t(a int, b text, c text[]);
SELECT dblink_disconnect();

-- disconnected again: not available
SELECT *
FROM dblink('SELECT * FROM foo') AS t(a int, b text, c text[])
WHERE t.a > 7;

-- dblink_exec through transient and persistent conns
SELECT substr(dblink_exec(connection_parameters(),'INSERT INTO foo VALUES(10,''k'',''{"a10","b10","c10"}'')'),1,6);
SELECT dblink_connect(connection_parameters());
SELECT substr(dblink_exec('INSERT INTO foo VALUES(11,''l'',''{"a11","b11","c11"}'')'),1,6);
SELECT * FROM dblink('SELECT * FROM foo') AS t(a int, b text, c text[]);
SELECT * FROM dblink('SELECT * FROM foobar',false) AS t(a int, b text, c text[]);
SELECT dblink_exec('UPDATE foo SET f3[2] = ''b99'' WHERE f1 = 11');
SELECT * FROM dblink('SELECT * FROM foo') AS t(a int, b text, c text[]) WHERE a = 11;
SELECT dblink_exec('UPDATE foobar SET f3[2] = ''b99'' WHERE f1 = 11',false);
SELECT dblink_exec('DELETE FROM foo WHERE f1 = 11');
SELECT dblink_exec('SELECT * FROM foo');
SELECT dblink_disconnect();

-- named connections
SELECT *
FROM dblink('myconn','SELECT * FROM foo') AS t(a int, b text, c text[])
WHERE t.a > 7;
SELECT dblink_connect('myconn',connection_parameters());
SELECT *
FROM dblink('myconn','SELECT * FROM foo') AS t(a int, b text, c text[])
WHERE t.a > 7;
SELECT *
FROM dblink('myconn','SELECT * FROM foobar',false) AS t(a int, b text, c text[])
WHERE t.a > 7;
SELECT dblink_connect('myconn',connection_parameters());
SELECT dblink_connect('myconn2',connection_parameters());
SELECT *
FROM dblink('myconn2','SELECT * FROM foo') AS t(a int, b text, c text[])
WHERE t.a > 7;
SELECT dblink_disconnect('myconn2');

-- cursor transactions on a named conn
SELECT dblink_open('myconn','rmt_foo_cursor','SELECT * FROM foobar',false);
SELECT dblink_exec('myconn','ABORT');
SELECT dblink_exec('myconn','BEGIN');
SELECT dblink_open('myconn','rmt_foo_cursor','SELECT * FROM foo');
SELECT dblink_close('myconn','rmt_foo_cursor');
SELECT dblink_exec('myconn','DECLARE xact_test CURSOR FOR SELECT * FROM foo');
SELECT dblink_exec('myconn','COMMIT');
SELECT dblink_open('myconn','rmt_foo_cursor','SELECT * FROM foo');
SELECT dblink_open('myconn','rmt_foo_cursor2','SELECT * FROM foo');
SELECT dblink_close('myconn','rmt_foo_cursor2');
SELECT dblink_exec('myconn','DECLARE xact_test CURSOR FOR SELECT * FROM foo');
SELECT dblink_close('myconn','rmt_foo_cursor');
SELECT dblink_exec('myconn','DECLARE xact_test CURSOR FOR SELECT * FROM foo');
SELECT dblink_exec('myconn','ABORT');
SELECT dblink_open('myconn','rmt_foo_cursor','SELECT * FROM foo');
SELECT * FROM dblink_fetch('myconn','rmt_foo_cursor',4) AS t(a int, b text, c text[]);
SELECT * FROM dblink_fetch('myconn','rmt_foobar_cursor',4,false) AS t(a int, b text, c text[]);
SELECT dblink_exec('myconn','ABORT');
SELECT * FROM dblink_fetch('myconn','rmt_foo_cursor',4) AS t(a int, b text, c text[]);
SELECT dblink_disconnect('myconn');
SELECT dblink_disconnect('myconn');

-- async family
SELECT dblink_connect('dtest1', connection_parameters());
SELECT * from dblink_send_query('dtest1', 'select * from foo where f1 < 3') as t1;
SELECT dblink_connect('dtest2', connection_parameters());
SELECT * from dblink_send_query('dtest2', 'select * from foo where f1 > 2 and f1 < 7') as t1;
SELECT dblink_connect('dtest3', connection_parameters());
SELECT * from dblink_send_query('dtest3', 'select * from foo where f1 > 6') as t1;

CREATE TEMPORARY TABLE result AS
(SELECT * from dblink_get_result('dtest1') as t1(f1 int, f2 text, f3 text[]))
UNION
(SELECT * from dblink_get_result('dtest2') as t2(f1 int, f2 text, f3 text[]))
UNION
(SELECT * from dblink_get_result('dtest3') as t3(f1 int, f2 text, f3 text[]))
ORDER by f1;

SELECT * FROM unnest(dblink_get_connections()) AS c(name) ORDER BY 1;
SELECT dblink_is_busy('dtest1');
SELECT dblink_disconnect('dtest1');
SELECT dblink_disconnect('dtest2');
SELECT dblink_disconnect('dtest3');
SELECT * from result;

SELECT dblink_connect('dtest1', connection_parameters());
SELECT * from dblink_send_query('dtest1', 'select * from foo where f1 < 3') as t1;
SELECT dblink_cancel_query('dtest1');
SELECT dblink_error_message('dtest1');
SELECT dblink_disconnect('dtest1');

-- foreign-server connections + non-superuser password policy
CREATE ROLE regress_dblink_user;
DO $d$
    BEGIN
        EXECUTE $$CREATE SERVER fdtest FOREIGN DATA WRAPPER dblink_fdw
            OPTIONS (host '$$||current_setting('unix_socket_directories')||$$',
                     dbname '$$||current_database()||$$',
                     port '$$||current_setting('port')||$$'
            )$$;
    END;
$d$;
CREATE USER MAPPING FOR public SERVER fdtest OPTIONS (user 'postgres');
GRANT USAGE ON FOREIGN SERVER fdtest TO regress_dblink_user;
GRANT EXECUTE ON FUNCTION dblink_connect_u(text, text) TO regress_dblink_user;

SET SESSION AUTHORIZATION regress_dblink_user;
-- should fail: no password in connstr for a non-superuser
SELECT dblink_connect('myconn', 'fdtest');
-- should succeed: SECURITY DEFINER bypass
SELECT dblink_connect_u('myconn', 'fdtest');
SELECT * FROM dblink('myconn','SELECT * FROM foo') AS t(a int, b text, c text[])
WHERE t.a > 7;
SELECT dblink_disconnect('myconn');
RESET SESSION AUTHORIZATION;

REVOKE USAGE ON FOREIGN SERVER fdtest FROM regress_dblink_user;
REVOKE EXECUTE ON FUNCTION dblink_connect_u(text, text) FROM regress_dblink_user;
DROP USER regress_dblink_user;
DROP USER MAPPING FOR public SERVER fdtest;
DROP SERVER fdtest;

-- repeated unnamed connect replaces
SELECT dblink_connect(connection_parameters());
SELECT dblink_connect(connection_parameters());
SELECT dblink_disconnect();

-- notifications
SELECT dblink_connect(connection_parameters());
SELECT dblink_exec('LISTEN regression');
SELECT dblink_exec('LISTEN foobar');
SELECT dblink_exec('NOTIFY regression');
SELECT dblink_exec('NOTIFY foobar');
SELECT notify_name, be_pid = (select t.be_pid from dblink('select pg_backend_pid()') as t(be_pid int)) AS is_self_notify, extra from dblink_get_notify();
SELECT * from dblink_get_notify();
SELECT dblink_disconnect();

-- remote GUC mimicry for datatype I/O
SET datestyle = ISO, MDY;
SET intervalstyle = postgres;
SET timezone = UTC;
SELECT dblink_connect('myconn',connection_parameters());
SELECT dblink_exec('myconn', 'SET datestyle = GERMAN, DMY;');

SELECT *
FROM dblink('myconn',
    'SELECT * FROM (VALUES (''12.03.2013 00:00:00+00'')) t')
  AS t(a timestamptz);

SELECT *
FROM dblink('myconn',
    'SELECT * FROM
     (VALUES (''12.03.2013 00:00:00+00''),
             (''12.03.2013 00:00:00+00'')) t')
  AS t(a timestamptz);

SELECT *
FROM dblink_send_query('myconn',
    'SELECT * FROM
     (VALUES (''12.03.2013 00:00:00+00'')) t');
CREATE TEMPORARY TABLE result2 AS
(SELECT * from dblink_get_result('myconn') as t(t timestamptz))
UNION ALL
(SELECT * from dblink_get_result('myconn') as t(t timestamptz));
SELECT * FROM result2;
DROP TABLE result2;

SELECT dblink_exec('myconn', 'SET intervalstyle = sql_standard;');
SELECT *
FROM dblink('myconn',
    'SELECT * FROM (VALUES (''-1 2:03:04'')) i')
  AS i(i interval);

SELECT dblink_exec('myconn', 'SET datestyle = ISO, MDY;');
SELECT *
FROM dblink('myconn',
            'SELECT * FROM (VALUES (''03.12.2013 00:00:00+00'')) t')
  AS t(a timestamptz);
SELECT dblink_exec('myconn', 'SET datestyle = GERMAN, DMY;');
SELECT *
FROM dblink('myconn',
            'SELECT * FROM (VALUES (''12.03.2013 00:00:00+00'')) t')
  AS t(a timestamptz);

-- error mid-conversion inside dblink_fetch
SELECT dblink_open('myconn','error_cursor',
       'SELECT * FROM (VALUES (''1''), (''not an int'')) AS t(text);');
SELECT *
FROM dblink_fetch('myconn','error_cursor', 1) AS t(i int);
SELECT *
FROM dblink_fetch('myconn','error_cursor', 1) AS t(i int);

-- local GUCs kept their values
SHOW datestyle;
SHOW intervalstyle;
SELECT dblink_disconnect('myconn');
RESET datestyle;
RESET intervalstyle;
RESET timezone;
