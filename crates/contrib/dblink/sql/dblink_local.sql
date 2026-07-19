CREATE EXTENSION dblink;

-- local catalog helpers: no connection involved
CREATE TABLE foo(f1 int, f2 text, f3 text[], primary key (f1,f2));
INSERT INTO foo VALUES (0,'a','{"a0","b0","c0"}');
INSERT INTO foo VALUES (1,'b','{"a1","b1","c1"}');
INSERT INTO foo VALUES (2,'c','{"a2","b2","c2"}');

SELECT * FROM dblink_get_pkey('foo');

SELECT dblink_build_sql_insert('foo','1 2',2,'{"0", "a"}','{"99", "xyz"}');
SELECT dblink_build_sql_insert('foo','1 2 3 4',4,'{"0", "a", "{a0,b0,c0}"}','{"99", "xyz", "{za0,zb0,zc0}"}');

SELECT dblink_build_sql_update('foo','1 2',2,'{"0", "a"}','{"99", "xyz"}');
SELECT dblink_build_sql_update('foo','1 2 3 4',4,'{"0", "a", "{a0,b0,c0}"}','{"99", "xyz", "{za0,zb0,zc0}"}');

SELECT dblink_build_sql_delete('foo','1 2',2,'{"0", "a"}');
SELECT dblink_build_sql_delete('foo','1 2 3 4',4,'{"0", "a", "{a0,b0,c0}"}');

-- quoted, schema-qualified
CREATE SCHEMA "MySchema";
CREATE TABLE "MySchema"."Foo"(f1 int, f2 text, f3 text[], primary key (f1,f2));
INSERT INTO "MySchema"."Foo" VALUES (0,'a','{"a0","b0","c0"}');

SELECT * FROM dblink_get_pkey('"MySchema"."Foo"');
SELECT dblink_build_sql_insert('"MySchema"."Foo"','1 2',2,'{"0", "a"}','{"99", "xyz"}');
SELECT dblink_build_sql_update('"MySchema"."Foo"','1 2',2,'{"0", "a"}','{"99", "xyz"}');
SELECT dblink_build_sql_delete('"MySchema"."Foo"','1 2',2,'{"0", "a"}');

-- pk-with-included-columns: only key columns count
CREATE TABLE foo_1(f1 int, f2 text, f3 text[], primary key (f1,f2) include (f3));
INSERT INTO foo_1 VALUES (0,'a','{"a0","b0","c0"}');
SELECT * FROM dblink_get_pkey('foo_1');
SELECT dblink_build_sql_insert('foo_1','1 2',2,'{"0", "a"}','{"99", "xyz"}');
DROP TABLE foo_1;

-- dropped columns in build_sql_*
CREATE TEMP TABLE test_dropped
(
	col1 INT NOT NULL DEFAULT 111,
	id SERIAL PRIMARY KEY,
	col2 INT NOT NULL DEFAULT 112,
	col2b INT NOT NULL DEFAULT 113
);
INSERT INTO test_dropped VALUES(default);
ALTER TABLE test_dropped
	DROP COLUMN col1,
	DROP COLUMN col2,
	ADD COLUMN col3 VARCHAR(10) NOT NULL DEFAULT 'foo',
	ADD COLUMN col4 INT NOT NULL DEFAULT 42;

SELECT dblink_build_sql_insert('test_dropped', '1', 1,
                               ARRAY['1'::TEXT], ARRAY['2'::TEXT]);
SELECT dblink_build_sql_update('test_dropped', '1', 1,
                               ARRAY['1'::TEXT], ARRAY['2'::TEXT]);
SELECT dblink_build_sql_delete('test_dropped', '1', 1,
                               ARRAY['2'::TEXT]);

-- current_query alias
SELECT dblink_current_query();

-- fdw validator: option-context rules, oauth rejection, closest-match hints
CREATE SERVER fdtest_opts FOREIGN DATA WRAPPER dblink_fdw
    OPTIONS (dbname 'contrib_regression', port '5432');
CREATE USER MAPPING FOR public SERVER fdtest_opts
  OPTIONS (server 'localhost');  -- fail, can't specify server here
CREATE USER MAPPING FOR public SERVER fdtest_opts OPTIONS (user 'postgres');
ALTER SERVER fdtest_opts OPTIONS (ADD oauth_issuer 'https://example.com');
ALTER SERVER fdtest_opts OPTIONS (ADD oauth_client_id 'myID');
ALTER USER MAPPING FOR public SERVER fdtest_opts
	OPTIONS (ADD oauth_issuer 'https://example.com');
ALTER USER MAPPING FOR public SERVER fdtest_opts
	OPTIONS (ADD oauth_client_id 'myID');
ALTER SERVER fdtest_opts OPTIONS (ADD password 'sekret');  -- fail, um-only
ALTER USER MAPPING FOR public SERVER fdtest_opts OPTIONS (ADD passwordd 'x');  -- fail + hint
ALTER FOREIGN DATA WRAPPER dblink_fdw OPTIONS (nonexistent 'fdw');  -- fail
DROP USER MAPPING FOR public SERVER fdtest_opts;
DROP SERVER fdtest_opts;
