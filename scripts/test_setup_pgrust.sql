--
-- pgrust regression bootstrap.
--
-- Keep the shared fixture data aligned with upstream PostgreSQL test_setup.sql
-- so regression diffs reflect SQL behavior instead of undersized sample data.
-- Skip the upstream LANGUAGE C helper section and replace it with pgrust-safe
-- helpers below.
--

-- directory paths are passed to us in environment variables
\getenv abs_srcdir PG_ABS_SRCDIR

SET synchronous_commit = on;

GRANT ALL ON SCHEMA public TO public;

SET allow_in_place_tablespaces = true;
CREATE TABLESPACE regress_tblspace LOCATION '';

CREATE TABLE CHAR_TBL(f1 char(4));
INSERT INTO CHAR_TBL (f1) VALUES
  ('a'),
  ('ab'),
  ('abcd'),
  ('abcd    ');
VACUUM CHAR_TBL;

CREATE TABLE FLOAT8_TBL(f1 float8);
INSERT INTO FLOAT8_TBL(f1) VALUES
  ('0.0'),
  ('-34.84'),
  ('-1004.30'),
  ('-1.2345678901234e+200'),
  ('-1.2345678901234e-200');
VACUUM FLOAT8_TBL;

CREATE TABLE INT2_TBL(f1 int2);
INSERT INTO INT2_TBL(f1) VALUES
  ('0   '),
  ('  1234 '),
  ('    -1234'),
  ('32767'),
  ('-32767');
VACUUM INT2_TBL;

CREATE TABLE INT4_TBL(f1 int4);
INSERT INTO INT4_TBL(f1) VALUES
  ('   0  '),
  ('123456     '),
  ('    -123456'),
  ('2147483647'),
  ('-2147483647');
VACUUM INT4_TBL;

CREATE TABLE INT8_TBL(q1 int8, q2 int8);
INSERT INTO INT8_TBL VALUES
  ('  123   ','  456'),
  ('123   ','4567890123456789'),
  ('4567890123456789','123'),
  (+4567890123456789,'4567890123456789'),
  ('+4567890123456789','-4567890123456789');
VACUUM INT8_TBL;

CREATE TABLE POINT_TBL(f1 point);
INSERT INTO POINT_TBL(f1) VALUES
  ('(0.0,0.0)'),
  ('(-10.0,0.0)'),
  ('(-3.0,4.0)'),
  ('(5.1, 34.5)'),
  ('(-5.0,-12.0)'),
  ('(1e-300,-1e-300)'),
  ('(1e+300,Inf)'),
  ('(Inf,1e+300)'),
  (' ( Nan , NaN ) '),
  ('10.0,10.0');
-- We intentionally don't vacuum point_tbl here; geometry depends on that.

CREATE TABLE TEXT_TBL (f1 text);
INSERT INTO TEXT_TBL VALUES
  ('doh!'),
  ('hi de ho neighbor');
VACUUM TEXT_TBL;

CREATE TABLE VARCHAR_TBL(f1 varchar(4));
INSERT INTO VARCHAR_TBL (f1) VALUES
  ('a'),
  ('ab'),
  ('abcd'),
  ('abcd    ');
VACUUM VARCHAR_TBL;

CREATE TABLE onek (
  unique1     int4,
  unique2     int4,
  two         int4,
  four        int4,
  ten         int4,
  twenty      int4,
  hundred     int4,
  thousand    int4,
  twothousand int4,
  fivethous   int4,
  tenthous    int4,
  odd         int4,
  even        int4,
  stringu1    name,
  stringu2    name,
  string4     name
);
\set filename :abs_srcdir '/data/onek.data'
COPY onek FROM :'filename';
VACUUM ANALYZE onek;

CREATE TABLE onek2 AS SELECT * FROM onek;
VACUUM ANALYZE onek2;

CREATE TABLE tenk1 (
  unique1     int4,
  unique2     int4,
  two         int4,
  four        int4,
  ten         int4,
  twenty      int4,
  hundred     int4,
  thousand    int4,
  twothousand int4,
  fivethous   int4,
  tenthous    int4,
  odd         int4,
  even        int4,
  stringu1    name,
  stringu2    name,
  string4     name
);
\set filename :abs_srcdir '/data/tenk.data'
COPY tenk1 FROM :'filename';
VACUUM ANALYZE tenk1;

CREATE TABLE tenk2 AS SELECT * FROM tenk1;
VACUUM ANALYZE tenk2;

CREATE TABLE person (
  name     text,
  age      int4,
  location point
);
\set filename :abs_srcdir '/data/person.data'
COPY person FROM :'filename';
VACUUM ANALYZE person;

CREATE TABLE emp (
  salary  int4,
  manager name
) INHERITS (person);
\set filename :abs_srcdir '/data/emp.data'
COPY emp FROM :'filename';
VACUUM ANALYZE emp;

CREATE TABLE student (
  gpa float8
) INHERITS (person);
\set filename :abs_srcdir '/data/student.data'
COPY student FROM :'filename';
VACUUM ANALYZE student;

CREATE TABLE stud_emp (
  percent int4
) INHERITS (emp, student);
\set filename :abs_srcdir '/data/stud_emp.data'
COPY stud_emp FROM :'filename';
VACUUM ANALYZE stud_emp;

CREATE TABLE road (
  name    text,
  thepath path
);
\set filename :abs_srcdir '/data/streets.data'
COPY road FROM :'filename';
VACUUM ANALYZE road;

CREATE TABLE ihighway () INHERITS (road);
INSERT INTO ihighway
  SELECT *
  FROM ONLY road
  WHERE name ~ 'I- .*';
VACUUM ANALYZE ihighway;

CREATE TABLE shighway (
  surface text
) INHERITS (road);
INSERT INTO shighway
  SELECT *, 'asphalt'
  FROM ONLY road
  WHERE name ~ 'State Hwy.*';
VACUUM ANALYZE shighway;

create type stoplight as enum ('red', 'yellow', 'green');

create type float8range as range (subtype = float8, subtype_diff = float8mi);
create type textrange as range (subtype = text, collation = "C");

--
-- Regression helper functions pgrust can express today.
--

CREATE FUNCTION binary_coercible(source oid, target oid)
RETURNS bool
LANGUAGE plpgsql
AS $$
BEGIN
  RETURN pg_rust_internal_binary_coercible(source, target);
END
$$;

CREATE FUNCTION fipshash(input bytea)
RETURNS text
LANGUAGE plpgsql
AS $$
BEGIN
  RETURN substr(encode(sha256(input), 'hex'), 1, 32);
END
$$;

CREATE FUNCTION fipshash(input text)
RETURNS text
LANGUAGE plpgsql
AS $$
BEGIN
  RETURN substr(encode(sha256(input::bytea), 'hex'), 1, 32);
END
$$;

--
-- Shared index-capable fixtures for pgrust regression coverage.
--

CREATE TABLE pgrust_index_tbl (
  id int4 not null,
  bucket int4 not null,
  payload text
);
INSERT INTO pgrust_index_tbl (id, bucket, payload) VALUES
  (1::int4, 1::int4, 'alpha'),
  (2::int4, 1::int4, 'beta'),
  (3::int4, 2::int4, 'gamma'),
  (4::int4, 2::int4, 'delta'),
  (5::int4, 3::int4, 'epsilon'),
  (6::int4, 3::int4, 'zeta'),
  (7::int4, 4::int4, 'eta'),
  (8::int4, 4::int4, 'theta');

CREATE INDEX pgrust_index_tbl_id_idx ON pgrust_index_tbl (id);
CREATE INDEX pgrust_index_tbl_bucket_id_idx ON pgrust_index_tbl (bucket, id);

CREATE TABLE pgrust_unique_tbl (
  id int4,
  note text
);
INSERT INTO pgrust_unique_tbl (id, note) VALUES
  (1::int4, 'one'),
  (2::int4, 'two'),
  (3::int4, 'three'),
  (NULL, 'null-a'),
  (NULL, 'null-b');

CREATE UNIQUE INDEX pgrust_unique_tbl_id_key ON pgrust_unique_tbl (id);

--
-- Keep explicit ANALYZE targets until bare ANALYZE can finish the full
-- bootstrap fixture set reliably on this branch.
--
ANALYZE onek;
ANALYZE onek2;
ANALYZE tenk1;
ANALYZE tenk2;
ANALYZE person;
ANALYZE emp;
ANALYZE student;
ANALYZE stud_emp;
ANALYZE road;
ANALYZE ihighway;
ANALYZE shighway;
ANALYZE pgrust_index_tbl;
ANALYZE pgrust_unique_tbl;
