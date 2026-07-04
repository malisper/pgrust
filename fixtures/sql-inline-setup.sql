-- SQL-function inlining matrix: setup run by C PG 18 (pgrust boots the same
-- datadir and must plan/execute these identically).
CREATE TABLE inl_t (id int PRIMARY KEY, a int, b text);
INSERT INTO inl_t SELECT g, g % 10, 'row' || g FROM generate_series(1, 100) g;
ANALYZE inl_t;

-- Inlinable.
CREATE FUNCTION inl_add(int, int) RETURNS int AS 'SELECT $1 + $2' LANGUAGE sql IMMUTABLE;
CREATE FUNCTION inl_add_named(x int, y int) RETURNS int AS 'SELECT x + y' LANGUAGE sql IMMUTABLE;
CREATE FUNCTION inl_stable(int) RETURNS int AS 'SELECT $1 + 1' LANGUAGE sql STABLE;
CREATE FUNCTION inl_sq(int) RETURNS int AS 'SELECT $1 * $1' LANGUAGE sql IMMUTABLE;
CREATE FUNCTION inl_poly(anyelement, anyelement) RETURNS anyelement
    AS 'SELECT CASE WHEN $1 > $2 THEN $1 ELSE $2 END' LANGUAGE sql STABLE;
CREATE FUNCTION inl_rec(int) RETURNS int
    AS 'SELECT CASE WHEN $1 <= 0 THEN 0 ELSE inl_rec($1 - 1) END' LANGUAGE sql;
CREATE FUNCTION inl_cast(int) RETURNS text AS 'SELECT $1::text' LANGUAGE sql IMMUTABLE;

-- Not inlinable.
CREATE FUNCTION inl_multi(int) RETURNS int AS 'SELECT $1; SELECT $1 + 1' LANGUAGE sql;
CREATE FUNCTION inl_subq(int) RETURNS int AS 'SELECT (SELECT $1)' LANGUAGE sql;
CREATE FUNCTION inl_strict_unused(int, int) RETURNS int AS 'SELECT $1' LANGUAGE sql STRICT;
CREATE FUNCTION inl_agg(int) RETURNS bigint AS 'SELECT sum($1)' LANGUAGE sql;
CREATE FUNCTION inl_imm_volatile_body(int) RETURNS int
    AS 'SELECT $1 + (random() * 0)::int' LANGUAGE sql IMMUTABLE;
CREATE FUNCTION inl_setconf(int) RETURNS int AS 'SELECT $1 + 1' LANGUAGE sql
    SET search_path = public;
