CREATE TABLE t(a int, b text);
INSERT INTO t VALUES (1,'one'),(2,'two'),(3,'three');
CREATE TABLE log(v int);

CREATE FUNCTION f_sum() RETURNS bigint LANGUAGE sql STABLE
    AS $$ SELECT sum(a) FROM t $$;
CREATE FUNCTION f_add(x int, y int) RETURNS int LANGUAGE sql STRICT
    AS $$ SELECT x + y + (SELECT count(*)::int FROM t) $$;
CREATE FUNCTION f_coalesce(x int) RETURNS int LANGUAGE sql
    AS $$ SELECT coalesce(x, -1) + (SELECT count(*)::int FROM t) $$;
CREATE FUNCTION f_ins(v int) RETURNS bigint LANGUAGE sql VOLATILE
    AS $$ INSERT INTO log VALUES (v); INSERT INTO log VALUES (v+1); SELECT count(*) FROM log $$;
CREATE FUNCTION f_void(v int) RETURNS void LANGUAGE sql VOLATILE
    AS $$ INSERT INTO log VALUES (v) $$;
CREATE FUNCTION f_insret(v int) RETURNS int LANGUAGE sql VOLATILE
    AS $$ INSERT INTO log VALUES (v) RETURNING v * 10 $$;
CREATE FUNCTION f_set() RETURNS SETOF int LANGUAGE sql
    AS $$ SELECT a FROM t ORDER BY a $$;
CREATE FUNCTION f_setj() RETURNS SETOF text LANGUAGE sql
    AS $$ SELECT b FROM t ORDER BY a DESC $$;
CREATE TYPE pair AS (x int, y text);
CREATE FUNCTION f_pair() RETURNS pair LANGUAGE sql
    AS $$ SELECT a, b FROM t ORDER BY a LIMIT 1 $$;
CREATE FUNCTION f_pairs() RETURNS SETOF pair LANGUAGE sql
    AS $$ SELECT a, b FROM t ORDER BY a $$;
CREATE FUNCTION f_out(IN v int, OUT d int, OUT s text) LANGUAGE sql
    AS $$ SELECT v*2, 'x' || v FROM t LIMIT 1 $$;
CREATE FUNCTION f_outs(IN v int, OUT d int, OUT s text) RETURNS SETOF record LANGUAGE sql
    AS $$ SELECT a*v, b FROM t ORDER BY a $$;
CREATE FUNCTION f_tab() RETURNS TABLE(x int, y text) LANGUAGE sql
    AS $$ SELECT a, b FROM t ORDER BY a $$;
CREATE FUNCTION f_poly(v anyelement) RETURNS anyelement LANGUAGE sql
    AS $$ SELECT v FROM t LIMIT 1 $$;
CREATE FUNCTION f_coerce() RETURNS bigint LANGUAGE sql
    AS $$ SELECT a FROM t ORDER BY a LIMIT 1 $$;
CREATE FUNCTION f_atomic(x int) RETURNS int LANGUAGE sql
    RETURN x + (SELECT count(*)::int FROM t);
CREATE FUNCTION f_atomic2(v int) RETURNS bigint LANGUAGE sql
    BEGIN ATOMIC INSERT INTO log VALUES (v); SELECT count(*) FROM log; END;
CREATE FUNCTION f_err(v int) RETURNS int LANGUAGE sql VOLATILE
    AS $$ INSERT INTO log VALUES (v); SELECT v / 0 $$;
CREATE FUNCTION f_inline(x int) RETURNS int IMMUTABLE LANGUAGE sql
    AS 'SELECT x + 1';
