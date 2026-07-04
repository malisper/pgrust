CREATE TABLE t1 (
    a int4 NOT NULL,
    b int4 DEFAULT 42,
    c text DEFAULT 'abc',
    d numeric(10,2) DEFAULT 0.00,
    e int8,
    CONSTRAINT t1_pk PRIMARY KEY (a),
    CONSTRAINT t1_b_check CHECK (b > 0 AND c <> 'forbidden'),
    CONSTRAINT t1_b_e_uniq UNIQUE (b, e)
);
CREATE TABLE t2 (
    a int4 NOT NULL,
    f int4,
    g text,
    CONSTRAINT t2_fk FOREIGN KEY (a) REFERENCES t1 (a) ON DELETE CASCADE
);
CREATE INDEX t1_b_idx ON t1 (b);
CREATE INDEX t1_multi_idx ON t1 (b DESC, e NULLS FIRST);
CREATE UNIQUE INDEX t1_e_uidx ON t1 (e);
CREATE INDEX t1_incl_idx ON t1 (b) INCLUDE (c);

CREATE VIEW v_simple AS SELECT a, b FROM t1;
CREATE VIEW v_alias AS SELECT a AS x, b + 1 AS bplus, c FROM t1;
CREATE VIEW v_where AS SELECT a FROM t1 WHERE (b > 1 AND c = 'y') OR NOT (e IS NULL);
CREATE VIEW v_join AS SELECT t1.a, t2.g FROM t1 JOIN t2 ON t1.a = t2.a WHERE t2.f > 0;
CREATE VIEW v_ljoin AS SELECT t1.a, t2.f FROM t1 LEFT JOIN t2 ON t1.a = t2.a;
CREATE VIEW v_group AS SELECT b, count(*) AS n, sum(e) AS tot FROM t1 GROUP BY b HAVING count(*) > 1;
CREATE VIEW v_order AS SELECT a, b FROM t1 ORDER BY b DESC, a LIMIT 10 OFFSET 2;
CREATE VIEW v_distinct AS SELECT DISTINCT b FROM t1;
CREATE VIEW v_setop AS SELECT a FROM t1 UNION ALL SELECT a FROM t2;
CREATE VIEW v_setop2 AS SELECT b FROM t1 INTERSECT SELECT f FROM t2;
CREATE VIEW v_case AS SELECT CASE WHEN b > 10 THEN 'big' WHEN b > 1 THEN 'mid' ELSE 'small' END AS sz FROM t1;
CREATE VIEW v_case_arg AS SELECT CASE b WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'many' END AS w FROM t1;
CREATE VIEW v_coalesce AS SELECT COALESCE(e, 0) AS e0, GREATEST(a, b) AS mx, LEAST(a, b) AS mn FROM t1;
CREATE VIEW v_sublink AS SELECT a FROM t1 WHERE a IN (SELECT a FROM t2) AND EXISTS (SELECT 1 FROM t2 WHERE t2.f = t1.b);
CREATE VIEW v_saop AS SELECT a FROM t1 WHERE b = ANY (ARRAY[1, 2, 3]);
CREATE VIEW v_cast AS SELECT e::int4 AS ei, a::int8 AS al, c::varchar(10) AS cv FROM t1;
CREATE VIEW v_subq AS SELECT x.a, x.bb FROM (SELECT a, b + 1 AS bb FROM t1) x WHERE x.a > 0;

CREATE TABLE rule_log (l int4, m int4, note text);

CREATE VIEW v_cte AS WITH s AS (SELECT a, b FROM t1 WHERE b > 0) SELECT s.a FROM s WHERE s.b < 10;
CREATE VIEW v_cte_rec AS WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 5) SELECT n FROM r;
CREATE VIEW v_cte_mat AS WITH m AS MATERIALIZED (SELECT a FROM t1), nm AS NOT MATERIALIZED (SELECT f FROM t2) SELECT m.a, nm.f FROM m, nm;
CREATE VIEW v_cte_alias AS WITH s(x, y) AS (SELECT a, b FROM t1) SELECT s.x, s.y FROM s;
CREATE VIEW v_window AS SELECT a, sum(b) OVER (PARTITION BY c ORDER BY a) AS s, row_number() OVER () AS rn FROM t1;
CREATE VIEW v_window_named AS SELECT a, rank() OVER w AS r, count(*) OVER w2 AS c2 FROM t1 WINDOW w AS (ORDER BY b), w2 AS (PARTITION BY b ORDER BY a DESC);
CREATE VIEW v_window_ref AS SELECT sum(b) OVER w2 AS s FROM t1 WINDOW w AS (PARTITION BY b), w2 AS (w ORDER BY a);
CREATE VIEW v_frame AS SELECT sum(b) OVER (ORDER BY a ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS s1, avg(e) OVER (ORDER BY a RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS s2, count(*) OVER (ORDER BY a GROUPS BETWEEN CURRENT ROW AND 2 FOLLOWING EXCLUDE TIES) AS s3 FROM t1;
CREATE VIEW v_values AS SELECT v.i, v.t FROM (VALUES (1, 'x'), (2, 'y')) v(i, t);
CREATE VIEW v_bare_values AS VALUES (1, 'x'), (2, 'y');
CREATE VIEW v_lateral AS SELECT t1.a, x.bb FROM t1, LATERAL (SELECT t1.b + 1 AS bb) x;
CREATE VIEW v_lateral_fn AS SELECT t1.a, g.g FROM t1, LATERAL generate_series(1, t1.a) g(g);
CREATE VIEW v_srf AS SELECT g.n FROM generate_series(1, 3) g(n);
CREATE VIEW v_unnest AS SELECT u.x FROM unnest(ARRAY[1, 2, 3]) u(x);
CREATE VIEW v_ordset AS SELECT percentile_cont(0.5::double precision) WITHIN GROUP (ORDER BY b) AS med, percentile_cont(0.25::double precision) WITHIN GROUP (ORDER BY e) AS q1 FROM t1;
CREATE VIEW v_collate AS SELECT c COLLATE "C" AS cc FROM t1 WHERE c > ('a' COLLATE "POSIX");
CREATE VIEW v_ties AS SELECT a FROM t1 ORDER BY b FETCH FIRST 3 ROWS WITH TIES;

CREATE RULE r_t2_ins AS ON INSERT TO t2 DO ALSO INSERT INTO rule_log VALUES (new.a, new.f, 'ins');
CREATE RULE r_t2_upd AS ON UPDATE TO t2 WHERE old.f > 0 DO ALSO UPDATE t1 SET b = t1.b + 1 WHERE t1.a = old.a;
CREATE RULE r_t2_del AS ON DELETE TO t2 DO INSTEAD NOTHING;
CREATE RULE r_t1_multi AS ON UPDATE TO t1 DO ALSO (INSERT INTO rule_log VALUES (old.a, new.b, 'multi'); DELETE FROM rule_log WHERE rule_log.l = old.a);
CREATE RULE r_t1_inssel AS ON INSERT TO t1 WHERE new.b > 100 DO ALSO INSERT INTO rule_log SELECT new.a, new.b, 'big';
CREATE RULE r_t1_delwhere AS ON DELETE TO t1 WHERE old.a > 10 DO ALSO DELETE FROM t2 WHERE t2.a = old.a;
CREATE RULE r_t1_defvals AS ON INSERT TO t1 WHERE new.b = 0 DO ALSO INSERT INTO rule_log DEFAULT VALUES;

CREATE FUNCTION f_add(x integer, y integer DEFAULT 1) RETURNS integer LANGUAGE plpgsql IMMUTABLE STRICT AS $$ begin return x + y; end $$;
CREATE FUNCTION f_sql(x integer) RETURNS SETOF integer LANGUAGE sql STABLE COST 500 ROWS 100 AS $$ SELECT x $$;
CREATE FUNCTION f_out(IN a integer, OUT b integer, OUT c text) LANGUAGE sql AS $$ SELECT a, 'z'::text $$;
CREATE FUNCTION f_secdef() RETURNS integer LANGUAGE sql SECURITY DEFINER SET search_path TO public, pg_temp SET work_mem = '4MB' AS $$ SELECT 1 $$;
CREATE PROCEDURE p_noop(IN x integer) LANGUAGE plpgsql AS $$ begin end $$;

CREATE VIEW v_isdistinct AS SELECT a IS DISTINCT FROM b AS d, e IS NOT DISTINCT FROM 1.5 AS nd FROM t1;
CREATE VIEW v_booltest AS SELECT (a > 0) IS TRUE AS bt, (b < 0) IS NOT FALSE AS bnf, (a = b) IS UNKNOWN AS bu FROM t1;
CREATE VIEW v_subscript AS SELECT (ARRAY[s.a, s.b])[1] AS one, s.arr[2] AS two, s.arr[1:2] AS slice FROM (SELECT a, b, ARRAY[a, b, a] AS arr FROM t1) s;
CREATE VIEW v_svf AS SELECT CURRENT_DATE AS d, CURRENT_TIMESTAMP(2) AS ts2, LOCALTIMESTAMP AS lts, LOCALTIME(1) AS lt1, CURRENT_ROLE AS cr, SESSION_USER AS su, CURRENT_SCHEMA AS cs;

CREATE VIEW v_search_cycle AS WITH RECURSIVE sc(id, pid) AS (SELECT 1, 0 UNION ALL SELECT sc.id + 1, sc.id FROM sc WHERE sc.id < 3) SEARCH DEPTH FIRST BY id SET ord CYCLE id SET is_c USING pth SELECT sc.id FROM sc;
CREATE VIEW v_search_breadth AS WITH RECURSIVE sb(id) AS (SELECT 1 UNION ALL SELECT sb.id + 1 FROM sb WHERE sb.id < 3) SEARCH BREADTH FIRST BY id SET o SELECT sb.id FROM sb;
CREATE VIEW v_cycle_marks AS WITH RECURSIVE cm(id) AS (SELECT 1 UNION ALL SELECT cm.id + 1 FROM cm WHERE cm.id < 3) CYCLE id SET mark TO 'y' DEFAULT 'n' USING pth SELECT cm.id FROM cm;
CREATE FUNCTION f_variadic(VARIADIC xs int4[]) RETURNS int4 LANGUAGE sql IMMUTABLE AS $$ SELECT 3 $$;
CREATE FUNCTION f_named(a int4, b text DEFAULT 'z') RETURNS text LANGUAGE sql IMMUTABLE AS $$ SELECT b || a $$;
CREATE VIEW v_variadic AS SELECT f_variadic(VARIADIC ARRAY[1, 2]) AS v, f_named(a => 1, b => 'y') AS n, concat(VARIADIC ARRAY['a', 'b']) AS c;
CREATE VIEW v_sqlsyntax AS SELECT EXTRACT(epoch FROM ts.t) AS ep, SUBSTRING(t1.c FROM 2 FOR 1) AS s1, SUBSTRING(t1.c SIMILAR 'a' ESCAPE '#') AS s2, (ts.t AT TIME ZONE 'UTC') AS tz, TRIM(BOTH 'x' FROM t1.c) AS tb, TRIM(LEADING FROM t1.c) AS tl, TRIM(TRAILING 'z' FROM t1.c) AS tt, POSITION(('b') IN (t1.c)) AS p, OVERLAY(t1.c PLACING 'z' FROM 2 FOR 1) AS o, NORMALIZE(t1.c, NFC) AS nn, (t1.c IS NFD NORMALIZED) AS isn, pg_collation_for(t1.c) AS cf, SYSTEM_USER AS su FROM t1, (SELECT now() AS t) ts;
CREATE VIEW v_overlaps AS SELECT (ts.t, ts.t) OVERLAPS (ts.t, ts.t) AS ov FROM (SELECT now() AS t) ts;
CREATE VIEW v_atlocal AS SELECT (ts.t AT LOCAL) AS tl FROM (SELECT now()::timestamp AS t) ts;

CREATE RULE r_merge AS ON UPDATE TO t2 DO ALSO (WITH mm AS (MERGE INTO t1 USING t2 s ON t1.a = s.a WHEN MATCHED AND s.f > 0 THEN UPDATE SET b = s.f WHEN MATCHED THEN DO NOTHING WHEN NOT MATCHED THEN INSERT (a, b) VALUES (s.a, s.f) RETURNING t1.a) SELECT 1);
CREATE RULE r_merge_src AS ON UPDATE TO t2 DO ALSO (WITH mm AS (MERGE INTO t1 USING t2 s ON t1.a = s.a WHEN NOT MATCHED BY SOURCE THEN DELETE WHEN NOT MATCHED BY TARGET THEN INSERT DEFAULT VALUES RETURNING t1.a) SELECT 1);

CREATE FUNCTION f_trig() RETURNS trigger LANGUAGE plpgsql AS $$ begin return new; end $$;
CREATE TRIGGER trg_row BEFORE INSERT OR UPDATE OF b, a OR DELETE ON t1 FOR EACH ROW WHEN (pg_trigger_depth() = 0) EXECUTE FUNCTION f_trig();
CREATE TRIGGER trg_stmt AFTER UPDATE ON t1 REFERENCING OLD TABLE AS ot NEW TABLE AS nt FOR EACH STATEMENT EXECUTE FUNCTION f_trig('a', 'b''c');
CREATE TRIGGER trg_instead INSTEAD OF INSERT ON v_simple FOR EACH ROW EXECUTE FUNCTION f_trig();
CREATE TRIGGER trg_trunc AFTER TRUNCATE ON t2 FOR EACH STATEMENT EXECUTE FUNCTION f_trig();
CREATE TRIGGER trg_when BEFORE UPDATE ON t1 FOR EACH ROW WHEN (old.b IS DISTINCT FROM new.b OR new.c = 'x') EXECUTE FUNCTION f_trig();
CREATE CONSTRAINT TRIGGER trg_constr AFTER INSERT ON t2 FROM t1 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION f_trig();

CREATE TABLE conf_t (a int4 PRIMARY KEY, b int4, c text, CONSTRAINT conf_t_b_uq UNIQUE (b));
CREATE UNIQUE INDEX conf_t_c_pat_uidx ON conf_t (c text_pattern_ops);
CREATE UNIQUE INDEX conf_t_partial_uidx ON conf_t (c) WHERE b > 0 AND c <> 'nope';
CREATE RULE r_conf_nothing AS ON INSERT TO t2 DO ALSO INSERT INTO conf_t VALUES (new.a, new.f, 'x') ON CONFLICT DO NOTHING;
CREATE RULE r_conf_arbiter AS ON INSERT TO t2 DO ALSO INSERT INTO conf_t VALUES (new.a, new.f, 'x') ON CONFLICT (a) DO NOTHING;
CREATE RULE r_conf_upd AS ON INSERT TO t2 DO ALSO INSERT INTO conf_t VALUES (new.a, new.f, 'y') ON CONFLICT (a) DO UPDATE SET b = excluded.b + 1, c = conf_t.c || 'z' WHERE conf_t.b < 10;
CREATE RULE r_conf_constr AS ON INSERT TO t2 DO ALSO INSERT INTO conf_t VALUES (new.a, new.f, 'w') ON CONFLICT ON CONSTRAINT conf_t_b_uq DO UPDATE SET c = 'dup';
CREATE RULE r_conf_opclass AS ON INSERT TO t2 DO ALSO INSERT INTO conf_t VALUES (new.a, new.f, 'p') ON CONFLICT (c text_pattern_ops) DO NOTHING;
CREATE RULE r_conf_where AS ON INSERT TO t2 DO ALSO INSERT INTO conf_t VALUES (new.a, new.f, 'q') ON CONFLICT (c) WHERE b > 0 AND c <> 'nope' DO NOTHING;

CREATE TABLE excl_t (x int4, y int4, CONSTRAINT excl_xy EXCLUDE USING btree (x WITH =, y WITH =) DEFERRABLE INITIALLY DEFERRED);

CREATE VIEW v_nullif AS SELECT NULLIF(a, b) AS n, NULLIF(c, 'x') AS nc FROM t1;
CREATE VIEW v_multidim AS SELECT ARRAY[ARRAY[a, b], ARRAY[b, a]] AS m, (ARRAY[[1, 2], [3, 4]])[2][1] AS el FROM t1;
CREATE VIEW v_syscols AS SELECT t1.ctid, t1.xmin, t1.xmax, t1.tableoid FROM t1;
CREATE VIEW v_named_ooo AS SELECT f_named(b => 'q', a => 2) AS x, f_named(2, b => 'r') AS y, f_named(a => 3) AS z;

CREATE TABLE arr_t (id int4, xs int4[], m int4[][]);
CREATE RULE r_arr_upd AS ON UPDATE TO t1 DO ALSO UPDATE arr_t SET xs[1] = new.b, m[1][2] = old.a WHERE arr_t.id = old.a;
CREATE RULE r_arr_slice AS ON UPDATE TO t1 DO ALSO UPDATE arr_t SET xs[1:2] = ARRAY[old.a, new.b] WHERE arr_t.id = old.a;
CREATE RULE r_multiassign AS ON UPDATE TO t2 DO ALSO UPDATE t1 SET (b, e) = (SELECT new.f, old.a::int8) WHERE t1.a = old.a;
CREATE RULE r_multi_sub AS ON UPDATE TO t2 DO ALSO UPDATE arr_t SET (xs[1], id) = (SELECT new.f, old.a) WHERE arr_t.id = old.a;

CREATE TYPE comp AS (x int4, y text);
CREATE TABLE compt (id int4, cc comp, ca comp[], CONSTRAINT compt_x_check CHECK ((cc).x > 0));
CREATE VIEW v_fsel AS SELECT (cc).x AS cx, (compt.cc).y AS cy FROM compt;
CREATE VIEW v_fsel_func AS SELECT (f_out(a)).c AS oc, (f_out(a)).b + 1 AS ob FROM t1;
CREATE VIEW v_fsel_row AS SELECT (ROW(a, b)).f1 AS r1 FROM t1;
CREATE VIEW v_fsel_sub AS SELECT (s.r).f2 AS sf FROM (SELECT ROW(a, b) AS r FROM t1) s;
CREATE VIEW v_fsel_subvar AS SELECT (s2.r2).y AS sy FROM (SELECT cc AS r2 FROM compt) s2;
CREATE VIEW v_fsel_subsub AS SELECT (o.r).f1 AS ff FROM (SELECT i.r FROM (SELECT ROW(a, b) AS r FROM t1) i) o;
CREATE VIEW v_fsel_wholerow AS SELECT (s3).a AS wa FROM (SELECT a, b FROM t1) s3;
CREATE VIEW v_fsel_cte AS WITH cw AS (SELECT ROW(a, b) AS r FROM t1) SELECT (cw.r).f2 AS cf FROM cw;
CREATE VIEW v_fsel_ctevar AS WITH cw2 AS (SELECT ROW(a, b) AS r FROM t1), cw3 AS (SELECT cw2.r FROM cw2) SELECT (cw3.r).f1 AS cf3 FROM cw3;
CREATE RULE r_fstore AS ON UPDATE TO t1 DO ALSO UPDATE compt SET cc.x = new.b WHERE compt.id = old.a;
CREATE RULE r_fstore_arr AS ON UPDATE TO t1 DO ALSO UPDATE compt SET ca[1].y = new.c WHERE compt.id = old.a;

CREATE TABLE xt (id int4, x xml, x2 xml, j jsonb);
CREATE VIEW v_xmlexpr AS SELECT XMLELEMENT(NAME el, XMLATTRIBUTES(id AS ident, id + 1 AS next), 'body', x) AS xe, XMLFOREST(id, x AS ex) AS xf, XMLCONCAT(x, x2) AS xc, XMLPI(NAME "my-pi", 'v1') AS xp FROM xt;
CREATE VIEW v_xmlexpr2 AS SELECT XMLROOT(x, VERSION '1.1', STANDALONE YES) AS xr, XMLROOT(x, VERSION NO VALUE) AS xr2, XMLPARSE(DOCUMENT ('<a>' || id::text || '</a>')) AS xpd, XMLPARSE(CONTENT 'c' PRESERVE WHITESPACE) AS xpc, XMLSERIALIZE(CONTENT x AS text) AS xs, XMLSERIALIZE(DOCUMENT x AS varchar INDENT) AS xsi, (x IS DOCUMENT) AS isd FROM xt;
CREATE VIEW v_xmlexists AS SELECT id FROM xt WHERE XMLEXISTS('//a' PASSING x);
CREATE VIEW v_xmltable AS SELECT xt2.* FROM xt, XMLTABLE('/root/item' PASSING x COLUMNS iid int4 PATH '@id', txt text PATH 'text()' DEFAULT 'missing', ord FOR ORDINALITY, req text PATH '@req' NOT NULL) xt2;
CREATE VIEW v_xmltable_ns AS SELECT z.* FROM xt, XMLTABLE(XMLNAMESPACES('http://x.example' AS ex, DEFAULT 'http://d.example'), '/ex:r' PASSING x COLUMNS c1 text PATH 'ex:c1') z;

CREATE VIEW v_jsonctor AS SELECT JSON_OBJECT('k1': a, 'k2': c ABSENT ON NULL WITH UNIQUE KEYS) AS jo, JSON_ARRAY(a, b NULL ON NULL) AS ja, JSON('{"x": 1}') AS jj, JSON_SCALAR(a) AS js, JSON_SERIALIZE(j) AS jser FROM t1, xt;
CREATE VIEW v_jsonagg AS SELECT JSON_OBJECTAGG(c: a) AS joa, JSON_ARRAYAGG(b RETURNING jsonb) AS jaa FROM t1;
CREATE VIEW v_jsonquery AS SELECT JSON_EXISTS(j, '$.a[*] ? (@ == $n)' PASSING id AS n) AS je, JSON_VALUE(j, '$.b' RETURNING int4 DEFAULT 0 ON ERROR) AS jv, JSON_QUERY(j, '$.c' WITH WRAPPER) AS jq, JSON_QUERY(j, '$.d' RETURNING text OMIT QUOTES EMPTY ARRAY ON EMPTY ERROR ON ERROR) AS jq2 FROM xt;
CREATE VIEW v_isjson AS SELECT (c IS JSON) AS ij, (c IS JSON OBJECT WITH UNIQUE KEYS) AS ijo, (c IS NOT JSON ARRAY) AS inj, (c IS JSON SCALAR) AS ijs FROM t1;
CREATE VIEW v_namedargs AS SELECT f_add(y => 5, x => a) AS s1, f_add(a, y => b) AS s2 FROM t1;

-- batch closeout: GROUPING SETS / TABLESAMPLE deparse
CREATE VIEW v_gsets AS SELECT a, b, count(*) AS n FROM t1 GROUP BY GROUPING SETS ((a), (b), ());
CREATE VIEW v_rollup AS SELECT a, b, sum(e) AS s FROM t1 GROUP BY ROLLUP (a, b);
CREATE VIEW v_cube AS SELECT a, b, max(b) AS m FROM t1 GROUP BY CUBE (a, (a, b));
CREATE VIEW v_gsets_nested AS SELECT a, b, c, count(*) AS n FROM t1 GROUP BY GROUPING SETS (ROLLUP (a, b), CUBE (c), ());
CREATE VIEW v_gsets_distinct AS SELECT a, b, count(*) AS n FROM t1 GROUP BY DISTINCT ROLLUP (a), ROLLUP (b);
CREATE VIEW v_tsample AS SELECT a FROM t1 TABLESAMPLE bernoulli(10);
CREATE VIEW v_tsample_rep AS SELECT t1.a, t2.f FROM t1 TABLESAMPLE system(5) REPEATABLE (42), t2 TABLESAMPLE bernoulli(2.5) REPEATABLE (1.5);

-- indexdef residue: non-btree ams, reloptions, attoptions, collation
CREATE INDEX idx_hash ON t2 USING hash (f);
CREATE INDEX idx_gin ON arr_t USING gin (xs);
CREATE INDEX idx_brin ON t1 USING brin (b) WITH (pages_per_range = 64);
CREATE INDEX idx_brin_attopt ON t1 USING brin (e int8_minmax_multi_ops (values_per_range = 16));
CREATE INDEX idx_spgist ON t2 USING spgist (g);
CREATE INDEX idx_relopt ON t1 (b) WITH (fillfactor = 70, deduplicate_items = off);
CREATE INDEX idx_collate ON t2 (g COLLATE "C" DESC);
CREATE TABLE geo_t (gid int4, p point);
CREATE INDEX idx_gist ON geo_t USING gist (p);

-- serial / identity sequences
CREATE TABLE ser_t (id serial PRIMARY KEY, iid int4 GENERATED ALWAYS AS IDENTITY, n int4);

-- partition constraint deparse
CREATE TABLE pt (pa int4, pb text) PARTITION BY RANGE (pa);
CREATE TABLE pt1 PARTITION OF pt FOR VALUES FROM (0) TO (10);
CREATE TABLE pt2 PARTITION OF pt DEFAULT;
CREATE TABLE ptl (la int4, lb int4) PARTITION BY LIST (la);
CREATE TABLE ptl1 PARTITION OF ptl FOR VALUES IN (1, 2, 3) PARTITION BY HASH (lb);
CREATE TABLE ptl1h PARTITION OF ptl1 FOR VALUES WITH (MODULUS 4, REMAINDER 1);

-- SQL-standard function bodies
CREATE FUNCTION f_ret(x int4) RETURNS int4 LANGUAGE sql IMMUTABLE RETURN x + 1;
CREATE FUNCTION f_atomic(x int4, y int4) RETURNS int8 LANGUAGE sql BEGIN ATOMIC SELECT sum(a) FROM t1 WHERE a > x; END;
CREATE FUNCTION f_atomic_multi(x int4) RETURNS int4 LANGUAGE sql BEGIN ATOMIC INSERT INTO rule_log VALUES (x, x, 'body'); SELECT x + 1; END;
CREATE FUNCTION f_body_unnamed(int4) RETURNS int4 LANGUAGE sql RETURN $1 * 2;

-- prefix operator deparse (generate_operator_name left arm)
CREATE VIEW v_prefix_op AS SELECT |/ (a::float8) AS sq, @ (b - 10) AS ab FROM t1;
CREATE OPERATOR public.@<< (RIGHTARG = int4, FUNCTION = int4um);
CREATE VIEW v_prefix_custom AS SELECT @<< a AS neg FROM t1;

-- extended statistics expressions
CREATE STATISTICS st_ab (dependencies) ON a, b FROM t1;
CREATE STATISTICS st_expr ON (a + b), (lower(c)), a FROM t1;

-- new columns under an aliased join
CREATE TABLE jt1 (ja int4, jz int4);
CREATE TABLE jt2 (ja int4, jw int4);
CREATE VIEW v_jalias AS SELECT j.* FROM (jt1 JOIN jt2 USING (ja)) j;
ALTER TABLE jt1 ADD COLUMN newcol1 int4;
ALTER TABLE jt2 ADD COLUMN newcol2 int4;
