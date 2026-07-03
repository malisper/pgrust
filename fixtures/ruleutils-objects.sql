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
