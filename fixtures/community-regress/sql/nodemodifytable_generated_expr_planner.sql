-- expand_generated_columns_in_expr (rewriteHandler.c) keeps system-column
-- Vars (a CHECK on tableoid must not index the attribute array with -6), and
-- ExecRelCheck / ExecRelGenVirtualNotNull / ExecInitGenerated prepare their
-- expressions through expression_planner (a CollateExpr must fold away).
CREATE TABLE genx_vc (a int, v int GENERATED ALWAYS AS (a + 1) VIRTUAL, CHECK (tableoid <> 0));
INSERT INTO genx_vc(a) VALUES (1);
UPDATE genx_vc SET a = 2 RETURNING *;
CREATE TABLE genx_vnn (a text, v text COLLATE "C" GENERATED ALWAYS AS (a COLLATE "C") VIRTUAL NOT NULL);
INSERT INTO genx_vnn(a) VALUES ('ok');
INSERT INTO genx_vnn(a) VALUES (NULL);
UPDATE genx_vnn SET a = 'x' RETURNING *;
CREATE TABLE genx_st (a text, v text GENERATED ALWAYS AS (a COLLATE "C") STORED, CHECK ((a COLLATE "C") <> 'zz'));
INSERT INTO genx_st(a) VALUES ('ok') RETURNING *;
INSERT INTO genx_st(a) VALUES ('zz');
UPDATE genx_st SET a = 'yy' RETURNING *;
DROP TABLE genx_vc, genx_vnn, genx_st;
