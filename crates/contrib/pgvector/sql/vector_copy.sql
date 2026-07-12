CREATE EXTENSION IF NOT EXISTS vector;
-- vector

CREATE TABLE t (val vector(3));
INSERT INTO t (val) VALUES ('[0,0,0]'), ('[1,2,3]'), ('[1,1,1]'), (NULL);

CREATE TABLE t2 (val vector(3));

\copy t TO '/tmp/pgvector_copy_e2e.bin' WITH (FORMAT binary)
\copy t2 FROM '/tmp/pgvector_copy_e2e.bin' WITH (FORMAT binary)

SELECT * FROM t2 ORDER BY val;

DROP TABLE t;
DROP TABLE t2;

