CREATE TABLE pr_t (id int primary key, tags text[], note text);
CREATE PUBLICATION pr_pub FOR TABLE pr_t (id, tags) WHERE (id > 0);
SELECT c.relname,
  (CASE WHEN pr.prattrs IS NOT NULL THEN
     (SELECT pg_catalog.string_agg(a.attname, ', ')
        FROM pg_catalog.generate_series(0, pg_catalog.array_upper(pr.prattrs::pg_catalog.int2[], 1)) s(i),
             pg_catalog.pg_attribute a
       WHERE a.attrelid = pr.prrelid AND a.attnum = (pr.prattrs::pg_catalog.int2[])[s.i])
   END) AS attnames
FROM pg_catalog.pg_publication_rel pr JOIN pg_catalog.pg_class c ON c.oid = pr.prrelid;
SELECT pubname, schemaname, tablename, attnames, rowfilter
FROM pg_catalog.pg_publication_tables ORDER BY 1, 2, 3;
DROP PUBLICATION pr_pub;
DROP TABLE pr_t;
