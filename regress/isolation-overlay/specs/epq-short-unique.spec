# Reproduce a Unique node inside an EPQ recheck plan. The waiting SELECT
# must rebuild its row after s1 commits and return the updated value.

setup
{
 CREATE TABLE epq_unique (id int PRIMARY KEY, parent_id int, value text NOT NULL);
 INSERT INTO epq_unique VALUES (1, NULL, 'before');
}

teardown
{
 DROP TABLE epq_unique;
}

session s1
setup       { BEGIN ISOLATION LEVEL READ COMMITTED; }
step s1u    { UPDATE epq_unique SET value = 'after' WHERE id = 1; }
step s1c    { COMMIT; }

session s2
step expl
{
 EXPLAIN (COSTS OFF)
 WITH requested AS (SELECT 1 AS id),
 lock_ids AS (
   SELECT id FROM requested
   UNION
   SELECT t.parent_id
     FROM epq_unique t JOIN requested r USING (id)
    WHERE t.parent_id IS NOT NULL
 )
 SELECT t.id, t.value
   FROM epq_unique t JOIN lock_ids USING (id)
  ORDER BY t.id
  FOR UPDATE OF t;
}
step lock
{
 WITH requested AS (SELECT 1 AS id),
 lock_ids AS (
   SELECT id FROM requested
   UNION
   SELECT t.parent_id
     FROM epq_unique t JOIN requested r USING (id)
    WHERE t.parent_id IS NOT NULL
 )
 SELECT t.id, t.value
   FROM epq_unique t JOIN lock_ids USING (id)
  ORDER BY t.id
  FOR UPDATE OF t;
}

session s3
step final  { SELECT id, value FROM epq_unique ORDER BY id; }

permutation expl s1u lock s1c final
