#!/usr/bin/env bash
#
# The boring SQL an ordinary application writes, against objkv tables: a
# transaction reading its own writes, savepoints, statements that read the
# rows they write, joins and aggregates, RETURNING, ON CONFLICT, cursors,
# errors, bulk loads, and the places objkv is refused.
#
. "$(dirname "$0")/server.sh"

fresh_cluster

echo "0. a schema an application would recognise"
sql "CREATE TABLE ord_dept (id int PRIMARY KEY, name text COLLATE \"C\") USING objkv;" >/dev/null
sql "CREATE TABLE ord_emp (id int PRIMARY KEY, dept int, name text COLLATE \"C\", salary int) USING objkv;" >/dev/null
sql "CREATE TABLE ord_heap (id int PRIMARY KEY, note text);" >/dev/null
sql "INSERT INTO ord_dept VALUES (1,'eng'),(2,'sales'),(3,'empty');" >/dev/null
sql "INSERT INTO ord_emp VALUES (1,1,'ada',100),(2,1,'bob',90),(3,2,'cyd',80),(4,2,'dee',80),(5,NULL,'eve',NULL);" >/dev/null
sql "INSERT INTO ord_heap VALUES (1,'on local disk');" >/dev/null
check "the rows are in" "5" "$(sql "SELECT count(*) FROM ord_emp;")"

echo "1. a transaction reads what it has already written"
check "INSERT then SELECT, same transaction" "6" "$(txn <<'SQL' | last
BEGIN;
INSERT INTO ord_emp VALUES (6,1,'fay',70);
SELECT 'RESULT=' || count(*) FROM ord_emp;
ROLLBACK;
SQL
)"
check "and the rollback took it away" "5" "$(sql "SELECT count(*) FROM ord_emp;")"
check "UPDATE then SELECT, same transaction" "111" "$(txn <<'SQL' | last
BEGIN;
UPDATE ord_emp SET salary = 111 WHERE id = 1;
SELECT 'RESULT=' || salary FROM ord_emp WHERE id = 1;
ROLLBACK;
SQL
)"
check "DELETE then SELECT, same transaction" "4" "$(txn <<'SQL' | last
BEGIN;
DELETE FROM ord_emp WHERE id = 5;
SELECT 'RESULT=' || count(*) FROM ord_emp;
ROLLBACK;
SQL
)"
check "an index lookup finds a row the same transaction inserted" "fay" "$(txn <<'SQL' | last
BEGIN;
INSERT INTO ord_emp VALUES (6,1,'fay',70);
SELECT 'RESULT=' || name FROM ord_emp WHERE id = 6;
ROLLBACK;
SQL
)"

echo "2. savepoints"
check "rolling back to a savepoint keeps what came before it" "7" "$(txn <<'SQL' | last
BEGIN;
INSERT INTO ord_emp VALUES (7,1,'gil',60);
SAVEPOINT s;
INSERT INTO ord_emp VALUES (8,1,'hal',50);
ROLLBACK TO SAVEPOINT s;
SELECT 'RESULT=' || id FROM ord_emp WHERE id > 6;
COMMIT;
SQL
)"
check "and the committed one is durable" "gil" "$(sql "SELECT name FROM ord_emp WHERE id = 7;")"
sql "DELETE FROM ord_emp WHERE id = 7;" >/dev/null

echo "3. a statement that reads the rows it is writing"
# A scan that saw its own inserts mid-statement would never stop; it must
# read the table as it was when the statement started.
sql "CREATE TABLE ord_dbl (n int) USING objkv;" >/dev/null
sql "INSERT INTO ord_dbl SELECT generate_series(1,100);" >/dev/null
sql "INSERT INTO ord_dbl SELECT n FROM ord_dbl;" >/dev/null
check "INSERT INTO t SELECT * FROM t exactly doubles" "200" "$(sql "SELECT count(*) FROM ord_dbl;")"
check "inside a transaction too" "400" "$(txn <<'SQL' | last
BEGIN;
INSERT INTO ord_dbl SELECT n FROM ord_dbl;
SELECT 'RESULT=' || count(*) FROM ord_dbl;
COMMIT;
SQL
)"
check "and through an index" "2" "$(txn <<'SQL' | last
BEGIN;
INSERT INTO ord_emp SELECT id + 100, dept, name, salary FROM ord_emp;
SELECT 'RESULT=' || count(*) FROM ord_emp WHERE id IN (1,101);
ROLLBACK;
SQL
)"
check "UPDATE over the whole table lands once per row" "5" "$(txn <<'SQL' | last
BEGIN;
UPDATE ord_emp SET salary = COALESCE(salary,0) + 1;
SELECT 'RESULT=' || count(*) FROM ord_emp;
ROLLBACK;
SQL
)"

echo "4. joins, aggregates and the rest of a day's work"
check "inner join"       "ada:eng"  "$(sql "SELECT e.name || ':' || d.name FROM ord_emp e JOIN ord_dept d ON d.id = e.dept WHERE e.id = 1;")"
check "left join keeps the unmatched row" "eve" "$(sql "SELECT e.name FROM ord_emp e LEFT JOIN ord_dept d ON d.id = e.dept WHERE d.id IS NULL;")"
check "self join"        "bob"      "$(sql "SELECT b.name FROM ord_emp a JOIN ord_emp b ON b.dept = a.dept AND b.id <> a.id WHERE a.id = 1;")"
check "group by"         "eng:2|sales:2" "$(sql "SELECT string_agg(d.name || ':' || c, '|' ORDER BY d.name) FROM (SELECT dept, count(*) c FROM ord_emp GROUP BY dept) g JOIN ord_dept d ON d.id = g.dept;")"
check "having"           "2"        "$(sql "SELECT count(*) FROM (SELECT dept FROM ord_emp GROUP BY dept HAVING sum(salary) > 150) x;")"
check "sum ignores NULL" "350"      "$(sql "SELECT sum(salary) FROM ord_emp;")"
check "count(col) ignores NULL, count(*) does not" "4:5" "$(sql "SELECT count(salary) || ':' || count(*) FROM ord_emp;")"
check "order by and limit" "ada,bob" "$(sql "SELECT string_agg(name, ',') FROM (SELECT name FROM ord_emp ORDER BY name LIMIT 2) x;")"
check "offset"           "cyd"      "$(sql "SELECT name FROM ord_emp ORDER BY name OFFSET 2 LIMIT 1;")"
check "distinct"         "100,80,90" "$(sql "SELECT string_agg(DISTINCT salary::text, ',' ORDER BY salary::text) FROM ord_emp WHERE salary IS NOT NULL;")"
check "exists"           "2"        "$(sql "SELECT count(*) FROM ord_dept d WHERE EXISTS (SELECT 1 FROM ord_emp e WHERE e.dept = d.id);")"
check "correlated scalar subquery" "100" "$(sql "SELECT (SELECT max(salary) FROM ord_emp e WHERE e.dept = d.id) FROM ord_dept d WHERE d.id = 1;")"
check "in"               "2"        "$(sql "SELECT count(*) FROM ord_emp WHERE dept IN (SELECT id FROM ord_dept WHERE name = 'sales');")"
check "union across the two storage kinds" "2" "$(sql "SELECT count(*) FROM (SELECT name FROM ord_dept WHERE id=1 UNION SELECT note FROM ord_heap) x;")"
check "except"           "2"        "$(sql "SELECT count(*) FROM (SELECT id FROM ord_emp EXCEPT SELECT id FROM ord_dept) x;")"
check "cte"              "2"        "$(sql "WITH big AS (SELECT * FROM ord_emp WHERE salary >= 90) SELECT count(*) FROM big;")"
check "window function"  "1"        "$(sql "SELECT rn FROM (SELECT name, row_number() OVER (ORDER BY name) rn FROM ord_emp) x WHERE name = 'ada';")"
check "case and coalesce" "unpaid"  "$(sql "SELECT CASE WHEN salary IS NULL THEN 'unpaid' ELSE 'paid' END FROM ord_emp WHERE id = 5;")"
check "a join against a heap table" "on local disk" "$(sql "SELECT h.note FROM ord_heap h JOIN ord_emp e ON e.id = h.id WHERE e.name = 'ada';")"

echo "5. writes that read"
sql "CREATE TABLE ord_up (id int PRIMARY KEY, bump int) USING objkv;" >/dev/null
sql "INSERT INTO ord_up VALUES (1,0),(2,0);" >/dev/null
check "UPDATE ... FROM" "100" "$(sql "UPDATE ord_up u SET bump = e.salary FROM ord_emp e WHERE e.id = u.id AND u.id = 1; SELECT 'RESULT=' || bump FROM ord_up WHERE id = 1;" | last)"
check "DELETE with a subquery" "1" "$(sql "DELETE FROM ord_up WHERE id IN (SELECT id FROM ord_emp WHERE salary = 90); SELECT 'RESULT=' || count(*) FROM ord_up;" | last)"
check "INSERT ... RETURNING" "9" "$(sql "INSERT INTO ord_emp VALUES (9,1,'ivy',10) RETURNING id;" | head -1)"
check "UPDATE ... RETURNING" "11" "$(sql "UPDATE ord_emp SET salary = 11 WHERE id = 9 RETURNING salary;" | head -1)"
check "DELETE ... RETURNING" "ivy" "$(sql "DELETE FROM ord_emp WHERE id = 9 RETURNING name;" | head -1)"
check "ON CONFLICT DO NOTHING" "ada" "$(sql "INSERT INTO ord_emp VALUES (1,1,'clash',1) ON CONFLICT DO NOTHING; SELECT 'RESULT=' || name FROM ord_emp WHERE id = 1;" | last)"
check "ON CONFLICT DO UPDATE" "77" "$(sql "INSERT INTO ord_emp VALUES (1,1,'ada',1) ON CONFLICT (id) DO UPDATE SET salary = 77; SELECT 'RESULT=' || salary FROM ord_emp WHERE id = 1;" | last)"
check "ON CONFLICT DO NOTHING with nothing to conflict with" "10" \
      "$(sql "INSERT INTO ord_emp VALUES (10,1,'jan',5) ON CONFLICT DO NOTHING RETURNING id;" | head -1)"
check "ON CONFLICT DO UPDATE with nothing to conflict with" "11" \
      "$(sql "INSERT INTO ord_emp VALUES (11,1,'ken',5) ON CONFLICT (id) DO UPDATE SET salary = 0 RETURNING id;" | head -1)"
check "DO UPDATE can read the row it collided with" "10" \
      "$(sql "INSERT INTO ord_emp VALUES (10,1,'jan',1) ON CONFLICT (id) DO UPDATE SET salary = ord_emp.salary * 2 RETURNING salary;" | head -1)"
sql "DELETE FROM ord_emp WHERE id IN (10,11);" >/dev/null
check "a data-modifying CTE" "2" "$(txn <<'SQL' | last
BEGIN;
WITH gone AS (DELETE FROM ord_emp WHERE dept = 2 RETURNING id) SELECT 'RESULT=' || count(*) FROM gone;
ROLLBACK;
SQL
)"

echo "5b. two sessions upserting the same key (known divergence from Postgres)"
# Known divergence from Postgres. There the second upsert blocks on the
# first's uncommitted key, and once it commits takes the DO UPDATE path:
# both succeed and hits is exactly 2, every time. objkv takes no key locks:
# the loser is refused at COMMIT with 40001 (accepted below as the only
# legitimate failure), and hits is 1 or 2 depending on who read what first.
# The isolation contract is in objkv_am.rs and docs/objkv.md; this step pins
# it so a change is noticed, not to bless it.
sql "CREATE TABLE ord_race (id int PRIMARY KEY, hits int) USING objkv;" >/dev/null
race() {
    txn <<SQL
BEGIN;
SELECT pg_sleep($1);
INSERT INTO ord_race VALUES (1,1) ON CONFLICT (id) DO UPDATE SET hits = ord_race.hits + 1;
COMMIT;
SQL
}
# Named pids, not a bare `wait`: the server is a background job of this shell too.
race 0 >"$WORK/race1" 2>&1 & R1=$!
race 0 >"$WORK/race2" 2>&1 & R2=$!
# Each on its own: `wait a b` reports only the last, and a client that
# failed outright would leave hits = 1 looking like a fair race.
wait "$R1"; S1=$?
wait "$R2"; S2=$?
# A racer may exit nonzero for exactly one reason: the conflict was refused.
# Anything else -- a connection failure, a client error with no ERROR line --
# would leave hits = 1 looking like a fair race.
for r in 1 2; do
    eval "st=\$S$r"
    if [ "$st" != 0 ] && ! grep -q "could not serialize" "$WORK/race$r"; then
        fail "racer $r failed for a reason other than the conflict: $(tail -2 "$WORK/race$r" | tr '\n' ' ')"
    elif grep -q "^ERROR" "$WORK/race$r" && ! grep -q "could not serialize" "$WORK/race$r"; then
        fail "racer $r reported an error other than the conflict: $(grep '^ERROR' "$WORK/race$r" | head -1)"
    fi
done
check "exactly one row exists" "1" "$(sql "SELECT count(*) FROM ord_race;")"
# Either serialization order is legitimate; what is not is two winners.
check "known divergence from Postgres: hits is 1 or 2 and a racer may get 40001, where Postgres would block and always reach 2" \
      "t" "$(sql "SELECT hits IN (1,2) FROM ord_race;")"
sql "DROP TABLE ord_race;" >/dev/null

echo "6. prepared statements and cursors"
check "PREPARE and EXECUTE" "ada" "$(txn <<'SQL' | last
PREPARE byid (int) AS SELECT 'RESULT=' || name FROM ord_emp WHERE id = $1;
EXECUTE byid(1);
SQL
)"
check "the same plan reused with another value" "bob" "$(txn <<'SQL' | last
PREPARE byid (int) AS SELECT 'RESULT=' || name FROM ord_emp WHERE id = $1;
EXECUTE byid(1);
EXECUTE byid(1);
EXECUTE byid(1);
EXECUTE byid(1);
EXECUTE byid(1);
EXECUTE byid(2);
SQL
)"
check "a cursor walks the rows" "ada" "$(txn <<'SQL' | last
BEGIN;
DECLARE c CURSOR FOR SELECT 'RESULT=' || name FROM ord_emp ORDER BY id;
FETCH 1 FROM c;
COMMIT;
SQL
)"

echo "7. errors leave the transaction usable afterwards"
check "a failed statement aborts the transaction" "1" "$(txn <<'SQL' | grep -c "current transaction is aborted"
BEGIN;
INSERT INTO ord_emp VALUES (1,1,'dup',1);
SELECT 'RESULT=' || count(*) FROM ord_emp;
ROLLBACK;
SQL
)"
check "and the connection works after the rollback" "t" "$(sql "SELECT count(*) > 0 FROM ord_emp;")"

echo "8. bulk"
sql "CREATE TABLE ord_bulk (id int PRIMARY KEY, v text COLLATE \"C\") USING objkv;" >/dev/null
sql "INSERT INTO ord_bulk SELECT g, 'v' || g FROM generate_series(1,5000) g;" >/dev/null
check "5000 rows in one statement" "5000" "$(sql "SELECT count(*) FROM ord_bulk;")"
check "and each is right"          "v4999" "$(sql "SELECT v FROM ord_bulk WHERE id = 4999;")"
check "COPY out"                   "5000"  "$(sql "COPY (SELECT * FROM ord_bulk) TO STDOUT;" | grep -c .)"
sql "DELETE FROM ord_bulk WHERE id % 2 = 0;" >/dev/null
check "half deleted"               "2500"  "$(sql "SELECT count(*) FROM ord_bulk;")"
check "TRUNCATE takes the rest"    "0"     "$(sql "TRUNCATE ord_bulk; SELECT count(*) FROM ord_bulk;" | tail -1)"

echo "9. objkv is refused where it cannot work"
# A temporary table is discarded at disconnect and objkv has no per-session
# namespace, by either spelling.
contains "CREATE TEMP TABLE ... USING objkv is refused" "objkv cannot store temporary tables" \
         "$(sql "CREATE TEMP TABLE ord_tmp1 (i int) USING objkv;")"
contains "and so is a pg_temp-qualified one" "objkv cannot store temporary tables" \
         "$(sql "CREATE TABLE pg_temp.ord_tmp2 (i int) USING objkv;")"
# An objkv default must not quietly send scratch tables to the bucket.
check "an objkv default sends a temp table to heap" "heap" \
      "$(sql "SET default_table_access_method = objkv; CREATE TEMP TABLE ord_tmp3 (i int); SELECT a.amname FROM pg_am a JOIN pg_class c ON c.relam = a.oid WHERE c.relname = 'ord_tmp3';" | tail -1)"
check "the pg_temp spelling too" "heap" \
      "$(sql "SET default_table_access_method = objkv; CREATE TABLE pg_temp.ord_tmp4 (i int); SELECT a.amname FROM pg_am a JOIN pg_class c ON c.relam = a.oid WHERE c.relname = 'ord_tmp4';" | tail -1)"

echo "10. the maintenance commands answer for themselves"
# VACUUM has nothing to do and succeeds; the two that rewrite or sample pages
# refuse, naming objkv rather than a buffer-slot error from three frames down.
check "VACUUM is a no-op, not an error" "VACUUM" "$(sql "VACUUM ord_emp;")"
contains "VACUUM FULL refused, and named objkv" "objkv does not support rewriting" "$(sql "VACUUM FULL ord_emp;")"
check "ANALYZE runs" "ANALYZE" "$(sql "ANALYZE ord_emp;")"

finish "ordinary SQL"
