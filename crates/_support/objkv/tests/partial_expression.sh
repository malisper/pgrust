#!/usr/bin/env bash
#
# Partial and expression indexes. The executor evaluates both on the way in;
# the objkv side has to evaluate them again on the way out, when an update or
# delete retires a row's entries from an image, and at CREATE INDEX over rows
# that already exist.
#
. "$(dirname "$0")/server.sh"
ROWS="${ROWS:-3000}"

echo "0. a table with an index built after the rows"
fresh_cluster
must "CREATE TABLE ev (id int PRIMARY KEY, kind text COLLATE \"C\", email text COLLATE \"C\", done bool) USING objkv;" >/dev/null
must "INSERT INTO ev SELECT g, CASE g % 3 WHEN 0 THEN 'a' WHEN 1 THEN 'b' ELSE 'c' END,
      'User' || g || '@Example.com', g % 5 = 0 FROM generate_series(1,$ROWS) g;" >/dev/null
must "CREATE INDEX ev_open ON ev (kind) WHERE NOT done;" >/dev/null
must "CREATE INDEX ev_email ON ev (lower(email));" >/dev/null
must "CREATE UNIQUE INDEX ev_email_uniq ON ev (upper(email)) WHERE done;" >/dev/null

echo "1. the partial index answers what it covers, and only that"
shows "a matching WHERE uses it" "ev_open" "$(plan "SELECT id FROM ev WHERE kind = 'a' AND NOT done;")"
agree "kind = a, not done" "SELECT count(*) FROM ev WHERE kind = 'a' AND NOT done;"
agree "the rows themselves" "SELECT string_agg(id::text, ',' ORDER BY id) FROM (SELECT id FROM ev WHERE kind = 'b' AND NOT done AND id < 40) x;"
check "a query the predicate does not imply does not use it" "" \
      "$(plan "SELECT id FROM ev WHERE kind = 'a';" | grep ev_open)"

echo "2. the expression index answers by the expression"
shows "lower(email) = ... uses it" "ev_email" "$(plan "SELECT id FROM ev WHERE lower(email) = 'user7@example.com';")"
check "and finds the row" "7" "$(idx "SELECT id FROM ev WHERE lower(email) = 'user7@example.com';")"
agree "a range on the expression" "SELECT count(*) FROM ev WHERE lower(email) < 'user2';"

echo "3. rows that move across the predicate boundary"
must "UPDATE ev SET done = true WHERE id IN (1, 2, 3);" >/dev/null
check "leaving the predicate removes the entry" "0" "$(idx "SELECT count(*) FROM ev WHERE kind = 'b' AND NOT done AND id = 1;")"
must "UPDATE ev SET done = false WHERE id = 5;" >/dev/null
check "entering it adds one" "1" "$(idx "SELECT count(*) FROM ev WHERE kind = 'c' AND NOT done AND id = 5;")"
agree "the partial index and the table still agree" "SELECT count(*) FROM ev WHERE NOT done;"
must "UPDATE ev SET email = 'Moved' || id || '@Example.com' WHERE id BETWEEN 10 AND 20;" >/dev/null
check "an updated expression value is found under the new value" "11" \
      "$(idx "SELECT count(*) FROM ev WHERE lower(email) LIKE 'moved%';")"
check "and not under the old one" "0" "$(idx "SELECT count(*) FROM ev WHERE lower(email) = 'user15@example.com';")"
must "DELETE FROM ev WHERE id BETWEEN 100 AND 110;" >/dev/null
agree "after a delete, every index agrees with the table" "SELECT count(*) FROM ev WHERE lower(email) >= 'user1' AND lower(email) < 'user2';"

echo "4. a unique expression index that is also partial"
check "a duplicate under the predicate is refused" "duplicate key" \
      "$(sql "INSERT INTO ev VALUES (99999, 'a', 'user1@EXAMPLE.com', true);" | grep -o 'duplicate key')"
check "the same value outside the predicate is fine" "INSERT 0 1" \
      "$(sql "INSERT INTO ev VALUES (99998, 'a', 'user1@EXAMPLE.com', false);" | tail -1)"

echo "5. the entries survive a restart"
stop
boot
agree "partial, after a restart"    "SELECT count(*) FROM ev WHERE kind = 'a' AND NOT done;"
agree "expression, after a restart" "SELECT count(*) FROM ev WHERE lower(email) LIKE 'moved%';"
check "the trace never kept a row the index did not point at" "0" "$(trace_mismatches)"

finish "partial and expression indexes"
