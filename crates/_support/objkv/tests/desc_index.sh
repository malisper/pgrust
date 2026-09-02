#!/usr/bin/env bash
#
# DESC and NULLS FIRST index columns. A descending column is stored with its
# bytes inverted and a nulls-first NULL under every value, so a scan in key
# order is the order the index declares -- and ORDER BY that matches it
# needs no sort, in either direction.
#
. "$(dirname "$0")/server.sh"
ROWS="${ROWS:-2000}"
export PGRUST_OBJKV_TRACE=1
# The bitmap path costs less than the ordered one on a table this size and
# would hide whether the ordered path exists; that is what is under test.
IDX_OPTS="-c enable_bitmapscan=off"

echo "0. a table and three indexes, each ordered its own way"
fresh_cluster
must "CREATE TABLE ev (id int PRIMARY KEY, grp int, score float8, tag text COLLATE \"C\") USING objkv;" >/dev/null
must "INSERT INTO ev SELECT g, g % 7, CASE WHEN g % 11 = 0 THEN NULL ELSE (g * 37 % 1000) / 10.0 END,
      CASE WHEN g % 13 = 0 THEN NULL ELSE 'tag-' || lpad((g * 7 % 500)::text, 3, '0') END
      FROM generate_series(1,$ROWS) g;" >/dev/null
must "CREATE INDEX ev_grp_score ON ev (grp, score DESC);" >/dev/null
must "CREATE INDEX ev_tag_nf ON ev (tag NULLS FIRST);" >/dev/null
must "CREATE INDEX ev_score_desc_nl ON ev (score DESC NULLS LAST);" >/dev/null
check "three indexes built" "3" "$(sql "SELECT count(*) FROM pg_class WHERE relname LIKE 'ev\\_%' AND relname <> 'ev_pkey' AND relkind = 'i';")"

echo "1. a descending column answers ranges the right way round"
agree "score > 40 in a group"   "SELECT count(*) FROM ev WHERE grp = 3 AND score > 40;"
agree "score <= 12.5 in a group" "SELECT count(*) FROM ev WHERE grp = 3 AND score <= 12.5;"
agree "a closed range"           "SELECT string_agg(id::text, ',' ORDER BY id) FROM ev WHERE grp = 5 AND score BETWEEN 30 AND 35;"
agree "equality on the descending column" "SELECT count(*) FROM ev WHERE grp = 1 AND score = 3.7;"
agree "an IN list on it"         "SELECT count(*) FROM ev WHERE grp = 2 AND score IN (3.7, 40.7, 99.9);"
shows "and it is the index doing it" "ev_grp_score" "$(plan "SELECT id FROM ev WHERE grp = 3 AND score > 40;")"

echo "2. ORDER BY that matches the index needs no sort"
nosort "grp, score DESC"      "SELECT id FROM ev WHERE grp = 4 ORDER BY grp, score DESC;"
nosort "and the reverse walk" "SELECT id FROM ev WHERE grp = 4 ORDER BY grp DESC, score ASC;"
nosort "score DESC NULLS LAST" "SELECT id FROM ev ORDER BY score DESC NULLS LAST LIMIT 10;"
nosort "tag NULLS FIRST"       "SELECT id FROM ev ORDER BY tag NULLS FIRST LIMIT 10;"
agree "top three scores in group 4, from the index" \
      "SELECT string_agg(id::text, ',') FROM (SELECT id FROM ev WHERE grp = 4 AND score IS NOT NULL ORDER BY score DESC, id LIMIT 3) x;"
agree "the ten highest scores overall" \
      "SELECT string_agg(score::text, ',') FROM (SELECT score FROM ev ORDER BY score DESC NULLS LAST LIMIT 10) x;"
agree "the first tags, nulls first" \
      "SELECT string_agg(coalesce(tag, '-'), ',') FROM (SELECT tag FROM ev ORDER BY tag NULLS FIRST, id LIMIT 5) x;"

echo "3. null tests against a nulls-first column"
NULLS=$(tbl "SELECT count(*) FROM ev WHERE tag IS NULL;")
check "IS NULL"     "$NULLS"            "$(idx "SELECT count(*) FROM ev WHERE tag IS NULL;")"
check "IS NOT NULL" "$((ROWS - NULLS))" "$(idx "SELECT count(*) FROM ev WHERE tag IS NOT NULL;")"
shows "IS NOT NULL uses the index" "ev_tag_nf" "$(plan "SELECT id FROM ev WHERE tag IS NOT NULL;")"
agree "a range on it"      "SELECT count(*) FROM ev WHERE tag < 'tag-100';"
agree "a prefix on it"     "SELECT count(*) FROM ev WHERE tag LIKE 'tag-2%';"
SNULLS=$(tbl "SELECT count(*) FROM ev WHERE score IS NULL;")
check "IS NULL on DESC NULLS LAST"     "$SNULLS"            "$(idx "SELECT count(*) FROM ev WHERE score IS NULL;")"
check "IS NOT NULL on DESC NULLS LAST" "$((ROWS - SNULLS))" "$(idx "SELECT count(*) FROM ev WHERE score IS NOT NULL;")"

echo "4. index-only scans read the value back out of an inverted key"
shows "an index-only plan" "Index Only" "$(plan "SELECT score FROM ev WHERE grp = 6 AND score > 90;")"
agree "with the right values" "SELECT string_agg(score::text, ',' ORDER BY score) FROM ev WHERE grp = 6 AND score > 90;"
shows "and on the nulls-first index" "Index Only" "$(plan "SELECT tag FROM ev WHERE tag > 'tag-400';")"
agree "with the right values" "SELECT string_agg(tag, ',' ORDER BY tag) FROM ev WHERE tag > 'tag-490';"

echo "5. updates and deletes retire the right entries"
must "UPDATE ev SET score = 999 WHERE id <= 20;" >/dev/null
must "DELETE FROM ev WHERE id BETWEEN 21 AND 40;" >/dev/null
agree "after the update" "SELECT count(*) FROM ev WHERE score >= 999;"
agree "in each group"    "SELECT string_agg(grp::text || ':' || c::text, ',' ORDER BY grp) FROM (SELECT grp, count(*) c FROM ev WHERE score > 50 GROUP BY grp) x;"
must "CREATE UNIQUE INDEX ev_id_desc ON ev (id DESC);" >/dev/null
# Without the primary key, or it would be the one refusing.
must "ALTER TABLE ev DROP CONSTRAINT ev_pkey;" >/dev/null
check "a unique descending index refuses a duplicate" "ev_id_desc" \
      "$(sql "INSERT INTO ev VALUES (50, 1, 1, 'x');" | grep -o 'ev_id_desc')"

echo "6. and all of it after a restart"
stop
boot
agree "ranges"  "SELECT count(*) FROM ev WHERE grp = 3 AND score > 40;"
agree "order"   "SELECT string_agg(id::text, ',') FROM (SELECT id FROM ev WHERE grp = 4 ORDER BY score DESC NULLS LAST, id LIMIT 5) x;"
check "no scan kept a row the index did not point at" "0" "$(trace_mismatches)"

finish "DESC and NULLS FIRST index columns"
