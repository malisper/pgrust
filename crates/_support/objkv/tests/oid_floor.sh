#!/usr/bin/env bash
#
# Object ids after a wipe. Postgres keeps the counter's place in the WAL and
# control file, both on the disk this design throws away, so the bucket has
# to remember it or a blank machine renumbers on top of relations the bucket
# already holds. finish_line.sh cannot catch this: creating tables after the
# flip records the counter on the way. Lift, flip, wipe, nothing between.
#
. "$(dirname "$0")/server.sh"
FRESH="$WORK/pgdata_fresh"
TABLES="${TABLES:-120}"

echo "0. an empty bucket and a cluster whose counter has run on a long way"
fresh_cluster
install_lift
# Each table burns several numbers, putting the counter well clear of what a
# fresh initdb starts from.
sql "DO \$\$ BEGIN FOR i IN 1..$TABLES LOOP
    EXECUTE format('CREATE TABLE burn_%s (id int PRIMARY KEY, v text COLLATE \"C\") USING objkv', i);
END LOOP; END \$\$;" >/dev/null
sql "INSERT INTO burn_1 VALUES (1,'planted');" >/dev/null
HIGH=$(sql "SELECT max(oid) FROM pg_class;")
echo "  the counter reached $HIGH after $TABLES tables"

echo "1. lift and flip, and nothing at all after it"
lift_all
stop

echo "2. a blank machine, numbering from scratch"
blank_directory "$FRESH"
boot "$FRESH"
check "the lifted tables are all there" "$TABLES" \
      "$(sql "SELECT count(*) FROM pg_class WHERE relname LIKE 'burn\_%' AND relkind = 'r';")"

echo "3. the first relation it creates must not reuse a number"
sql "CREATE TABLE after_wipe (id int PRIMARY KEY, v text COLLATE \"C\") USING objkv;" >/dev/null
check "its number is above everything the bucket already held" "t" \
      "$(sql "SELECT 'after_wipe'::regclass::oid > $HIGH;")"

echo "4. and a hundred more of them collide with nothing"
sql "DO \$\$ BEGIN FOR i IN 1..100 LOOP
    EXECUTE format('CREATE TABLE post_%s (id int) USING objkv', i);
END LOOP; END \$\$;" >/dev/null
check "all hundred created" "100" "$(sql "SELECT count(*) FROM pg_class WHERE relname LIKE 'post\_%';")"
check "no two relations share a number" "0" \
      "$(sql "SELECT count(*) FROM (SELECT oid FROM pg_class GROUP BY oid HAVING count(*) > 1) x;")"
check "and the row planted before the wipe still reads" "planted" "$(sql "SELECT v FROM burn_1 WHERE id = 1;")"

finish "the bucket remembers where the counter got to"
