#!/usr/bin/env bash
#
# One database must never read another's objkv rows. Oids are unique within a
# database, not a cluster, and CREATE DATABASE copies catalog rows keeping
# them; so keys carry the database, and cloning a template that holds objkv
# tables is refused rather than producing tables that exist and read empty.
#
. "$(dirname "$0")/server.sh"

fresh_cluster

echo "1. two databases, one table name, no shared rows"
# From template0, which holds no objkv rows; a clone of it needs its own AMs.
sql "CREATE DATABASE scope_a TEMPLATE template0;" >/dev/null
sql "CREATE DATABASE scope_b TEMPLATE template0;" >/dev/null
install_ams
sql_in scope_a "CREATE TABLE scope_probe (id int, tag text) USING objkv;" >/dev/null
sql_in scope_b "CREATE TABLE scope_probe (id int, tag text) USING objkv;" >/dev/null
sql_in scope_a "INSERT INTO scope_probe VALUES (1,'from-a');" >/dev/null
sql_in scope_b "INSERT INTO scope_probe VALUES (1,'from-b');" >/dev/null

# Not guaranteed equal; the equal-relid case is a unit test in tableam.
OID_A=$(sql_in scope_a "SELECT 'scope_probe'::regclass::oid;")
OID_B=$(sql_in scope_b "SELECT 'scope_probe'::regclass::oid;")
echo "  (relid in scope_a: $OID_A, in scope_b: $OID_B)"

check "scope_a sees only its own row"  "from-a" "$(sql_in scope_a "SELECT tag FROM scope_probe;")"
check "scope_b sees only its own row"  "from-b" "$(sql_in scope_b "SELECT tag FROM scope_probe;")"
check "scope_a has exactly one row"    "1"      "$(sql_in scope_a "SELECT count(*) FROM scope_probe;")"
check "scope_b has exactly one row"    "1"      "$(sql_in scope_b "SELECT count(*) FROM scope_probe;")"

sql_in scope_a "DELETE FROM scope_probe;" >/dev/null
check "deleting in scope_a leaves scope_b alone" "1" "$(sql_in scope_b "SELECT count(*) FROM scope_probe;")"

echo "2. cloning a database that holds objkv tables is refused, not half-done"
# A scratch database as the template, so the cluster's own template1 is left
# alone and still clones.
sql "CREATE DATABASE scope_tpl TEMPLATE template0;" >/dev/null
sql_in scope_tpl "CREATE ACCESS METHOD objkv TYPE TABLE HANDLER heap_tableam_handler;" >/dev/null
sql_in scope_tpl "CREATE TABLE scope_probe (id int, tag text) USING objkv;" >/dev/null
sql_in scope_tpl "INSERT INTO scope_probe VALUES (1,'written in the template');" >/dev/null
contains "refused with a message that says why" "tables stored in objkv" \
         "$(sql "CREATE DATABASE scope_clone TEMPLATE scope_tpl;")"
check "and no database was left behind" "0" \
      "$(sql "SELECT count(*) FROM pg_database WHERE datname='scope_clone';")"

echo "3. a template with no objkv rows still clones"
sql "CREATE DATABASE scope_clone TEMPLATE template1;" >/dev/null
check "clone created" "1" "$(sql "SELECT count(*) FROM pg_database WHERE datname='scope_clone';")"
check "and it is usable" "ok" "$(sql_in scope_clone "SELECT 'ok';")"

echo "4. dropping the table does not make the template clonable again"
# DROP TABLE leaves the rows in the bucket (nothing collects a dropped
# relation yet), and the check is about rows, so the refusal stands.
sql_in scope_tpl "DROP TABLE scope_probe;" >/dev/null
contains "still refused after DROP TABLE" "tables stored in objkv" \
         "$(sql "CREATE DATABASE scope_after_drop TEMPLATE scope_tpl;")"

finish "database scope"
