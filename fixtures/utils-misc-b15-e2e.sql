-- bugs/batch-15-backend-utils-misc: SQL-visible fixes in utils/misc, expected
-- captured from C 18.6.
\set VERBOSITY verbose
-- guc.c:3759 assign-hook order: assign_session_replication_role compares the
-- old value, so the plan cache is reset and the stale rewritten INSERT is not
-- reused under the new role.
CREATE TABLE b15_t(i int);
CREATE TABLE b15_audit(i int);
CREATE RULE b15_r AS ON INSERT TO b15_t DO ALSO INSERT INTO b15_audit VALUES (NEW.i);
PREPARE b15_p(int) AS INSERT INTO b15_t VALUES ($1);
EXECUTE b15_p(1);
SET session_replication_role = replica;
EXECUTE b15_p(2);
SELECT * FROM b15_audit ORDER BY i;
RESET session_replication_role;
EXECUTE b15_p(3);
SELECT * FROM b15_audit ORDER BY i;
DEALLOCATE b15_p;
DROP TABLE b15_t, b15_audit;
-- show_all_settings is value-per-call: a later row sees a GUC changed by a
-- sibling expression of the same ProjectSet.
SET work_mem = '4MB';
SELECT (s).setting FROM (SELECT pg_show_all_settings() AS s, set_config('work_mem', '8MB', false) AS changed OFFSET 0) q WHERE (s).name = 'work_mem';
SHOW work_mem;
RESET work_mem;
SELECT count(*) > 300 FROM pg_settings;
SELECT name, setting, unit, context, vartype, min_val, max_val, enumvals, boot_val, reset_val, pending_restart FROM pg_settings WHERE name IN ('work_mem', 'session_replication_role', 'seed', 'datestyle', 'enable_seqscan') ORDER BY name;
