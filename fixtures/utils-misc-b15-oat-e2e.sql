-- bugs/batch-15-backend-utils-misc: InvokeObjectPostAlterHookArgStr sites C
-- reaches from guc_funcs.c:155 (SET/RESET) and guc.c:4812 (ALTER SYSTEM),
-- witnessed through the test_oat_hooks recorder (NOTICE shape and the
-- deny_set_variable / deny_alter_system refusals are test_oat_hooks.c's).
-- The C oracle install carries no test modules, so the expected file is
-- captured from pgrust.
LOAD 'test_oat_hooks';
CREATE ROLE b15_oat_probe;
GRANT SET ON PARAMETER work_mem TO b15_oat_probe;
GRANT ALTER SYSTEM ON PARAMETER work_mem TO b15_oat_probe;
SET test_oat_hooks.audit = on;
SET work_mem = '8MB';
RESET work_mem;
ALTER SYSTEM SET work_mem = '8MB';
ALTER SYSTEM RESET work_mem;
SET test_oat_hooks.audit = off;
-- The hook runs after the SET took effect, so the user switch itself is the
-- first non-superuser SET the denial policy sees (as in test_oat_hooks.out).
SET test_oat_hooks.deny_set_variable = on;
SET SESSION AUTHORIZATION b15_oat_probe;
SET ROLE b15_oat_probe;
SET test_oat_hooks.deny_set_variable = off;
REVOKE SET ON PARAMETER work_mem FROM b15_oat_probe;
REVOKE ALTER SYSTEM ON PARAMETER work_mem FROM b15_oat_probe;
DROP ROLE b15_oat_probe;
