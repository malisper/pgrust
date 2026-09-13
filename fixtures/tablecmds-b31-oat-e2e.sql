-- bugs/batch-31-backend-commands-tablecmds: InvokeObjectTruncateHook
-- (tablecmds.c:2422, truncate_check_rel) on explicit, inherited and CASCADE
-- targets, witnessed through the test_oat_hooks recorder. The C oracle
-- install carries no test modules, so the expected file is captured from
-- pgrust and the NOTICE shape is test_oat_hooks.c's.
LOAD 'test_oat_hooks';
CREATE TABLE b31_oat_p (a int PRIMARY KEY);
CREATE TABLE b31_oat_c () INHERITS (b31_oat_p);
CREATE TABLE b31_oat_f (a int REFERENCES b31_oat_p);
SET test_oat_hooks.audit = on;
TRUNCATE b31_oat_p;
TRUNCATE b31_oat_p CASCADE;
SET test_oat_hooks.audit = off;
DROP TABLE b31_oat_f, b31_oat_c, b31_oat_p;
