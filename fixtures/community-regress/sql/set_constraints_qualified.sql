-- Issue malisper/pgrust#70 (part b): catalog-qualified constraint names in
-- SET CONSTRAINTS. A qualifier naming another database is a clean
-- feature_not_supported error; a qualifier naming the current database is
-- accepted and falls through to schema resolution (C: AfterTriggerSetState,
-- trigger.c).
BEGIN;
SET CONSTRAINTS somedb.someschema.somename DEFERRED;
ROLLBACK;
BEGIN;
SET CONSTRAINTS postgres.someschema.somename DEFERRED;
ROLLBACK;
