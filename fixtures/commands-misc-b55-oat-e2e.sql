-- bugs/batch-55-backend-commands-misc, fp-commands-event_trigger#1: the
-- object-access hooks at event_trigger.c:340 (create), :465 (enable/disable)
-- and :568 (owner change) fire. Witnessed via LOAD 'test_oat_hooks'; the
-- expected file is captured from pgrust (the C install has no test modules).
LOAD 'test_oat_hooks';
SET test_oat_hooks.audit = on;
CREATE FUNCTION b55_etf() RETURNS event_trigger LANGUAGE plpgsql AS $$ BEGIN END $$;
CREATE ROLE b55_oat_su SUPERUSER;
CREATE EVENT TRIGGER b55_et ON ddl_command_end EXECUTE FUNCTION b55_etf();
ALTER EVENT TRIGGER b55_et DISABLE;
ALTER EVENT TRIGGER b55_et ENABLE;
ALTER EVENT TRIGGER b55_et OWNER TO b55_oat_su;
ALTER EVENT TRIGGER b55_et OWNER TO b55_oat_su;
DROP EVENT TRIGGER b55_et;
DROP FUNCTION b55_etf();
DROP ROLE b55_oat_su;
