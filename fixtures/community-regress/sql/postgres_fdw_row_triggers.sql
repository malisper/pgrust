-- Row-level triggers on a postgres_fdw foreign table (ExecBR*/ExecAR*
-- with fdw_trigtuple, AfterTriggerSaveEvent's FDW tuplestore): BEFORE
-- triggers see the wholerow OLD tuple and can replace or drop NEW, AFTER
-- triggers fire from spooled row images. Loopback server on this backend.
CREATE EXTENSION postgres_fdw;
DO $d$
BEGIN
  EXECUTE format('CREATE SERVER mt_loopback FOREIGN DATA WRAPPER postgres_fdw OPTIONS (dbname %L, port %L, host %L)',
                 current_database(), current_setting('port'), current_setting('unix_socket_directories'));
  EXECUTE format('CREATE USER MAPPING FOR CURRENT_USER SERVER mt_loopback OPTIONS (user %L)', current_user);
END
$d$;
CREATE TABLE ftrig_remote(i int, t text);
CREATE FOREIGN TABLE ftrig_ft(i int, t text) SERVER mt_loopback OPTIONS (table_name 'ftrig_remote');
CREATE FUNCTION ftrig_log() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE NOTICE '% % % old=% new=%', TG_NAME, TG_WHEN, TG_OP, OLD, NEW;
  IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
  RETURN NEW;
END$$;
CREATE FUNCTION ftrig_mod() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.i = 99 THEN RETURN NULL; END IF;
  NEW.t := NEW.t || '+br';
  RETURN NEW;
END$$;
CREATE TRIGGER ftrig_t1 BEFORE INSERT OR UPDATE ON ftrig_ft FOR EACH ROW EXECUTE FUNCTION ftrig_mod();
CREATE TRIGGER ftrig_t2 AFTER INSERT OR UPDATE OR DELETE ON ftrig_ft FOR EACH ROW EXECUTE FUNCTION ftrig_log();
CREATE TRIGGER ftrig_t3 AFTER INSERT OR UPDATE OR DELETE ON ftrig_ft FOR EACH ROW WHEN (pg_trigger_depth() = 0) EXECUTE FUNCTION ftrig_log();
CREATE TRIGGER ftrig_t4 AFTER UPDATE OF t ON ftrig_ft FOR EACH ROW EXECUTE FUNCTION ftrig_log();
CREATE TRIGGER ftrig_t5 BEFORE DELETE ON ftrig_ft FOR EACH ROW EXECUTE FUNCTION ftrig_log();
INSERT INTO ftrig_ft VALUES (1, 'a'), (2, 'b') RETURNING *;
INSERT INTO ftrig_ft VALUES (99, 'skip') RETURNING *;
SELECT * FROM ftrig_ft ORDER BY i;
UPDATE ftrig_ft SET t = t || '!' WHERE i = 1 RETURNING *;
UPDATE ftrig_ft SET i = i + 10 WHERE i = 2 RETURNING *;
DELETE FROM ftrig_ft WHERE i = 12 RETURNING *;
SELECT * FROM ftrig_ft ORDER BY i;
CREATE TRIGGER ftrig_t6 AFTER INSERT ON ftrig_ft FOR EACH STATEMENT EXECUTE FUNCTION ftrig_log();
INSERT INTO ftrig_ft VALUES (3, 'c');
-- a trigger for another operation, and disabled triggers, never block DML
ALTER TABLE ftrig_ft DISABLE TRIGGER ftrig_t1;
ALTER TABLE ftrig_ft DISABLE TRIGGER ftrig_t2;
ALTER TABLE ftrig_ft DISABLE TRIGGER ftrig_t3;
INSERT INTO ftrig_ft VALUES (4, 'd') RETURNING *;
DELETE FROM ftrig_ft WHERE i = 4;
SELECT * FROM ftrig_ft ORDER BY i;
DROP FOREIGN TABLE ftrig_ft;
DROP TABLE ftrig_remote;
DROP FUNCTION ftrig_log(), ftrig_mod();
DROP USER MAPPING FOR CURRENT_USER SERVER mt_loopback;
DROP SERVER mt_loopback;
DROP EXTENSION postgres_fdw;
