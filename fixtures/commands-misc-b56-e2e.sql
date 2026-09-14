-- bugs/batch-56-backend-commands-misc: commands crates, single-session legs.
-- Expected captured from C 18.6 (scripts/commands-misc-b56-e2e.sh header).
\set VERBOSITY verbose

-- fp-commands-tsearchcmds#3: buildDefItem keeps a strtod ERANGE value
-- (1e999) as a String, so the re-serialized option is quoted.
CREATE TEXT SEARCH TEMPLATE b56_tmpl (INIT = pg_catalog.prsd_lextype, LEXIZE = pg_catalog.dsimple_lexize);
CREATE TEXT SEARCH DICTIONARY b56_dict (TEMPLATE = b56_tmpl, x = 1e999, u = 1e-999, f = 1.5e3);
ALTER TEXT SEARCH DICTIONARY b56_dict (y = 'a');
SELECT dictinitoption FROM pg_ts_dict WHERE dictname = 'b56_dict';

-- fp-commands-typecmds-p2#2: AlterDomainAddConstraint holds RowExclusiveLock
-- on pg_type while the CHECK expression is validated against existing rows.
CREATE DOMAIN b56_d AS integer;
CREATE TABLE b56_t (v b56_d);
INSERT INTO b56_t VALUES (1);
CREATE FUNCTION b56_holds_type_lock(integer) RETURNS boolean LANGUAGE sql VOLATILE AS $$
  SELECT EXISTS (SELECT 1 FROM pg_locks WHERE pid = pg_backend_pid()
                 AND relation = 'pg_catalog.pg_type'::regclass AND mode = 'RowExclusiveLock' AND granted) $$;
ALTER DOMAIN b56_d ADD CONSTRAINT c CHECK (b56_holds_type_lock(VALUE));
SELECT conname FROM pg_constraint WHERE contypid = 'b56_d'::regtype;

-- fp-commands-subscriptioncmds#2: the keyword=value CONNECTION lane accepts
-- the legacy requiressl keyword (conninfo_storeval rewrites it to sslmode).
CREATE SUBSCRIPTION b56_sub CONNECTION 'requiressl=0 host=localhost' PUBLICATION b56_pub WITH (connect = false);
ALTER SUBSCRIPTION b56_sub CONNECTION 'requiressl=1 host=localhost';
SELECT subconninfo FROM pg_subscription WHERE subname = 'b56_sub';
ALTER SUBSCRIPTION b56_sub SET (slot_name = NONE);
DROP SUBSCRIPTION b56_sub;

-- fp-commands-trigger-p1#4: the rename callback's ownership check (a
-- non-owner is refused before any lock is taken; the two-session leg is in
-- scripts/commands-misc-b56-sessions-e2e.sh).
CREATE ROLE b56_r;
CREATE TABLE b56_trg (a int);
CREATE FUNCTION b56_tf() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$;
CREATE TRIGGER b56_tr BEFORE INSERT ON b56_trg FOR EACH ROW EXECUTE FUNCTION b56_tf();
GRANT ALL ON b56_trg TO b56_r;
SET ROLE b56_r;
ALTER TRIGGER b56_tr ON b56_trg RENAME TO b56_tr2;
ALTER TRIGGER b56_tr ON pg_class RENAME TO b56_tr2;
RESET ROLE;
ALTER TRIGGER b56_tr ON pg_class RENAME TO b56_tr2;
DROP TABLE b56_trg;
DROP FUNCTION b56_tf();
DROP ROLE b56_r;
