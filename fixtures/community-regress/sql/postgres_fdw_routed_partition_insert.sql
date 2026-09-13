-- INSERT routed to a postgres_fdw foreign partition: ExecInitPartitionInfo's
-- CheckValidResultRel(leaf, CMD_INSERT) consults the FDW, ExecInitRoutingInfo
-- runs BeginForeignInsert, and ExecInsert's ri_FdwRoutine arm inserts
-- remotely (RETURNING, generated columns, ON CONFLICT DO NOTHING, row
-- triggers on the partition, cross-partition moves). Loopback server.
CREATE EXTENSION postgres_fdw;
DO $d$
BEGIN
  EXECUTE format('CREATE SERVER mt_loopback FOREIGN DATA WRAPPER postgres_fdw OPTIONS (dbname %L, port %L, host %L)',
                 current_database(), current_setting('port'), current_setting('unix_socket_directories'));
  EXECUTE format('CREATE USER MAPPING FOR CURRENT_USER SERVER mt_loopback OPTIONS (user %L)', current_user);
END
$d$;
CREATE TABLE frt_remote(id int);
CREATE TABLE frt_p(id int) PARTITION BY RANGE(id);
CREATE FOREIGN TABLE frt_pf PARTITION OF frt_p FOR VALUES FROM (0) TO (10) SERVER mt_loopback OPTIONS (table_name 'frt_remote');
CREATE TABLE frt_pl PARTITION OF frt_p FOR VALUES FROM (10) TO (20);
INSERT INTO frt_p VALUES (1);
INSERT INTO frt_p VALUES (11);
INSERT INTO frt_p VALUES (2), (12) RETURNING tableoid::regclass, *;
SELECT tableoid::regclass, * FROM frt_p ORDER BY id;
SELECT * FROM frt_remote ORDER BY id;
CREATE FUNCTION frt_log() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RAISE NOTICE '% % new=%', TG_NAME, TG_WHEN, NEW; RETURN NEW; END$$;
CREATE TRIGGER frt_bi BEFORE INSERT ON frt_pf FOR EACH ROW EXECUTE FUNCTION frt_log();
CREATE TRIGGER frt_ai AFTER INSERT ON frt_pf FOR EACH ROW EXECUTE FUNCTION frt_log();
INSERT INTO frt_p VALUES (3), (13) RETURNING tableoid::regclass, *;
-- transition tables cannot collect rows from a foreign partition
CREATE FUNCTION frt_st() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RAISE NOTICE 'n=%', (SELECT count(*) FROM newrows); RETURN NULL; END$$;
CREATE TRIGGER frt_as AFTER INSERT ON frt_p REFERENCING NEW TABLE AS newrows FOR EACH STATEMENT EXECUTE FUNCTION frt_st();
INSERT INTO frt_p VALUES (4);
INSERT INTO frt_p VALUES (14);
DROP TRIGGER frt_as ON frt_p;
-- cross-partition UPDATE moving a row into and out of the foreign partition
UPDATE frt_p SET id = 5 WHERE id = 14;
SELECT tableoid::regclass, * FROM frt_p ORDER BY id;
UPDATE frt_p SET id = 15 WHERE id = 5;
-- stored generated column and remote DEFAULT through the routed insert
CREATE TABLE frt_remote3(id int, g int, d int DEFAULT 42);
CREATE TABLE frt_p3(id int, g int GENERATED ALWAYS AS (id * 2) STORED, d int) PARTITION BY RANGE(id);
CREATE FOREIGN TABLE frt_pf3 PARTITION OF frt_p3 FOR VALUES FROM (0) TO (10) SERVER mt_loopback OPTIONS (table_name 'frt_remote3');
INSERT INTO frt_p3(id) VALUES (1) RETURNING *;
INSERT INTO frt_p3(id) VALUES (2) ON CONFLICT DO NOTHING RETURNING *;
SELECT * FROM frt_remote3 ORDER BY id;
DROP TABLE frt_p, frt_p3, frt_remote, frt_remote3;
DROP FUNCTION frt_log(), frt_st();
DROP USER MAPPING FOR CURRENT_USER SERVER mt_loopback;
DROP SERVER mt_loopback;
DROP EXTENSION postgres_fdw;
