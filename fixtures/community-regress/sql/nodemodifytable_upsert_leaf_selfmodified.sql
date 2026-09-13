-- ExecUpdate's TM_SelfModified arm (nodeModifyTable.c:2584) on the routed
-- ON CONFLICT DO UPDATE leg: a BEFORE UPDATE row trigger that updates the
-- conflicting row itself raises 27000, on a partition exactly as on a table.
CREATE TABLE upsm_p(id int PRIMARY KEY, v int) PARTITION BY RANGE(id);
CREATE TABLE upsm_p1 PARTITION OF upsm_p FOR VALUES FROM (0) TO (100);
INSERT INTO upsm_p VALUES (1,0);
CREATE FUNCTION upsm_trig() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN IF pg_trigger_depth() = 1 THEN UPDATE upsm_p SET v = v + 1 WHERE id = OLD.id; END IF; RETURN NEW; END$$;
CREATE TRIGGER upsm_bu BEFORE UPDATE ON upsm_p FOR EACH ROW EXECUTE FUNCTION upsm_trig();
INSERT INTO upsm_p VALUES (1,10) ON CONFLICT(id) DO UPDATE SET v = excluded.v;
SELECT * FROM upsm_p;
CREATE TABLE upsm_t(id int PRIMARY KEY, v int);
INSERT INTO upsm_t VALUES (1,0);
CREATE FUNCTION upsm_trig2() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN IF pg_trigger_depth() = 1 THEN UPDATE upsm_t SET v = v + 1 WHERE id = OLD.id; END IF; RETURN NEW; END$$;
CREATE TRIGGER upsm_bu BEFORE UPDATE ON upsm_t FOR EACH ROW EXECUTE FUNCTION upsm_trig2();
INSERT INTO upsm_t VALUES (1,10) ON CONFLICT(id) DO UPDATE SET v = excluded.v;
SELECT * FROM upsm_t;
DROP TABLE upsm_p, upsm_t;
DROP FUNCTION upsm_trig(), upsm_trig2();
