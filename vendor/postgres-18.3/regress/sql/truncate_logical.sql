-- With wal_level=logical, a plain table must be truncatable without invoking
-- an unimplemented seam or crashing.
CREATE TABLE truncate_logical (
  id integer
);
INSERT INTO truncate_logical VALUES (1);
TRUNCATE truncate_logical;
SELECT count(*) FROM truncate_logical;
