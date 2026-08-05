-- Imported from public PR malisper/pgrust#35 (author: jschaf, head 61d680bd5e).
-- Expected output captured from real PostgreSQL 18.
-- pgrust-runner: wal_level=logical
-- With wal_level=logical, a plain table must be truncatable without invoking
-- an unimplemented seam or crashing.
CREATE TABLE truncate_logical (
  id integer
);
INSERT INTO truncate_logical VALUES (1);
TRUNCATE truncate_logical;
SELECT count(*) FROM truncate_logical;
