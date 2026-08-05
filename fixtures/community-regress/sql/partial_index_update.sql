-- Imported from public PR malisper/pgrust#35 (author: jschaf, head 61d680bd5e).
-- Expected output captured from real PostgreSQL 18.
-- Creating a partial expression index after updating a candidate row exercises
-- the broken-HOT-chain path and must complete successfully.
BEGIN;
CREATE TABLE partial_index_update (
  id integer PRIMARY KEY,
  display_name text NOT NULL,
  state text NOT NULL
);
INSERT INTO partial_index_update VALUES (1, 'Alpha', 'active');
UPDATE partial_index_update
SET display_name = 'Beta'
WHERE id = 1;
CREATE UNIQUE INDEX partial_index_update_active_name
  ON partial_index_update (lower(display_name))
  WHERE state = 'active';
COMMIT;

SELECT indisvalid, indisready
FROM pg_index
WHERE indexrelid = 'partial_index_update_active_name'::regclass;
