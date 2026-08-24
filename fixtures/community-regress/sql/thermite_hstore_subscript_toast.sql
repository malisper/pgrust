-- Finding (thermite idx 79): hstore subscript read undetoasted text datums,
-- causing an out-of-bounds read for external/compressed values. An
-- external-storage (>2KB) key must round-trip through detoast.
CREATE EXTENSION IF NOT EXISTS hstore;
CREATE TABLE thermite_hs(h hstore, k text);
ALTER TABLE thermite_hs ALTER k SET STORAGE EXTERNAL;
INSERT INTO thermite_hs(k) VALUES (repeat('z', 3000));
UPDATE thermite_hs SET h[k] = 'v';
SELECT length(skeys(h)) AS keylen, (h -> repeat('z',3000)) AS val FROM thermite_hs;
DROP TABLE thermite_hs;
