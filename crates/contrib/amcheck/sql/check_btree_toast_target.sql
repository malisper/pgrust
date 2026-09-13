CREATE EXTENSION amcheck;

-- bt_normalize_tuple reforms an uncompressed extended/main varlena wider than
-- TOAST_INDEX_TARGET (MaxHeapTupleSize/16 = 510 bytes) so a PLAIN-built index
-- tuple fingerprints the same as its heap-callback twin after the column's
-- storage changes.
CREATE TABLE toast_target_heap(v text);
ALTER TABLE toast_target_heap ALTER v SET STORAGE PLAIN;
INSERT INTO toast_target_heap VALUES (repeat('x', 1000));
CREATE INDEX toast_target_idx ON toast_target_heap(v);
ALTER TABLE toast_target_heap ALTER v SET STORAGE EXTENDED;
SELECT attstorage FROM pg_attribute WHERE attrelid = 'toast_target_idx'::regclass AND attnum = 1;
SELECT bt_index_check('toast_target_idx', true);
SELECT bt_index_parent_check('toast_target_idx', true);
DROP TABLE toast_target_heap;
