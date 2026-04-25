\set bucket random(0, 9)
SELECT count(*) FROM scanbench WHERE touched = :bucket;
