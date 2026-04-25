\set start_id random(1, :rows)
\set end_id :start_id + 100
SELECT count(*) FROM scanbench WHERE id >= :start_id AND id < :end_id;
